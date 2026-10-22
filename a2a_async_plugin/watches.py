"""Durable background A2A watches.

``a2a_await(background=True)`` cannot block the agent turn: it captures a completion target from
the invoking tool call, stores a watch in the profile's ``a2a_tasks.db``, and returns a ``watch_id``.
A single profile-scoped thread polls the watched tasks and, on an authoritative terminal state,
publishes one message through ``PluginContext.inject_message`` — the supported
Hermes API that turns plugin background work into a new turn in the session that started it.

The rules this module enforces, and why each exists:

* **Only a peer settles a task.** A local watch cancellation, expiry, or polling failure says
  something about the watcher, never about the remote task. Those outcomes publish a distinct
  ``watch-*`` event and leave the task record alone.
* **One poller per profile.** Every process that loads the plugin can hold the same profile's
  watches, so a watch is leased in SQLite before it is polled, the lease is renewed before each
  task poll, and every state write is fenced on the lease owner. A second process skips a live
  watch; a crashed owner's watch is reclaimed when its lease expires, and a stale owner's writes
  are rejected instead of overwriting the new owner's.
* **Publication is durable, not best-effort.** A publish can fail (host admission refused, the
  profile's ledger briefly locked). The event is written to an outbox *before* the attempt and
  removed only after the publisher accepted it, so a failure is retried on the next pass and after
  a restart — never silently dropped with the watch closed.
* **Publish once per observation.** The outbox row is keyed by the event id, so re-observing a
  state cannot enqueue twice (the core publisher also de-duplicates by event id).
* **Bounded work.** Sequential polling, bounded HTTP timeouts, per-item backoff for transient
  failures, a finite watch lifetime (default 7 days), a give-up threshold after repeated errors,
  and a retention window that prunes only finished watches whose publications are all delivered.

The manager never imports Hermes internals for delivery: it publishes through the
``PluginContext`` it was registered with, and stores state in the plugin's own database. It does
bind the owning profile scope (home + secret scope) around every background operation, because a
plain thread inherited from a multiplexed gateway carries the launch profile's ambient scope and
would resolve peers, credentials and transcript paths against the wrong profile.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from . import ownership, protocol

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_S = 5.0
MIN_POLL_INTERVAL_S = 2.0
MAX_POLL_INTERVAL_S = 60.0
DEFAULT_WATCH_TTL_S = 7 * 24 * 3600.0
# A watch is leased before it is polled so two processes on one profile do not both poll it. The
# lease is short enough that a crashed owner's watches resume quickly, long enough to cover a slow
# GetTask round trip; it is renewed before each task so a long batch cannot outlive it.
LEASE_S = 90.0
# Give up on a task after this many consecutive polling errors (about the 12th failed poll), with
# exponential backoff capped at MAX_BACKOFF_S. The remote task is never marked failed by this.
MAX_CONSECUTIVE_ERRORS = 12
MAX_BACKOFF_S = 600.0
# The live status line is a status-bar notice: it is edited in place, so it must not repaint the
# TUI on every 5s poll. State changes bypass this; elapsed-time refreshes wait for it.
STATUS_REFRESH_S = 15.0
# Status lines are one truncated line in the status rule; show at most this many peers by name.
STATUS_MAX_PEERS = 2
# Publication retries are independent of polling: a failed publish is retried every few seconds,
# backing off to five minutes, and survives restarts.
PUBLICATION_BACKOFF_S = 5.0
MAX_PUBLICATION_BACKOFF_S = 300.0
# Finished watches and delivered outbox rows are pruned after this window (no unpublished rows).
RETENTION_S = 7 * 24 * 3600.0
# An unowned task is cancelled remotely after this long (it is costing a peer work nobody is
# waiting for) and abandoned after the TTL. Both are configurable per profile.
DEFAULT_UNOWNED_CANCEL_AFTER_S = 24 * 3600.0
DEFAULT_UNOWNED_TTL_S = 7 * 24 * 3600.0
# Keep a published completion inside the core publisher's content budget (32k chars).
MAX_REPLY_CHARS = 24_000

WATCH_ACTIVE = "active"
WATCH_EXPIRING = "expiring"  # expired, but its notice may not be durably queued yet
WATCH_PAUSED = "paused"
WATCH_COMPLETE = "complete"
WATCH_CANCELED = "canceled"
WATCH_EXPIRED = "expired"

ITEM_DONE = "done"
ITEM_PAUSED = "paused"
ITEM_UNWATCHED = "unwatched"


class WatchError(ValueError):
    """A background watch that cannot be created (bad peer, no core API, no session)."""


class TransientPollError(RuntimeError):
    """A poll that failed for a reason retrying can fix (network, server error, RPC error)."""


# --------------------------------------------------------------------------
# Profile scope
# --------------------------------------------------------------------------

@contextlib.contextmanager
def profile_scope(home: Optional[str]):
    """Run plugin work in ``home``'s profile scope (writes no state of its own).

    A watcher thread started from a multiplexed gateway inherits the LAUNCH profile's ambient
    scope. Pinning the SQLite path is not enough: peer resolution, ``.env`` secrets and transcript
    persistence all resolve from the ambient home. Binding the home override and, when the
    process serves several profiles, the profile's secret scope makes every resolution in the
    block use the profile that created the watch.
    """
    if not home:
        yield
        return
    try:
        from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    except Exception:  # pragma: no cover - a runtime without the override helper
        yield
        return
    token = set_hermes_home_override(Path(home))
    secret_token = None
    try:
        try:
            from agent.secret_scope import (
                build_profile_secret_scope, is_multiplex_active, set_secret_scope,
            )
            if is_multiplex_active():
                secret_token = set_secret_scope(
                    build_profile_secret_scope(Path(home)), profile_home=str(home))
        except Exception:  # older runtime: the home override is still the important half
            secret_token = None
        yield
    finally:
        if secret_token is not None:
            try:
                from agent.secret_scope import reset_secret_scope
                reset_secret_scope(secret_token)
            except Exception:  # pragma: no cover
                logger.debug("A2A: could not reset the profile secret scope", exc_info=True)
        reset_hermes_home_override(token)


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------

class WatchStore:
    """Profile-scoped SQLite store for watches, their items and the publication outbox.

    Shares ``a2a_tasks.db`` with :class:`protocol.TaskStore` so a watch, its task updates and the
    replies they produce live in one file (and one schema version).
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path or protocol.TaskStore._resolve_db_path(None)

    # -- connection -------------------------------------------------------
    @contextlib.contextmanager
    def _session(self):
        """Connection + transaction boundary; closes the connection (sqlite's own CM does not)."""
        con = self._connect()
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _connect(self) -> sqlite3.Connection:
        parent = os.path.dirname(self.db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        con = sqlite3.connect(self.db_path, timeout=10)
        con.execute("PRAGMA journal_mode=WAL")
        con.row_factory = sqlite3.Row
        # Only pay the schema write lock when the database is new or older: the poller opens a
        # connection per operation and a BEGIN IMMEDIATE on every read would serialize against the
        # plugin's task-store writes.
        version = int(con.execute("PRAGMA user_version").fetchone()[0])
        if version != protocol.A2A_TASK_SCHEMA_VERSION:
            protocol.TaskStore._ensure_db(con)
        return con

    # -- create / read ----------------------------------------------------
    def create(self, *, watch_id: str, session_id: str, target: dict,
               tasks: Iterable[tuple[str, str]], ttl_s: float = DEFAULT_WATCH_TTL_S) -> None:
        now = time.time()
        items = list(tasks)
        if not items:
            raise WatchError("a watch needs at least one task.")
        with self._session() as con:
            con.execute(
                """INSERT INTO a2a_watches
                       (watch_id, session_id, target_json, state, created_at, updated_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (watch_id, session_id, json.dumps(target, sort_keys=True), WATCH_ACTIVE,
                 now, now, now + max(60.0, ttl_s)))
            for peer, task_id in items:
                con.execute(
                    """INSERT OR IGNORE INTO a2a_watch_items
                           (watch_id, peer, task_id, state, next_poll_at, updated_at)
                       VALUES (?, ?, ?, '', 0, ?)""",
                    (watch_id, peer, task_id, now))
            con.commit()

    def get(self, watch_id: str) -> Optional[dict]:
        with self._session() as con:
            row = con.execute("SELECT * FROM a2a_watches WHERE watch_id=?", (watch_id,)).fetchone()
            if row is None:
                return None
            watch = dict(row)
            watch["items"] = [dict(r) for r in con.execute(
                "SELECT * FROM a2a_watch_items WHERE watch_id=? ORDER BY peer, task_id",
                (watch_id,)).fetchall()]
        # Keep the raw column alongside the decoded view: the poller publishes from the row it
        # read, and a caller inspecting a watch wants both.
        watch["target"] = _load_target(watch.get("target_json"))
        return watch

    def status_rows(self, session_id: str) -> list[dict]:
        """This session's live watches with their unfinished items, oldest watch first.

        Input for the status line only: finished items are omitted (the watch is settling or
        already settled), and no target is decoded because the summary never shows it.
        """
        if not session_id:
            return []
        with self._session() as con:
            watches = [dict(r) for r in con.execute(
                """SELECT watch_id, state, created_at FROM a2a_watches
                    WHERE session_id=? AND state IN (?, ?) ORDER BY created_at""",
                (session_id, WATCH_ACTIVE, WATCH_EXPIRING)).fetchall()]
            for watch in watches:
                watch["items"] = [dict(r) for r in con.execute(
                    """SELECT peer, task_id, state, errors, completed_at FROM a2a_watch_items
                        WHERE watch_id=? AND completed_at IS NULL ORDER BY peer, task_id""",
                    (watch["watch_id"],)).fetchall()]
        return watches

    def last_watch(self, session_id: str) -> Optional[dict]:
        """The most recently settled watch of one session, with every item and its outcome."""
        if not session_id:
            return None
        with self._session() as con:
            row = con.execute(
                """SELECT * FROM a2a_watches
                    WHERE session_id=? AND state NOT IN (?, ?)
                    ORDER BY updated_at DESC LIMIT 1""",
                (session_id, WATCH_ACTIVE, WATCH_EXPIRING)).fetchone()
            if row is None:
                return None
            watch = dict(row)
            watch["items"] = [dict(r) for r in con.execute(
                """SELECT peer, task_id, state, short_state, completed_at FROM a2a_watch_items
                    WHERE watch_id=? ORDER BY peer, task_id""",
                (watch["watch_id"],)).fetchall()]
        return watch

    def active_sessions(self) -> list[str]:
        """Distinct session ids with a live watch (status-line refresh target)."""
        with self._session() as con:
            rows = con.execute(
                """SELECT DISTINCT session_id FROM a2a_watches
                    WHERE state IN (?, ?) AND session_id<>''""",
                (WATCH_ACTIVE, WATCH_EXPIRING)).fetchall()
        return [str(r["session_id"]) for r in rows]

    def list_watches(self, limit: int = 50) -> list[dict]:
        """Every persisted watch, live ones first, then the most recently touched."""
        with self._session() as con:
            watches = [dict(r) for r in con.execute(
                """SELECT * FROM a2a_watches
                    ORDER BY (state IN (?, ?)) DESC, updated_at DESC LIMIT ?""",
                (WATCH_ACTIVE, WATCH_EXPIRING, max(1, int(limit)))).fetchall()]
            for watch in watches:
                watch["items"] = [dict(r) for r in con.execute(
                    """SELECT peer, task_id, state, short_state, errors, completed_at,
                              updated_at
                        FROM a2a_watch_items WHERE watch_id=? ORDER BY peer, task_id""",
                    (watch["watch_id"],)).fetchall()]
            return watches

    def active_count(self) -> int:
        with self._session() as con:
            row = con.execute(
                "SELECT COUNT(*) FROM a2a_watches WHERE state=?", (WATCH_ACTIVE,)).fetchone()
        return int(row[0]) if row else 0

    def active_count_readonly(self) -> int:
        """Active watches WITHOUT creating or migrating the database.

        Plugin registration probes this on every load in every process. Opening through
        :meth:`_connect` would apply the schema (a write lock, and a v3 database upgraded just
        because a plugin loaded), so the probe is read-only and treats "no watch table yet" as
        "nothing to resume".
        """
        row = self._readonly_scalar(
            "SELECT COUNT(*) FROM a2a_watches WHERE state IN (?, ?)",
            (WATCH_ACTIVE, WATCH_EXPIRING), "a2a_watches")
        return row

    def pending_publication_count_readonly(self) -> int:
        """Outbox rows not yet accepted by the host, read without creating or migrating."""
        return self._readonly_scalar(
            "SELECT COUNT(*) FROM a2a_watch_publications WHERE published_at IS NULL",
            (), "a2a_watch_publications")

    def resumable_count_readonly(self) -> int:
        """Work a reload must pick up: live watches plus undelivered publications."""
        return self.active_count_readonly() + self.pending_publication_count_readonly()

    def _readonly_scalar(self, sql: str, params: tuple, table: str) -> int:
        if not self.db_path or not os.path.exists(self.db_path):
            return 0
        try:
            con = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=5)
            try:
                if con.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone() is None:
                    return 0
                row = con.execute(sql, params).fetchone()
            finally:
                con.close()
        except sqlite3.Error:
            return 0
        return int(row[0]) if row else 0

    def due(self, now: Optional[float] = None) -> list[dict]:
        """Active watches with a due item whose lease is free (or expired)."""
        now = time.time() if now is None else now
        with self._session() as con:
            rows = con.execute(
                """SELECT * FROM a2a_watches w
                    WHERE w.state=? AND w.expires_at>? AND (w.lease_until<=?)
                      AND EXISTS (SELECT 1 FROM a2a_watch_items i
                                   WHERE i.watch_id=w.watch_id AND i.completed_at IS NULL
                                     AND i.state NOT IN (?, ?) AND i.next_poll_at<=?)
                    ORDER BY w.created_at""",
                (WATCH_ACTIVE, now, now, ITEM_DONE, ITEM_PAUSED, now)).fetchall()
        return [dict(r) for r in rows]

    # -- leasing ----------------------------------------------------------
    def claim(self, watch_id: str, owner: str, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        with self._session() as con:
            cur = con.execute(
                """UPDATE a2a_watches SET lease_owner=?, lease_until=?, updated_at=?
                    WHERE watch_id=? AND state=? AND lease_until<=?""",
                (owner, now + LEASE_S, now, watch_id, WATCH_ACTIVE, now))
            con.commit()
            return cur.rowcount == 1

    def renew(self, watch_id: str, owner: str, now: Optional[float] = None) -> bool:
        """Extend our lease; False means another owner holds the watch (stop working on it)."""
        now = time.time() if now is None else now
        with self._session() as con:
            cur = con.execute(
                """UPDATE a2a_watches SET lease_until=?, updated_at=?
                    WHERE watch_id=? AND state=? AND lease_owner=?""",
                (now + LEASE_S, now, watch_id, WATCH_ACTIVE, owner))
            con.commit()
            return cur.rowcount == 1

    def release(self, watch_id: str, owner: str) -> None:
        with self._session() as con:
            con.execute(
                """UPDATE a2a_watches SET lease_owner='', lease_until=0, updated_at=?
                    WHERE watch_id=? AND lease_owner=?""",
                (time.time(), watch_id, owner))
            con.commit()

    @staticmethod
    def _fence(owner: Optional[str]) -> tuple[str, tuple]:
        """SQL fragment that rejects a write from a process that lost the lease."""
        if not owner:
            return "", ()
        return (" AND EXISTS (SELECT 1 FROM a2a_watches w WHERE w.watch_id=a2a_watch_items.watch_id"
                " AND w.lease_owner=?)"), (owner,)

    # -- item updates -----------------------------------------------------
    def record_working(self, watch_id: str, peer: str, task_id: str, state: str,
                       context_id: str = "", next_poll_at: float = 0.0,
                       owner: Optional[str] = None) -> None:
        now = time.time()
        fence_sql, fence_params = self._fence(owner)
        with self._session() as con:
            con.execute(
                f"""UPDATE a2a_watch_items
                      SET state=?, short_state=?, context_id=COALESCE(NULLIF(?, ''), context_id),
                          errors=0, next_poll_at=?, updated_at=?
                    WHERE watch_id=? AND peer=? AND task_id=?{fence_sql}""",
                (state or "", _short_state(state) if state else "",
                 context_id or "", next_poll_at, now, watch_id, peer, task_id, *fence_params))
            con.commit()

    def record_error(self, watch_id: str, peer: str, task_id: str, message: str,
                     next_poll_at: float, owner: Optional[str] = None) -> int:
        """Count a failed poll; returns the new consecutive-error count (0 when fenced out)."""
        now = time.time()
        fence_sql, fence_params = self._fence(owner)
        with self._session() as con:
            con.execute(
                f"""UPDATE a2a_watch_items SET errors=errors+1, next_poll_at=?, updated_at=?
                    WHERE watch_id=? AND peer=? AND task_id=?{fence_sql}""",
                (next_poll_at, now, watch_id, peer, task_id, *fence_params))
            con.execute(
                "UPDATE a2a_watches SET last_error=?, updated_at=? WHERE watch_id=?",
                (message[:500], now, watch_id))
            row = con.execute(
                """SELECT errors FROM a2a_watch_items
                    WHERE watch_id=? AND peer=? AND task_id=?""",
                (watch_id, peer, task_id)).fetchone()
            con.commit()
        return int(row["errors"]) if row else 0

    def mark_item(self, watch_id: str, peer: str, task_id: str, item_state: str,
                  published_state: str = "", owner: Optional[str] = None,
                  short_state: str = "") -> None:
        now = time.time()
        fence_sql, fence_params = self._fence(owner)
        with self._session() as con:
            con.execute(
                f"""UPDATE a2a_watch_items
                      SET state=?, published_state=CASE WHEN ?='' THEN published_state ELSE ? END,
                          short_state=CASE WHEN ?='' THEN short_state ELSE ? END,
                          errors=0, next_poll_at=?, completed_at=?, updated_at=?
                    WHERE watch_id=? AND peer=? AND task_id=?{fence_sql}""",
                (item_state, published_state, published_state, short_state, short_state,
                 now, now, now, watch_id, peer, task_id, *fence_params))
            con.commit()

    def settle(self, watch_id: str, now: Optional[float] = None) -> str:
        """Close a watch whose items are all done/paused/unwatched; return its state."""
        now = time.time() if now is None else now
        with self._session() as con:
            rows = con.execute(
                "SELECT state, completed_at FROM a2a_watch_items WHERE watch_id=?",
                (watch_id,)).fetchall()
            if not rows:
                return WATCH_ACTIVE
            if any(r["completed_at"] is None for r in rows):
                return WATCH_ACTIVE
            state = WATCH_PAUSED if any(r["state"] == ITEM_PAUSED for r in rows) else WATCH_COMPLETE
            con.execute(
                "UPDATE a2a_watches SET state=?, lease_owner='', lease_until=0, updated_at=? "
                "WHERE watch_id=? AND state=?",
                (state, now, watch_id, WATCH_ACTIVE))
            con.commit()
        return state

    def cancel_session(self, session_id: str, task_ids: Optional[Iterable[str]] = None) -> int:
        """Stop watching a session's tasks (session end, or an ownership release).

        With ``task_ids``, only those items stop and each affected watch is settled; the watch
        keeps polling its remaining tasks. Without, every active watch of the session is cancelled.
        Returns the number of watches stopped or settled.
        """
        if not session_id:
            return 0
        now = time.time()
        self.drop_pending_publications(session_id, task_ids)
        with self._session() as con:
            if task_ids is None:
                rows = con.execute(
                    """SELECT watch_id FROM a2a_watches
                        WHERE session_id=? AND state=?""",
                    (session_id, WATCH_ACTIVE)).fetchall()
                con.execute(
                    """UPDATE a2a_watches SET state=?, lease_owner='', lease_until=0, updated_at=?
                        WHERE session_id=? AND state=?""",
                    (WATCH_CANCELED, now, session_id, WATCH_ACTIVE))
                con.commit()
                return len(rows)
            ids = [str(t) for t in task_ids if t]
            if not ids:
                return 0
            marks = ",".join("?" for _ in ids)
            rows = con.execute(
                f"""SELECT DISTINCT watch_id FROM a2a_watch_items
                     WHERE task_id IN ({marks})
                       AND watch_id IN (SELECT watch_id FROM a2a_watches
                                         WHERE session_id=? AND state=?)""",
                (*ids, session_id, WATCH_ACTIVE)).fetchall()
            con.execute(
                f"""UPDATE a2a_watch_items SET state=?, completed_at=?, updated_at=?
                     WHERE task_id IN ({marks})
                       AND watch_id IN (SELECT watch_id FROM a2a_watches
                                         WHERE session_id=? AND state=?)""",
                (ITEM_UNWATCHED, now, now, *ids, session_id, WATCH_ACTIVE))
            con.commit()
        watch_ids = [row["watch_id"] for row in rows]
        for watch_id in watch_ids:
            self.settle(watch_id, now=now)
        return len(watch_ids)

    def cancel(self, watch_id: str, session_id: str = "") -> Optional[str]:
        """Stop polling a watch. Returns the prior state, or None when unknown/not owned."""
        now = time.time()
        with self._session() as con:
            row = con.execute("SELECT state, session_id FROM a2a_watches WHERE watch_id=?",
                              (watch_id,)).fetchone()
            if row is None:
                return None
            if session_id and row["session_id"] and session_id != row["session_id"]:
                raise WatchError("this watch belongs to a different session.")
            if row["state"] != WATCH_ACTIVE:
                return row["state"]
            con.execute(
                "UPDATE a2a_watches SET state=?, lease_owner='', lease_until=0, updated_at=? "
                "WHERE watch_id=?",
                (WATCH_CANCELED, now, watch_id))
            con.commit()
        return WATCH_ACTIVE

    def expire(self, now: Optional[float] = None) -> list[dict]:
        """Move overdue watches to ``expiring`` and return them, plus any left over.

        The state change is committed before the notice is enqueued, so a crash in between must
        not lose the notice: an ``expiring`` watch is picked up again on the next pass (the event
        id is stable, so re-enqueueing is idempotent) and only then marked ``expired``.
        """
        now = time.time() if now is None else now
        with self._session() as con:
            rows = con.execute(
                "SELECT * FROM a2a_watches WHERE state=? AND expires_at<=?",
                (WATCH_ACTIVE, now)).fetchall()
            for watch in rows:
                con.execute(
                    "UPDATE a2a_watches SET state=?, lease_owner='', lease_until=0, updated_at=? "
                    "WHERE watch_id=?",
                    (WATCH_EXPIRING, now, watch["watch_id"]))
            pending = con.execute(
                "SELECT * FROM a2a_watches WHERE state=?", (WATCH_EXPIRING,)).fetchall()
            con.commit()
        expired = [dict(r) for r in pending]
        for watch in expired:
            watch["items"] = [dict(r) for r in con_items(self.db_path, watch["watch_id"])]
        return expired

    def mark_expired(self, watch_id: str, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        with self._session() as con:
            con.execute(
                "UPDATE a2a_watches SET state=?, updated_at=? WHERE watch_id=? AND state=?",
                (WATCH_EXPIRED, now, watch_id, WATCH_EXPIRING))
            con.commit()

    def drop_pending_publications(self, session_id: str,
                                  task_ids: Optional[Iterable[str]] = None) -> int:
        """Drop undelivered outbox rows for tasks leaving this session (release or cancel).

        The message rows stay undelivered, so the next owner receives them; publishing them to a
        target the old session owned would deliver work into a session that gave it up.
        """
        if not session_id:
            return 0
        with self._session() as con:
            if task_ids is None:
                cur = con.execute(
                    """DELETE FROM a2a_watch_publications
                        WHERE published_at IS NULL AND watch_id IN (
                              SELECT watch_id FROM a2a_watches WHERE session_id=?)""",
                    (session_id,))
            else:
                ids = [str(t) for t in task_ids if t]
                if not ids:
                    return 0
                total = 0
                from .ownership import publication_names_task
                rows = con.execute(
                    "SELECT event_id, metadata_json FROM a2a_watch_publications"
                    " WHERE published_at IS NULL").fetchall()
                for row in rows:
                    if any(publication_names_task(row["metadata_json"], task_id)
                           for task_id in ids):
                        total += con.execute(
                            "DELETE FROM a2a_watch_publications WHERE event_id=?",
                            (row["event_id"],)).rowcount
                return total
            return cur.rowcount

    # -- publication outbox ----------------------------------------------
    def enqueue_publication(self, *, watch_id: str, event_id: str, content: str,
                            metadata: dict) -> None:
        """Persist an event for delivery. Idempotent per event id; the row IS the retry state.

        A changed body reopens a settled row: an artifact keeps its identity across snapshots, so
        its outbox event id is stable and the final revision must be deliverable again after an
        earlier one was accepted.
        """
        now = time.time()
        with self._session() as con:
            con.execute(
                """INSERT INTO a2a_watch_publications
                       (event_id, watch_id, content, metadata_json, created_at, next_attempt_at)
                   VALUES (?, ?, ?, ?, ?, 0)
                   ON CONFLICT(event_id) DO UPDATE SET
                       content=excluded.content,
                       metadata_json=excluded.metadata_json,
                       attempts=0,
                       next_attempt_at=0,
                       published_at=NULL
                   WHERE a2a_watch_publications.content <> excluded.content""",
                (event_id, watch_id, content, json.dumps(metadata, sort_keys=True), now))
            con.commit()

    @contextlib.contextmanager
    def publication_guard(self, event_id: str):
        """Hold the plugin DB write lock across one publish, re-reading the row first.

        Ownership can change (release + adoption) between reading an outbox row and delivering
        it, which would publish work into the session that gave the task up. Taking BEGIN
        IMMEDIATE and re-reading under it serializes delivery against every release/cancel, so a
        row that no longer exists is never published.
        """
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                """SELECT p.*, w.target_json, w.session_id AS watch_session_id
                     FROM a2a_watch_publications p
                     JOIN a2a_watches w ON w.watch_id = p.watch_id
                    WHERE p.event_id=? AND p.published_at IS NULL""",
                (event_id,)).fetchone()
            if row is not None and not self._publication_authorized(con, row):
                # The task was released (and possibly adopted) after this row was written. The
                # message stays queued for the next owner; publishing it would deliver work into
                # the session that gave it up.
                logger.info("A2A: dropping %s; its task left session %s", event_id,
                            row["watch_session_id"])
                con.execute("DELETE FROM a2a_watch_publications WHERE event_id=?", (event_id,))
                row = None
            yield con, (dict(row) if row else None)
        except Exception:
            con.rollback()
            raise
        else:
            con.commit()
        finally:
            con.close()

    @staticmethod
    def _publication_authorized(con, row) -> bool:
        """True while the session that owns the watch still owns every task the row names."""
        session = str(row["watch_session_id"] or "")
        task_ids: list[str] = []
        try:
            meta = json.loads(row["metadata_json"] or "{}")
        except (TypeError, ValueError):
            meta = {}
        if meta.get("task_id"):
            task_ids.append(str(meta["task_id"]))
        for listed in meta.get("tasks") or []:
            task_ids.append(str(listed).rsplit("/", 1)[-1])
        for task_id in task_ids:
            owner = con.execute(
                "SELECT owner_session_id FROM a2a_tasks WHERE task_id=?", (task_id,)).fetchone()
            if owner is None:
                # Fail closed: a publication naming a task with no record cannot be authorized,
                # and a missing row must never be read as "nobody owns it, so anyone may publish".
                logger.error("A2A: publication names unknown task %s; refusing to deliver it",
                             task_id)
                return False
            if str(owner[0] or "") != session:
                return False
        return True

    def publish_if_owned(self, session_id: str, task_id: str, publish,
                         mark_messages: Iterable[str] = (),
                         expected_text: Optional[str] = None) -> bool:
        """Publish one message and mark it delivered, in ONE transaction.

        Ownership is verified before the host sees the message and the backlog mark commits with
        it, so an adoption cannot land between "delivered to A" and "marked delivered": the
        adopter would otherwise replay a message A already received, into a different session.

        ``expected_text`` is the revision the caller is about to publish. The backlog row is keyed
        on the message identity, not its content, so a background poll can revise the artifact
        between the caller's snapshot and this call; without the revision fence the mark would
        acknowledge content the host never saw (Amy round 2, P1).
        """
        ids = [str(m) for m in mark_messages if m]
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            owner = con.execute(
                "SELECT owner_session_id FROM a2a_tasks WHERE task_id=?", (task_id,)).fetchone()
            current = "" if owner is None else str(owner[0] or "")
            if current != str(session_id or ""):
                # Exact match required: a released task (owner "") must not accept a publish from
                # the session that just gave it up, and a bound session must own what it replays.
                con.rollback()
                return False
            undelivered: dict[str, str] = {}
            if ids:
                # Publish only what is still undelivered. The outbox flush and a direct backlog
                # replay are two paths for the same durable message; whichever loses this check
                # must not inject a second copy (Amy's PR #9 finding 3).
                marks = ",".join("?" for _ in ids)
                rows = con.execute(
                    f"""SELECT message_id, text, delivered_at FROM a2a_task_messages
                         WHERE task_id=? AND message_id IN ({marks})""",
                    (task_id, *ids)).fetchall()
                undelivered = {r["message_id"]: str(r["text"] or "")
                               for r in rows if r["delivered_at"] is None}
                if not undelivered:
                    con.rollback()
                    return False
                if expected_text is not None and any(
                        text != expected_text for text in undelivered.values()):
                    # The caller snapshotted an older revision of the artifact. Publishing it
                    # would let the mark settle content the host never saw (Amy round 2, P1).
                    logger.info("A2A: %s/%s changed since it was read; leaving it queued",
                                task_id, ", ".join(sorted(undelivered)))
                    con.rollback()
                    return False
            # The callback's own verdict matters: a host that REFUSED the message must leave it
            # queued for the next attempt, not be reported as delivered because we own the task.
            delivered = publish()
            if delivered and ids:
                now = time.time()
                for message_id in ids:
                    con.execute(
                        """UPDATE a2a_task_messages
                              SET delivered_at=?, delivered_to_session=?
                            WHERE task_id=? AND message_id=? AND delivered_at IS NULL
                              AND text=?""",
                        (now, session_id, task_id, message_id,
                         undelivered.get(message_id, expected_text or "")))
                    # Settle the matching outbox row in the SAME commit. Leaving it pending let
                    # the next flush deliver a message this path already accepted.
                    self._settle_publications(con, task_id, message_id, now)
            con.commit()
            return bool(delivered)
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    @staticmethod
    def _settle_publications(con: sqlite3.Connection, task_id: str, message_id: str,
                             now: float) -> None:
        """Retire pending outbox rows for one delivered message.

        Metadata is parsed in Python rather than with ``json_extract``: a malformed row would
        otherwise raise inside the publish transaction and roll back a delivery the host had
        already accepted (local review, D4). An unreadable row is left untouched.
        """
        rows = con.execute(
            "SELECT event_id, metadata_json FROM a2a_watch_publications"
            " WHERE published_at IS NULL").fetchall()
        for row in rows:
            meta = _load_metadata(row["metadata_json"])
            if (str(meta.get("task_id") or "") != task_id
                    or str(meta.get("message_id") or "") != message_id):
                continue
            con.execute(
                "UPDATE a2a_watch_publications SET published_at=?, next_attempt_at=0"
                " WHERE event_id=?", (now, row["event_id"]))

    def due_publications(self, now: Optional[float] = None) -> list[dict]:
        now = time.time() if now is None else now
        with self._session() as con:
            rows = con.execute(
                """SELECT p.*, w.target_json FROM a2a_watch_publications p
                     JOIN a2a_watches w ON w.watch_id = p.watch_id
                    WHERE p.published_at IS NULL AND p.next_attempt_at<=?
                    ORDER BY p.created_at LIMIT 25""",
                (now,)).fetchall()
        return [dict(r) for r in rows]

    def mark_published(self, event_id: str, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        with self._session() as con:
            row = con.execute(
                "SELECT watch_id, metadata_json FROM a2a_watch_publications WHERE event_id=?",
                (event_id,)).fetchone()
            if row is None:
                return
            con.execute(
                "UPDATE a2a_watch_publications SET published_at=?, next_attempt_at=0 "
                "WHERE event_id=?",
                (now, event_id))
            meta = _load_metadata(row["metadata_json"])
            peer = str(meta.get("peer") or "")
            task_id = str(meta.get("task_id") or "")
            state = str(meta.get("state") or "")
            if peer and task_id and state:
                # Only now is the observation honestly "published": a refused publish must stay
                # eligible on the item, not look delivered.
                con.execute(
                    """UPDATE a2a_watch_items SET published_state=?, updated_at=?
                        WHERE watch_id=? AND peer=? AND task_id=?""",
                    (state, now, row["watch_id"], peer, task_id))
            con.commit()

    def record_publication_failure(self, event_id: str, now: Optional[float] = None) -> int:
        now = time.time() if now is None else now
        with self._session() as con:
            con.execute(
                "UPDATE a2a_watch_publications SET attempts=attempts+1 WHERE event_id=?",
                (event_id,))
            row = con.execute(
                "SELECT attempts FROM a2a_watch_publications WHERE event_id=?",
                (event_id,)).fetchone()
            attempts = int(row["attempts"]) if row else 0
            con.execute(
                "UPDATE a2a_watch_publications SET next_attempt_at=? WHERE event_id=?",
                (now + _publication_backoff(attempts), event_id))
            con.commit()
        return attempts

    def prune(self, now: Optional[float] = None) -> None:
        """Age out delivered events and finished watches that have nothing left to publish."""
        now = time.time() if now is None else now
        cutoff = now - RETENTION_S
        with self._session() as con:
            con.execute(
                "DELETE FROM a2a_watch_publications WHERE published_at IS NOT NULL AND published_at<?",
                (cutoff,))
            con.execute(
                """DELETE FROM a2a_watch_items WHERE watch_id IN (
                       SELECT w.watch_id FROM a2a_watches w
                        WHERE w.state!=? AND w.updated_at<?
                          AND NOT EXISTS (SELECT 1 FROM a2a_watch_publications p
                                           WHERE p.watch_id=w.watch_id AND p.published_at IS NULL))""",
                (WATCH_ACTIVE, cutoff))
            con.execute(
                """DELETE FROM a2a_watches
                    WHERE state!=? AND updated_at<?
                      AND NOT EXISTS (SELECT 1 FROM a2a_watch_publications p
                                       WHERE p.watch_id=a2a_watches.watch_id
                                         AND p.published_at IS NULL)""",
                (WATCH_ACTIVE, cutoff))
            con.commit()


def con_items(db_path: str, watch_id: str) -> list[sqlite3.Row]:
    con = sqlite3.connect(db_path, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        return con.execute(
            "SELECT * FROM a2a_watch_items WHERE watch_id=? ORDER BY peer, task_id",
            (watch_id,)).fetchall()
    finally:
        con.close()


def _load_target(raw: Any) -> dict:
    try:
        value = json.loads(raw)
    except (TypeError, ValueError) as exc:  # pragma: no cover - only a corrupt row
        raise WatchError("stored completion target is unreadable") from exc
    if not isinstance(value, dict):
        raise WatchError("stored completion target is not a mapping")
    return value


def _load_metadata(raw: Any) -> dict:
    try:
        value = json.loads(raw) if raw else {}
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


# --------------------------------------------------------------------------
# Polling
# --------------------------------------------------------------------------

def poll_task(peer_name: str, task_id: str) -> dict:
    """One authoritative ``GetTask`` round trip; raises TransientPollError on transport/RPC issues."""
    from . import tools

    peer = tools._resolve_peer(peer_name)
    if not peer or not peer.get("url"):
        raise TransientPollError(f"unknown peer '{peer_name}'")
    try:
        resp = tools._rpc_request(peer, "GetTask", {"id": task_id}, timeout=30)
    except Exception as exc:  # urllib and JSON errors are all retryable here
        raise TransientPollError(str(exc) or type(exc).__name__) from exc
    if "error" in resp:
        err = resp.get("error") or {}
        raise TransientPollError(f"peer returned {err.get('code', '')}: {err.get('message', err)}")
    payload = resp.get("result")
    if not isinstance(payload, dict):
        raise TransientPollError("peer returned a non-task GetTask result")
    return payload


def _short_state(raw_state: str) -> str:
    from . import tools
    return tools._short_state(raw_state) or "unknown"


def _reply_text(payload: dict) -> str:
    from . import tools
    return (tools._reply_text_from_result(payload) or "").strip()


def _age(seconds: float) -> str:
    """Compact elapsed label: 42s, 3m07s, 2h05m."""
    total = max(0, int(seconds))
    if total < 60:
        return f"{total}s"
    minutes, sec = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


# --------------------------------------------------------------------------
# Status line
# --------------------------------------------------------------------------

class WatchStatusLine:
    """One editable status notice per session: what that session's watches are doing.

    Delivery uses the public plugin message surface, not Hermes internals:
    ``PluginContext.post_message`` returns a handle bound to the calling session, and
    ``PluginMessage.edit`` replaces that same sticky notice in place. The handle is created inside
    the tool call, the only place a session is bound, and the poller thread edits it later (edits
    are thread-safe and hop to the gateway loop).

    Everything here is best-effort presentation. A host without ``post_message``, a refused post
    (classic CLI, messaging adapter without editing), or a closed viewer means no line -- never a
    failed watch. The completion delivery is a separate, durable path.
    """

    def __init__(self, manager: "WatchManager", *, clock: Callable[[], float] = time.time,
                 min_interval: float = STATUS_REFRESH_S) -> None:
        self.manager = manager
        self.clock = clock
        self.min_interval = max(0.0, float(min_interval))
        self._handles: dict[str, Any] = {}
        self._text: dict[str, str] = {}
        self._posted_at: dict[str, float] = {}

    # -- publishing -------------------------------------------------------
    def post(self, session_id: str, text: str) -> bool:
        """Create the session's status notice; False when this host cannot show one."""
        ctx = getattr(self.manager, "ctx", None)
        post = getattr(ctx, "post_message", None)
        if not session_id or not text or not callable(post):
            return False
        if self._handles.get(session_id) is not None:
            # A second watch in the same session updates the line instead of stacking a new
            # notice behind the first (the TUI shows one notice at a time anyway).
            return self.refresh(session_id, force=True, text=text)
        try:
            handle = post(text)
        except Exception:
            logger.debug("A2A: status post failed", exc_info=True)
            return False
        if not getattr(handle, "ok", False):
            logger.debug("A2A: status post refused: %s", getattr(handle, "error", "?"))
            return False
        self._handles[session_id] = handle
        self._text[session_id] = text
        self._posted_at[session_id] = self.clock()
        return True

    def live(self, session_id: str) -> bool:
        """Whether this session currently has a status notice we can still edit."""
        return session_id in self._handles

    def refresh(self, session_id: str, *, force: bool = False,
                text: Optional[str] = None) -> bool:
        """Recompute (or accept) the summary and edit the notice when it changed.

        ``force`` is for state changes (a terminal result, a pause, a cancellation): they must
        land now. Without it the edit waits for ``min_interval`` so a 5s poll does not repaint the
        status bar on every pass.
        """
        handle = self._handles.get(session_id)
        if handle is None:
            return False
        text = self.render(session_id) if text is None else text
        if not text or text == self._text.get(session_id):
            return False
        now = self.clock()
        if not force and now - self._posted_at.get(session_id, 0.0) < self.min_interval:
            return False
        try:
            result = handle.edit(text)
        except Exception:
            logger.debug("A2A: status edit failed", exc_info=True)
            return False
        if not isinstance(result, dict) or not result.get("ok"):
            logger.debug("A2A: status edit refused: %s",
                         result.get("error") if isinstance(result, dict) else result)
            return False
        self._text[session_id] = text
        self._posted_at[session_id] = now
        return True

    def render(self, session_id: str) -> str:
        """The line for this session: live watches first, else the last settled one."""
        try:
            active = self.manager.store.status_rows(session_id)
        except Exception:
            logger.debug("A2A: status read failed", exc_info=True)
            return ""
        if active:
            return self._render_active(active, self.clock())
        try:
            last = self.manager.store.last_watch(session_id)
        except Exception:
            return ""
        return self._render_settled(last) if last else ""

    def forget(self, session_id: str) -> None:
        """Drop the handle (the notice itself cannot be cleared by a plugin)."""
        self._handles.pop(session_id, None)
        self._text.pop(session_id, None)
        self._posted_at.pop(session_id, None)

    def reset(self) -> None:
        """Forget every handle: the manager is being torn down or reloaded."""
        self._handles.clear()
        self._text.clear()
        self._posted_at.clear()

    # -- rendering --------------------------------------------------------
    @staticmethod
    def _render_active(watches: list[dict], now: float) -> str:
        items: list[tuple[str, str, str]] = []
        for watch in watches:
            age = _age(now - float(watch.get("created_at") or now))
            for item in watch.get("items") or []:
                peer = str(item.get("peer") or "?")
                raw_state = str(item.get("state") or "")
                state = _short_state(raw_state) if raw_state else "working"
                items.append((peer, state.replace("-", " "), age))
        if not items:
            return ""
        states = {state for _peer, state, _age_text in items}
        if len(items) == 1:
            peer, state, age = items[0]
            return f"⧗ a2a {peer} · {state} · {age}"
        state = states.pop() if len(states) == 1 else "watching"
        shown = items[:STATUS_MAX_PEERS]
        extra = f" +{len(items) - len(shown)}" if len(items) > len(shown) else ""
        peers = " · ".join(f"{peer} {age}" for peer, _state, age in shown)
        return f"⧗ a2a {len(items)} tasks · {state} · {peers}{extra}"

    @staticmethod
    def _render_settled(watch: dict) -> str:
        state = str(watch.get("state") or "")
        if state == WATCH_CANCELED:
            return "⧗ a2a watch stopped"
        if state == WATCH_EXPIRED:
            return "⧗ a2a watch expired"
        outcomes = [(str(item.get("peer") or "?"),
                     _short_state(str(item.get("short_state") or item.get("state") or "")))
                    for item in watch.get("items") or []]
        if not outcomes:
            return ""
        if len(outcomes) == 1:
            peer, item_state = outcomes[0]
            glyph = ("✓" if item_state == "completed"
                     else "✗" if item_state in ("failed", "rejected")
                     else "!" if item_state == "input-required" else "⊘")
            return f"{glyph} a2a {peer} · {item_state or state}"
        shown = outcomes[:STATUS_MAX_PEERS]
        extra = f" +{len(outcomes) - len(shown)}" if len(outcomes) > len(shown) else ""
        states = {item_state for _peer, item_state in outcomes}
        glyph = ("!" if "input-required" in states
                 else "✗" if states & {"failed", "rejected"}
                 else "✓" if states == {"completed"} else "⧗")
        summary = " · ".join(f"{peer} {item_state or 'done'}" for peer, item_state in shown)
        return f"{glyph} a2a {len(outcomes)} done · {summary}{extra}"


# --------------------------------------------------------------------------
# Manager
# --------------------------------------------------------------------------

class WatchManager:
    """One profile-scoped poller for persisted background watches.

    ``poll`` is injectable for tests; production uses :func:`poll_task`. ``clock``/``sleeper`` are
    likewise injectable so a test can drive ``run_once`` deterministically without sleeping.
    ``profile_home`` is the home of the profile that owns the watch store; every background
    operation runs inside :func:`profile_scope` of it.
    """

    def __init__(
        self, ctx: Any, *, store: Optional[WatchStore] = None,
        poll: Callable[[str, str], dict] = poll_task,
        interval: Optional[float] = None, ttl_s: Optional[float] = None,
        clock: Callable[[], float] = time.time,
        profile_home: Optional[str] = None,
    ) -> None:
        self.ctx = ctx
        self.store = store or WatchStore()
        self.poll = poll
        self.clock = clock
        self.profile_home = str(profile_home) if profile_home else None
        self.interval = _env_float("A2A_WATCH_INTERVAL", interval, DEFAULT_POLL_INTERVAL_S,
                                   MIN_POLL_INTERVAL_S, MAX_POLL_INTERVAL_S)
        self.ttl_s = _env_float("A2A_WATCH_TTL", ttl_s, DEFAULT_WATCH_TTL_S, 60.0, 365 * 24 * 3600.0)
        self.owner = f"{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.ownership = ownership.OwnershipStore(self.store.db_path)
        self.unowned_cancel_after_s = _config_float(
            "unowned_cancel_after_s", "A2A_UNOWNED_CANCEL_AFTER", DEFAULT_UNOWNED_CANCEL_AFTER_S)
        self.unowned_ttl_s = _config_float(
            "unowned_task_ttl_s", "A2A_UNOWNED_TASK_TTL", DEFAULT_UNOWNED_TTL_S)
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._lifecycle = threading.Lock()
        self._restart_requested = False
        # Epochs make a queued hand-over belong to one stop(): a later stop() bumps the epoch
        # and invalidates the request, so a poller cannot outlive a stop() that already returned.
        self._stop_epoch = 0
        self._restart_epoch = 0
        # The live status line: one editable notice per session. `_status_dirty` carries the
        # sessions whose state changed this pass so their edit skips the repaint throttle.
        self._status = WatchStatusLine(self, clock=self.clock)
        self._status_dirty: set[str] = set()
        self._status_lock = threading.Lock()

    # -- lifecycle --------------------------------------------------------
    def start(self) -> "WatchManager":
        with self._lifecycle:
            if self._thread is not None and self._thread.is_alive():
                if self._stop.is_set():
                    # stop() could not join the old generation (it is inside a peer request).
                    # Clearing the shared stop flag for a second thread would leave two pollers
                    # under one lease-owner token; defer instead and let the exiting thread
                    # hand over (Amy's PR #9 finding 4).
                    self._restart_requested = True
                    self._restart_epoch = self._stop_epoch
                return self
            self._start_locked()
            return self

    def _start_locked(self) -> None:
        """Start a poller generation. The caller holds ``_lifecycle``."""
        self._stop = threading.Event()
        stop = self._stop
        self._thread = threading.Thread(
            target=self._loop, args=(stop,), name="a2a-watch-manager", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        thread = self._thread
        with self._lifecycle:
            self._stop.set()
            self._stop_epoch += 1
            # A stop after a deferred start cancels the hand-over: the caller wants it stopped.
            self._restart_requested = False
        # The handles belong to this process's gateway loop; a teardown or reload must not edit
        # a notice through a stale target.
        self._status.reset()
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        with self._lifecycle:
            # Keep the handle while the thread is still alive. Dropping it here is what let
            # start() clear the shared stop flag and run a second poller beside the original.
            if self._thread is thread and (thread is None or not thread.is_alive()):
                self._thread = None

    def _loop(self, stop: threading.Event) -> None:
        try:
            while not stop.wait(self.interval):
                try:
                    self.run_once(stop=stop)
                except Exception:  # a watcher bug must not kill the poller
                    logger.exception("A2A watch manager iteration failed")
        finally:
            with self._lifecycle:
                if self._thread is threading.current_thread():
                    self._thread = None
                restart = (self._restart_requested
                           and self._restart_epoch == self._stop_epoch)
                self._restart_requested = False
                if restart:
                    # Hand over under the lock stop() also takes: a stop() that already
                    # returned cannot be undone by this thread spawning a new poller after it.
                    self._start_locked()

    def _scope(self):
        return profile_scope(self.profile_home)

    # -- operations -------------------------------------------------------
    def add(self, *, session_id: str, target: dict,
            tasks: Iterable[tuple[str, str]]) -> dict:
        """Persist a watch, take ownership of its tasks, and start the poller."""
        watch_id = "watch_" + uuid.uuid4().hex[:16]
        items = list(tasks)
        from . import ownership as ownership_mod
        with self._scope():
            # Atomic acquisition: a watch for another session's task can never deliver, and
            # ignoring the failed acquire handed its backlog to the wrong session.
            owners_before = self.ownership.owners([task_id for _peer, task_id in items])
            for _peer, task_id in items:
                owner = owners_before.get(task_id, "")
                if owner and owner != (session_id or ""):
                    raise ownership_mod.OwnershipConflict(task_id, owner)
            self.store.create(watch_id=watch_id, session_id=session_id or "", target=target,
                              tasks=items, ttl_s=self.ttl_s)
            # Ownership is a session fact: a watch is only created for tasks this session may
            # deliver. The caller refuses another session's tasks first (tools._acquire_task); the
            # acquire here covers direct manager use and makes the record exist before the first
            # poll so messages have somewhere to land.
            task_store = protocol.TaskStore(self.store.db_path)
            # Rollback is limited to ownership THIS call created. Releasing a task the session
            # already owned would drop that task's pending publication and break an existing
            # watch for a batch that failed on a later item (Amy's PR #9 finding 2).
            newly_acquired: list[str] = []
            for peer, task_id in items:
                if task_store.get(task_id) is None:
                    task_store.create(task_id, task_id, peer, direction="outbound",
                                      remote_state=protocol.STATE_SUBMITTED)
                if session_id and not self.ownership.acquire(task_id, session_id):
                    # Undo only what THIS call took: a task it already owned has a live watch
                    # and pending delivery of its own.
                    for claimed in newly_acquired:
                        self.ownership.release(claimed, session_id)
                    self.store.cancel(watch_id)
                    raise ownership_mod.OwnershipConflict(task_id, self.ownership.owner(task_id))
                if session_id and owners_before.get(task_id, "") != session_id:
                    newly_acquired.append(task_id)
            watch = self.store.get(watch_id) or {"watch_id": watch_id}
        self.start()
        # Post the live line from INSIDE the tool call: this is the only context where a session
        # is bound, and the returned handle is what the poller edits afterwards.
        if str(session_id or "") and callable(getattr(self.ctx, "post_message", None)):
            self._status.post(str(session_id), self._status.render(str(session_id)))
        return watch

    def status_live(self, session_id: str) -> bool:
        """Whether this session has a live status notice (diagnostics and the tool reply)."""
        return self._status.live(str(session_id or ""))

    # -- status line ------------------------------------------------------
    def _mark_status_dirty(self, session_id: str) -> None:
        if not session_id:
            return
        with self._status_lock:
            self._status_dirty.add(session_id)

    def _refresh_status(self) -> None:
        """Refresh every session with live work, forcing the ones whose state just changed."""
        with self._status_lock:
            dirty = set(self._status_dirty)
            self._status_dirty.clear()
        try:
            sessions = set(self.store.active_sessions())
        except Exception:
            logger.debug("A2A: status session probe failed", exc_info=True)
            sessions = set()
        # A session whose last watch just settled has no active row left; `dirty` still names it
        # so its final line lands now rather than on the next poll.
        for session_id in sessions | dirty:
            if not self._status.live(session_id):
                continue
            self._status.refresh(session_id, force=session_id in dirty)

    def publish_if_owned(self, session_id: str, task_id: str, publish,
                         mark_messages: Iterable[str] = (),
                         expected_text: Optional[str] = None) -> bool:
        """One publish under the store write lock, only while this session still owns the task."""
        return self.store.publish_if_owned(session_id, task_id, publish,
                                           mark_messages=mark_messages,
                                           expected_text=expected_text)

    def cancel(self, watch_id: str, *, session_id: str = "") -> Optional[str]:
        with self._scope():
            watch = self.store.get(watch_id) or {}
            prior = self.store.cancel(watch_id, session_id=session_id)
        if prior == WATCH_ACTIVE:
            # The caller is in a tool call with a live viewer: show the stop now.
            self._status.refresh(str(watch.get("session_id") or session_id or ""), force=True)
        return prior

    def cancel_session(self, session_id: str, task_ids: Optional[Iterable[str]] = None) -> int:
        """Stop polling what a session no longer owns (session end or an ownership release)."""
        with self._scope():
            stopped = self.store.cancel_session(session_id, task_ids=task_ids)
        if stopped:
            self._status.refresh(str(session_id or ""), force=True)
        return stopped

    def rehydrate(self) -> int:
        """Resume persisted watches, undelivered publications and unowned-task timers.

        Read-only probe first: a plugin load with nothing to resume must not create or migrate the
        profile's task database as a side effect. Unowned-but-released tasks count as work: the
        auto-cancel and TTL timers only run in the poller, so a restart must not forget them.
        """
        with self._scope():
            pending = self.store.resumable_count_readonly()
            try:
                pending += self.ownership.unowned_count_readonly()
            except Exception:
                logger.debug("A2A: unowned task probe failed", exc_info=True)
        if pending:
            self.start()
        return pending

    def run_once(self, now: Optional[float] = None, *,
                 stop: Optional[threading.Event] = None) -> int:
        """Poll every due watch and drain the outbox; returns the number of tasks polled.

        ``stop`` is the generation event of the calling poller. A restarted manager gets a fresh
        event, so an in-flight iteration from the old generation must check the one it started
        with, never ``self._stop``.
        """
        polls = 0
        stop = self._stop if stop is None else stop
        with self._scope():
            now = self.clock() if now is None else now
            for expired in self.store.expire(now):
                # Enqueue first (idempotent per event id), then close: a crash between the two
                # leaves the watch in 'expiring', which the next pass finishes.
                self._enqueue_expiry(expired)
                self.store.mark_expired(expired["watch_id"], now)
                self._mark_status_dirty(str(expired.get("session_id") or ""))
            for watch in self.store.due(now):
                if not self.store.claim(watch["watch_id"], self.owner, now):
                    continue
                try:
                    polls += self._poll_watch(watch, now, stop)
                finally:
                    self.store.release(watch["watch_id"], self.owner)
            self.flush_publications(now)
            self._maintain_unowned(now)
            self._refresh_status()
            self.store.prune(now)
        return polls

    def flush_publications(self, now: Optional[float] = None) -> int:
        """Attempt every due outbox row; returns how many the host accepted.

        Runs inside the caller's profile scope (``run_once`` opens it) and outside any watch lease:
        a delivery must not be lost just because the watch that produced it has closed.
        """
        now = self.clock() if now is None else now
        published = 0
        for due in self.store.due_publications(now):
            event_id = str(due["event_id"])
            with self.store.publication_guard(event_id) as (con, row):
                if row is None:
                    continue  # released or already delivered while this pass was running
                try:
                    target = _load_target(row.get("target_json"))
                except WatchError:
                    logger.error("A2A watch %s has an unreadable target; dropping %s",
                                 row.get("watch_id"), event_id)
                    con.execute(
                        "UPDATE a2a_watch_publications SET published_at=? WHERE event_id=?",
                        (now, event_id))
                    continue
                from . import tools as _tools
                meta = _load_metadata(row.get("metadata_json"))
                if self._message_content_changed(con, meta, row["content"]):
                    # The artifact was revised after this row was written; the poll refreshes the
                    # row. Delivering now would retire it with content the host already outgrew.
                    logger.debug("A2A watch: %s carries an older revision; leaving it pending",
                                 event_id)
                    continue
                if self._message_already_delivered(con, meta):
                    # A direct backlog replay accepted this durable message after this row was
                    # written. The backlog mark is authoritative; injecting again would duplicate.
                    logger.info("A2A watch: %s was already delivered by a backlog replay", event_id)
                    con.execute(
                        "UPDATE a2a_watch_publications SET published_at=?, next_attempt_at=0 "
                        "WHERE event_id=?", (now, event_id))
                    continue
                attempts = int(row.get("attempts", 0))
                try:
                    accepted = _tools._inject(self.ctx, target, row["content"])
                except Exception as exc:  # the host itself raised: treat as "not now"
                    logger.warning("A2A watch could not publish %s: %s", event_id, exc)
                    accepted = False
                if not accepted:
                    if attempts + 1 >= _MAX_PUBLICATION_ATTEMPTS:
                        # A host that refuses this many times (no live session, missing
                        # allow_gateway_injection consent, a closed CLI) will not take it later.
                        # Retire the row so it cannot spin forever; the watch's own notice path
                        # reports the loss on its next terminal event.
                        logger.error("A2A watch %s gave up delivering %s after %d attempts",
                                     row.get("watch_id"), event_id, attempts + 1)
                        con.execute(
                            "UPDATE a2a_watch_publications SET published_at=? WHERE event_id=?",
                            (now, event_id))
                        continue
                    con.execute(
                        """UPDATE a2a_watch_publications
                              SET attempts=attempts+1, next_attempt_at=? WHERE event_id=?""",
                        (now + _publication_backoff(attempts + 1), event_id))
                    continue
                con.execute(
                    "UPDATE a2a_watch_publications SET published_at=?, next_attempt_at=0 "
                    "WHERE event_id=?", (now, event_id))
                if meta.get("peer") and meta.get("task_id") and meta.get("state"):
                    con.execute(
                        """UPDATE a2a_watch_items SET published_state=?, updated_at=?
                            WHERE watch_id=? AND peer=? AND task_id=?""",
                        (str(meta["state"]), now, row.get("watch_id"),
                         str(meta["peer"]), str(meta["task_id"])))
                # Mark the backlog row in the SAME transaction as the accepted publication: a
                # release between commit and a separate mark would let the next owner replay a
                # message this session already received.
                task_id = str(meta.get("task_id") or "")
                message_id = str(meta.get("message_id") or "")
                session_id = str(meta.get("session_id") or "")
                if task_id and message_id and session_id:
                    con.execute(
                        """UPDATE a2a_task_messages
                              SET delivered_at=?, delivered_to_session=?
                            WHERE task_id=? AND message_id=? AND delivered_at IS NULL""",
                        (now, session_id, task_id, message_id))
                published += 1
        return published

    @staticmethod
    def _message_content_changed(con: sqlite3.Connection, meta: dict, content: str) -> bool:
        """True when the outbox row's body no longer matches the artifact's current revision.

        The outbox row is written after the artifact is recorded, but a concurrent poll can revise
        the artifact before this flush runs. Injecting the older body would retire the row with
        text the host already outgrew (Amy round 2, P1); the poll refreshes the row instead.
        """
        task_id = str(meta.get("task_id") or "")
        message_id = str(meta.get("message_id") or "")
        if not task_id or not message_id:
            return False
        row = con.execute(
            "SELECT text, kind FROM a2a_task_messages WHERE task_id=? AND message_id=?",
            (task_id, message_id)).fetchone()
        if row is None:
            return False
        current = _message_content(str(meta.get("peer") or ""), task_id,
                                   str(row["kind"] or meta.get("kind") or "progress"),
                                   str(row["text"] or ""))
        return current != content

    @staticmethod
    def _message_already_delivered(con: sqlite3.Connection, meta: dict) -> bool:
        """True when the outbox row's durable message was already accepted by another path."""
        task_id = str(meta.get("task_id") or "")
        message_id = str(meta.get("message_id") or "")
        if not task_id or not message_id:
            return False
        row = con.execute(
            """SELECT 1 FROM a2a_task_messages
                WHERE task_id=? AND message_id=? AND delivered_at IS NOT NULL""",
            (task_id, message_id)).fetchone()
        return row is not None

    # -- internals --------------------------------------------------------
    def _poll_watch(self, watch: dict, now: float, stop: threading.Event) -> int:
        watch_id = watch["watch_id"]
        polls = 0
        items = [dict(r) for r in con_items(self.store.db_path, watch_id)]
        for item in items:
            if stop.is_set():
                return polls  # unload asked us to stop; never start another poll
            if item["completed_at"] is not None:
                continue
            if item["state"] in (ITEM_DONE, ITEM_PAUSED, ITEM_UNWATCHED):
                continue
            if item["next_poll_at"] > now:
                continue
            # Renew before each task: one lease must cover an unbounded batch, and a lost lease
            # means another process owns this watch now -- stop rather than write stale results.
            if not self.store.renew(watch_id, self.owner, self.clock()):
                logger.debug("A2A watch %s lease lost before %s/%s", watch_id,
                             item["peer"], item["task_id"])
                return polls
            polls += 1
            try:
                payload = self.poll(item["peer"], item["task_id"])
            except TransientPollError as exc:
                errors = self.store.record_error(
                    watch_id, item["peer"], item["task_id"], str(exc),
                    next_poll_at=now + _backoff(item["errors"] + 1, self.interval),
                    owner=self.owner)
                if errors >= MAX_CONSECUTIVE_ERRORS:
                    self._unwatch_item(watch, item, errors, str(exc))
                continue
            except Exception as exc:  # unexpected: treat as transient, never as remote failure
                logger.debug("A2A watch poll failed: %s", exc, exc_info=True)
                self.store.record_error(
                    watch_id, item["peer"], item["task_id"], str(exc),
                    next_poll_at=now + _backoff(item["errors"] + 1, self.interval),
                    owner=self.owner)
                continue
            # The poll can outlive the lease: another process then owns this watch. Re-check
            # before the observation is folded, so a late response cannot record or publish a
            # result the new owner has already superseded.
            if stop.is_set() or not self.store.renew(watch_id, self.owner, self.clock()):
                return polls
            self._apply_payload(watch, item, payload, now)
        self.store.settle(watch_id, now=now)
        return polls

    def _apply_payload(self, watch: dict, item: dict, payload: dict, now: float) -> None:
        """Fold one authoritative task payload into local state and deliver what it contains.

        Every peer message the payload carries (progress note, artifact/result, input request) is
        recorded on the task once, keyed by its message id. While this session owns the task the
        new messages are published to it; while the task is unowned they stay queued for whoever
        adopts it.
        """
        raw_state = str((payload.get("status") or {}).get("state") or "")
        context_id = str(payload.get("contextId") or "")
        task_id, peer, watch_id = item["task_id"], item["peer"], watch["watch_id"]
        session_id = str(watch.get("session_id") or "")
        text = _reply_text(payload)
        terminal = raw_state in protocol.TERMINAL_STATES
        if terminal:
            # The row must exist before the fenced state update; creating a missing row cannot
            # clobber an adopter's record because there is none.
            self._ensure_task_record(task_id, context_id, peer, raw_state)

        queued = self._record_messages(
            task_id, peer, payload, raw_state, session_id,
            state=raw_state if terminal else "", reply=text if terminal else "")
        if queued is None:
            # Ownership moved while this poll was in flight. Recording and the task-state
            # transition share the fence, so nothing was written, including the reply. This
            # watch's session no longer owns the task: stop polling it.
            self.store.mark_item(watch_id, peer, task_id, ITEM_UNWATCHED, owner=self.owner)
            logger.debug("A2A watch: %s/%s changed owner; observation not recorded",
                         task_id, peer)
            return
        if terminal:
            # The durable state/reply committed with the observation; only the transcript mirror
            # and the status line remain, and only for an authorized poll.
            self._mirror_terminal(task_id, text, context_id, peer)
            self._mark_status_dirty(session_id)
        elif raw_state == protocol.STATE_INPUT_REQUIRED:
            self._mark_status_dirty(session_id)
        else:
            self.store.record_working(watch_id, peer, task_id, raw_state, context_id,
                                      next_poll_at=now + self.interval, owner=self.owner)
        delivered: list[str] = []
        enqueue_failed = False
        # Artifacts are deliverables, not progress notes: a streaming peer that grows one
        # artifact on every poll must not fan out a message per revision (local review, D2).
        # They stay queued, and the row's latest revision is published when the task turns
        # terminal or needs input.
        publish_results = (raw_state in protocol.TERMINAL_STATES
                           or raw_state == protocol.STATE_INPUT_REQUIRED)
        if session_id and self.ownership.owner(task_id) == session_id:
            for msg in queued:
                if str(msg.get("kind") or "") == "result" and not publish_results:
                    continue
                try:
                    self.store.enqueue_publication(
                        watch_id=watch_id, event_id=f"a2a:{task_id}:{msg['message_id']}",
                        content=_message_content(peer, task_id, str(msg["kind"]),
                                                 str(msg["text"])),
                        metadata={"source": "a2a", "peer": peer, "task_id": task_id,
                                  "watch_id": watch_id, "state": _short_state(raw_state),
                                  "kind": msg["kind"], "message_id": msg["message_id"],
                                  "session_id": session_id})
                except Exception:
                    logger.exception("A2A: could not enqueue %s/%s", task_id, msg["message_id"])
                    enqueue_failed = True
                    continue
                delivered.append(str(msg["message_id"]))
            # Messages stay queued until the host ACCEPTS them (flush_publications marks them):
            # a refused publish must not look delivered, and a release must leave them for the
            # next owner instead of publishing them into the session that gave the task up.
        # The item is marked done only after its message is durably in the outbox. A crash or a
        # failed enqueue leaves the item open, so the next poll retries it instead of losing the
        # completion with the item already closed.
        if raw_state in protocol.TERMINAL_STATES and not enqueue_failed:
            self.store.mark_item(watch_id, peer, task_id, ITEM_DONE,
                                 short_state=_short_state(raw_state), owner=self.owner)
        elif raw_state == protocol.STATE_INPUT_REQUIRED and not enqueue_failed:
            # Non-terminal by design: the peer needs a follow-up before it can finish. Only now,
            # with the notice queued, is it safe to stop polling this task.
            self.store.record_working(watch_id, peer, task_id, raw_state, context_id,
                                      next_poll_at=float("inf"), owner=self.owner)
            self.store.mark_item(watch_id, peer, task_id, ITEM_PAUSED,
                                 short_state=_short_state(raw_state), owner=self.owner)

    def _record_messages(self, task_id: str, peer: str, payload: dict,
                         raw_state: str, session_id: str = "",
                         *, state: str = "", reply: str = "") -> Optional[list[dict]]:
        """Store every new peer message on the task; returns the undelivered rows.

        ``None`` means another session owns the task now: the observation is refused and nothing
        is written, so an adopted task's queued answer cannot be overwritten by this watch's
        stale poll. An unowned task still records (its backlog waits for the adopter).
        """
        from . import tools
        entries: list[tuple[str, str, str, bool]] = []
        # History first (chronology), peer-originated only: ROLE_USER entries are OUR requests,
        # not peer progress, and status/artifacts are the newest messages.
        for index, message in enumerate(payload.get("history") or []):
            if not tools._peer_role(message.get("role")):
                continue
            text = tools._text_of_message(message)
            if not text:
                continue
            message_id = str(message.get("messageId") or message.get("message_id")
                             or f"history:{index}:{tools._digest(text)}")
            entries.append((message_id, "progress", text, False))
        status_msg = (payload.get("status") or {}).get("message") or {}
        status_text = tools._text_of_message(status_msg)
        if status_text:
            kind = "input-required" if raw_state == protocol.STATE_INPUT_REQUIRED else "progress"
            message_id = str(status_msg.get("messageId") or status_msg.get("message_id")
                             or f"status:{tools._digest(status_text)}")
            entries.append((message_id, kind, status_text, False))
        for index, artifact in enumerate(payload.get("artifacts") or []):
            text = tools._text_of_message(artifact)
            if not text:
                continue
            kind = "input-required" if raw_state == protocol.STATE_INPUT_REQUIRED else "result"
            entries.append((tools._artifact_message_id(artifact, index, text), kind, text, True))
        if (raw_state in protocol.TERMINAL_STATES
                and not any(kind == "result" for _mid, kind, _text, _update in entries)
                and not self.ownership.has_kind(task_id, "result")):
            entries.append((f"state:{raw_state}", "result",
                            f"task reached {_short_state(raw_state)} with no text reply", False))
        return self.ownership.record_messages(task_id, entries, session_id=session_id,
                                              state=state, reply=reply)

    def _maintain_unowned(self, now: float) -> None:
        """Cancel unowned tasks after the configured delay, abandon them after the TTL."""
        from . import tools
        try:
            cancel, abandon = self.ownership.unowned_due(
                now, cancel_after_s=self.unowned_cancel_after_s, ttl_s=self.unowned_ttl_s)
        except Exception:
            logger.debug("A2A: unowned task sweep failed", exc_info=True)
            return
        for rec in cancel:
            task_id = str(rec["task_id"])
            # Fence first: the selection is a snapshot, and an adoption may have happened since.
            # Claiming the action atomically is what proves the task is still unowned.
            if not self.ownership.mark_cancel_requested(task_id, now, rec.get("released_at")):
                continue
            peer_name = str(rec["peer"] or "")
            peer = tools._resolve_peer(peer_name)
            if peer and peer.get("url"):
                try:
                    tools._rpc_request(peer, "CancelTask", {"id": task_id}, timeout=15)
                    logger.info("A2A: cancelled unowned task %s on %s", task_id, peer_name)
                except Exception as exc:
                    logger.debug("A2A: CancelTask for unowned %s failed: %s", task_id, exc)
            self.ownership.add_message(
                task_id, f"auto-cancel:{int(now)}", "error",
                "This task was unowned for longer than the auto-cancellation timeout, so the "
                "watcher asked the peer to cancel it. Its remote state may still be working.")
        for rec in abandon:
            task_id = str(rec["task_id"])
            if self.ownership.abandon(task_id, now, rec.get("released_at")):
                logger.info("A2A: abandoned unowned task %s after the TTL", task_id)

    def _ensure_task_record(self, task_id: str, context_id: str, peer: str,
                            raw_state: str) -> None:
        """Create the shared row when a watch predates it; never touch an existing record."""
        store = protocol.TaskStore(self.store.db_path)
        if store.get(task_id) is None:
            store.create(task_id, context_id or task_id, peer, direction="outbound",
                         ownership="remote", remote_state=raw_state)

    def _mirror_terminal(self, task_id: str, text: str, context_id: str, peer: str) -> None:
        """Mirror an authorized terminal observation into the transcript.

        The task state and reply were written by the fenced recording transaction; this only
        persists the peer text for replay and is best-effort.
        """
        if context_id and text:
            self._persist_peer_text(context_id, text, task_id, peer)

    def _persist_peer_text(self, context_id: str, text: str, task_id: str, peer: str) -> None:
        from . import security
        try:
            protocol.persist_message(context_id, "agent", security.filter_inbound(text),
                                     task_id, peer=peer)
        except Exception:  # transcript persistence is best-effort, never blocks the notification
            logger.debug("A2A watch could not persist peer reply", exc_info=True)

    def _unwatch_item(self, watch: dict, item: dict, errors: int, message: str) -> None:
        """Stop watching a task after repeated polling failures; never call the task failed."""
        self._enqueue(
            watch, item, suffix="watch-error",
            content=_watch_problem_content(
                item["peer"], item["task_id"], watch["watch_id"],
                f"stopped polling after {errors} consecutive errors: {message}",
                item["state"]),
            metadata={"source": "a2a", "peer": item["peer"], "task_id": item["task_id"],
                      "watch_id": watch["watch_id"], "state": "watch-error",
                      "remote_state": item["state"], "error": message[:300]})
        self.store.mark_item(watch["watch_id"], item["peer"], item["task_id"], ITEM_UNWATCHED,
                             owner=self.owner)
        self._mark_status_dirty(str(watch.get("session_id") or ""))

    def _enqueue_expiry(self, watch: dict) -> None:
        watch_id = watch["watch_id"]
        pending = [i for i in watch.get("items", [])
                   if i.get("completed_at") is None and i.get("state") != ITEM_DONE]
        if not pending:
            return
        if len(pending) == 1:
            item = pending[0]
            self.store.enqueue_publication(
                watch_id=watch_id,
                event_id=f"a2a:{watch_id}:{item['peer']}:{item['task_id']}:watch-expired",
                content=_watch_problem_content(
                    item["peer"], item["task_id"], watch_id,
                    "the local watch expired before the peer reported a terminal state",
                    item.get("state") or ""),
                metadata={"source": "a2a", "peer": item["peer"], "task_id": item["task_id"],
                          "watch_id": watch_id, "state": "watch-expired",
                          "remote_state": item.get("state") or ""})
            return
        listed = ", ".join(f"{i['peer']}/{i['task_id']}" for i in pending)
        self.store.enqueue_publication(
            watch_id=watch_id, event_id=f"a2a:{watch_id}:watch-expired",
            content=(f"A2A background watch {watch_id} expired before these tasks reached a "
                     f"terminal state: {listed}.\nThe remote tasks were not cancelled; poll "
                     f"a2a_get_task or start a new watch if you still need them."),
            metadata={"source": "a2a", "watch_id": watch_id, "state": "watch-expired",
                      "tasks": [f"{i['peer']}/{i['task_id']}" for i in pending]})

    def _enqueue(self, watch: dict, item: dict, *, suffix: str, content: str,
                 metadata: dict) -> None:
        """Write the event to the outbox; delivery (and retry) is the outbox's job."""
        event_id = f"a2a:{watch['watch_id']}:{item['peer']}:{item['task_id']}:{suffix}"
        self.store.enqueue_publication(watch_id=watch["watch_id"], event_id=event_id,
                                       content=content, metadata=metadata)


# --------------------------------------------------------------------------
# Formatting
# --------------------------------------------------------------------------

def _message_content(peer: str, task_id: str, kind: str, text: str) -> str:
    from . import tools
    return tools._peer_message_content(peer, task_id, kind, text)


def _config_float(key: str, env_name: str, default: float) -> float:
    """A tuning value from ``a2a.<key>`` in config.yaml, else the env var, else the default."""
    try:
        from . import tools
        section = (tools._load_config() or {}).get("a2a") or {}
        raw = section.get(key)
    except Exception:
        raw = None
    if raw in (None, ""):
        raw = os.getenv(env_name, "").strip()
    try:
        value = float(raw) if raw not in (None, "") else default
    except (TypeError, ValueError):
        value = default
    return max(60.0, value)


def _terminal_content(peer: str, task_id: str, short: str, text: str) -> str:
    body = (text or "(the peer returned no text reply)")[:MAX_REPLY_CHARS]
    return (f"A2A task {task_id} on peer '{peer}' reached a terminal state: {short}.\n\n"
            f"{body}")


def _input_required_content(peer: str, task_id: str, text: str, context_id: str) -> str:
    body = (text or "")[:MAX_REPLY_CHARS]
    hint = (f"Reply with a2a_submit(agent='{peer}', context_id='{context_id}', message=...)."
            if context_id else "Reply with a2a_submit using the task's context id.")
    return (f"A2A task {task_id} on peer '{peer}' needs input before it can finish.\n{hint}\n\n"
            f"{body}").strip()


def _watch_problem_content(peer: str, task_id: str, watch_id: str, problem: str,
                           remote_state: str) -> str:
    state_note = f" Last observed remote state: {remote_state or 'unknown'}." if remote_state else ""
    return (f"A2A background watch {watch_id} {problem} for task {task_id} on peer '{peer}'."
            f"{state_note} This describes the watcher, not the remote task: the task was not "
            f"cancelled or marked failed. Use a2a_get_task to check it directly.")


def _backoff(errors: int, interval: float) -> float:
    return min(MAX_BACKOFF_S, max(interval, interval * (2 ** min(errors, 6))))


# A host that refuses delivery this many times (no live session, no gateway-injection consent, a
# closed CLI) is not coming back within the watch's life; retire the row instead of spinning.
_MAX_PUBLICATION_ATTEMPTS = 12


def _publication_backoff(attempts: int) -> float:
    return min(MAX_PUBLICATION_BACKOFF_S, PUBLICATION_BACKOFF_S * (2 ** min(max(attempts, 1), 6)))


def _env_float(name: str, override: Optional[float], default: float,
               minimum: float, maximum: float) -> float:
    raw: Any = override
    if raw is None:
        raw = os.getenv(name, "").strip()
    try:
        value = float(raw) if raw not in (None, "") else default
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))
