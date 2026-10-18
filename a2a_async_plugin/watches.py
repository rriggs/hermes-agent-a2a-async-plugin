"""Durable background A2A watches.

``a2a_await(background=True)`` cannot block the agent turn: it captures a completion target from
the invoking tool call, stores a watch in the profile's ``a2a_tasks.db``, and returns a ``watch_id``.
A single profile-scoped thread polls the watched tasks and, on an authoritative terminal state,
publishes one idempotent completion through ``PluginContext.publish_completion`` — the supported
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
                      SET state=?, context_id=COALESCE(NULLIF(?, ''), context_id),
                          errors=0, next_poll_at=?, updated_at=?
                    WHERE watch_id=? AND peer=? AND task_id=?{fence_sql}""",
                (state or "", context_id or "", next_poll_at, now, watch_id, peer, task_id,
                 *fence_params))
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
                  published_state: str = "", owner: Optional[str] = None) -> None:
        now = time.time()
        fence_sql, fence_params = self._fence(owner)
        with self._session() as con:
            con.execute(
                f"""UPDATE a2a_watch_items
                      SET state=?, published_state=CASE WHEN ?='' THEN published_state ELSE ? END,
                          errors=0, next_poll_at=?, completed_at=?, updated_at=?
                    WHERE watch_id=? AND peer=? AND task_id=?{fence_sql}""",
                (item_state, published_state, published_state, now, now, now,
                 watch_id, peer, task_id, *fence_params))
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
                for task_id in ids:
                    total += con.execute(
                        """DELETE FROM a2a_watch_publications
                            WHERE published_at IS NULL
                              AND (json_extract(metadata_json, '$.task_id')=?
                                   OR EXISTS (SELECT 1 FROM json_each(metadata_json, '$.tasks')
                                               WHERE json_each.value=?))""",
                        (task_id, task_id)).rowcount
                return total
            return cur.rowcount

    # -- publication outbox ----------------------------------------------
    def enqueue_publication(self, *, watch_id: str, event_id: str, content: str,
                            metadata: dict) -> None:
        """Persist an event for delivery. Idempotent per event id; the row IS the retry state."""
        now = time.time()
        with self._session() as con:
            con.execute(
                """INSERT OR IGNORE INTO a2a_watch_publications
                       (event_id, watch_id, content, metadata_json, created_at, next_attempt_at)
                   VALUES (?, ?, ?, ?, ?, 0)""",
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
                """SELECT p.*, w.target_json FROM a2a_watch_publications p
                     JOIN a2a_watches w ON w.watch_id = p.watch_id
                    WHERE p.event_id=? AND p.published_at IS NULL""",
                (event_id,)).fetchone()
            yield con, (dict(row) if row else None)
        except Exception:
            con.rollback()
            raise
        else:
            con.commit()
        finally:
            con.close()

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

    # -- lifecycle --------------------------------------------------------
    def start(self) -> "WatchManager":
        if self._thread is not None and self._thread.is_alive():
            return self
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="a2a-watch-manager", daemon=True)
        self._thread.start()
        return self

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._thread = None

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.run_once()
            except Exception:  # a watcher bug must not kill the poller
                logger.exception("A2A watch manager iteration failed")

    def _scope(self):
        return profile_scope(self.profile_home)

    # -- operations -------------------------------------------------------
    def add(self, *, session_id: str, target: dict,
            tasks: Iterable[tuple[str, str]]) -> dict:
        """Persist a watch, take ownership of its tasks, and start the poller."""
        watch_id = "watch_" + uuid.uuid4().hex[:16]
        items = list(tasks)
        with self._scope():
            self.store.create(watch_id=watch_id, session_id=session_id or "", target=target,
                              tasks=items, ttl_s=self.ttl_s)
            # Ownership is a session fact: a watch is only created for tasks this session may
            # deliver. The caller refuses another session's tasks first (tools._acquire_task); the
            # acquire here covers direct manager use and makes the record exist before the first
            # poll so messages have somewhere to land.
            task_store = protocol.TaskStore(self.store.db_path)
            for peer, task_id in items:
                if task_store.get(task_id) is None:
                    task_store.create(task_id, task_id, peer, direction="outbound",
                                      remote_state=protocol.STATE_SUBMITTED)
                if session_id:
                    self.ownership.acquire(task_id, session_id)
            watch = self.store.get(watch_id) or {"watch_id": watch_id}
        self.start()
        return watch

    def cancel(self, watch_id: str, *, session_id: str = "") -> Optional[str]:
        with self._scope():
            return self.store.cancel(watch_id, session_id=session_id)

    def cancel_session(self, session_id: str, task_ids: Optional[Iterable[str]] = None) -> int:
        """Stop polling what a session no longer owns (session end or an ownership release)."""
        with self._scope():
            return self.store.cancel_session(session_id, task_ids=task_ids)

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

    def run_once(self, now: Optional[float] = None) -> int:
        """Poll every due watch and drain the outbox; returns the number of tasks polled."""
        polls = 0
        with self._scope():
            now = self.clock() if now is None else now
            for expired in self.store.expire(now):
                # Enqueue first (idempotent per event id), then close: a crash between the two
                # leaves the watch in 'expiring', which the next pass finishes.
                self._enqueue_expiry(expired)
                self.store.mark_expired(expired["watch_id"], now)
            for watch in self.store.due(now):
                if not self.store.claim(watch["watch_id"], self.owner, now):
                    continue
                try:
                    polls += self._poll_watch(watch, now)
                finally:
                    self.store.release(watch["watch_id"], self.owner)
            self.flush_publications(now)
            self._maintain_unowned(now)
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
                try:
                    self.ctx.publish_completion(
                        target=target, event_id=event_id, content=row["content"],
                        metadata=_load_metadata(row.get("metadata_json")))
                except Exception as exc:
                    name = type(exc).__name__
                    if name.startswith("Completion"):
                        # The publisher refused this event permanently (content or target
                        # validation). Retrying cannot succeed; retire the row and say so.
                        logger.error("A2A watch %s was refused permanently (%s); dropping %s",
                                     row.get("watch_id"), name, event_id)
                        con.execute(
                            "UPDATE a2a_watch_publications SET published_at=? WHERE event_id=?",
                            (now, event_id))
                        continue
                    con.execute(
                        """UPDATE a2a_watch_publications
                              SET attempts=attempts+1, next_attempt_at=? WHERE event_id=?""",
                        (now + _publication_backoff(int(row.get("attempts", 0)) + 1), event_id))
                    logger.warning("A2A watch could not publish %s: %s", event_id, exc)
                    continue
                con.execute(
                    "UPDATE a2a_watch_publications SET published_at=?, next_attempt_at=0 "
                    "WHERE event_id=?", (now, event_id))
                meta = _load_metadata(row.get("metadata_json"))
                if meta.get("peer") and meta.get("task_id") and meta.get("state"):
                    con.execute(
                        """UPDATE a2a_watch_items SET published_state=?, updated_at=?
                            WHERE watch_id=? AND peer=? AND task_id=?""",
                        (str(meta["state"]), now, row.get("watch_id"),
                         str(meta["peer"]), str(meta["task_id"])))
                published += 1
            meta = _load_metadata(row.get("metadata_json"))
            task_id = str(meta.get("task_id") or "")
            message_id = str(meta.get("message_id") or "")
            session_id = str(meta.get("session_id") or "")
            if task_id and message_id and session_id:
                try:
                    self.ownership.mark_delivered(task_id, [message_id], session_id, now)
                except Exception:
                    logger.debug("A2A: delivery marker failed for %s", event_id, exc_info=True)
        return published

    # -- internals --------------------------------------------------------
    def _poll_watch(self, watch: dict, now: float) -> int:
        watch_id = watch["watch_id"]
        polls = 0
        items = [dict(r) for r in con_items(self.store.db_path, watch_id)]
        for item in items:
            if self._stop.is_set():
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
            if self._stop.is_set() or not self.store.renew(watch_id, self.owner, self.clock()):
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
        text = _reply_text(payload)

        if raw_state in protocol.TERMINAL_STATES:
            self._reconcile_local_task(task_id, raw_state, text, context_id, peer)
        elif raw_state == protocol.STATE_INPUT_REQUIRED:
            pass  # paused after the notice is durably queued (a failed enqueue must retry)
        else:
            self.store.record_working(watch_id, peer, task_id, raw_state, context_id,
                                      next_poll_at=now + self.interval, owner=self.owner)

        queued = self._record_messages(task_id, peer, payload, raw_state)
        session_id = str(watch.get("session_id") or "")
        delivered: list[str] = []
        enqueue_failed = False
        if session_id and self.ownership.owner(task_id) == session_id:
            for msg in queued:
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
            self.store.mark_item(watch_id, peer, task_id, ITEM_DONE, owner=self.owner)
        elif raw_state == protocol.STATE_INPUT_REQUIRED and not enqueue_failed:
            # Non-terminal by design: the peer needs a follow-up before it can finish. Only now,
            # with the notice queued, is it safe to stop polling this task.
            self.store.record_working(watch_id, peer, task_id, raw_state, context_id,
                                      next_poll_at=float("inf"), owner=self.owner)
            self.store.mark_item(watch_id, peer, task_id, ITEM_PAUSED, owner=self.owner)

    def _record_messages(self, task_id: str, peer: str, payload: dict,
                         raw_state: str) -> list[dict]:
        """Store every new peer message on the task; returns the still-undelivered ones."""
        from . import tools
        status_msg = (payload.get("status") or {}).get("message") or {}
        status_text = tools._text_of_message(status_msg)
        if status_text:
            kind = "input-required" if raw_state == protocol.STATE_INPUT_REQUIRED else "progress"
            message_id = str(status_msg.get("messageId") or status_msg.get("message_id")
                             or f"status:{tools._digest(status_text)}")
            self.ownership.add_message(task_id, message_id, kind, status_text)
        for index, artifact in enumerate(payload.get("artifacts") or []):
            text = tools._text_of_message(artifact)
            if not text:
                continue
            artifact_id = str(artifact.get("artifactId") or artifact.get("artifact_id")
                              or f"artifact:{index}:{tools._digest(text)}")
            kind = "input-required" if raw_state == protocol.STATE_INPUT_REQUIRED else "result"
            self.ownership.add_message(task_id, artifact_id, kind, text)
        if raw_state in protocol.TERMINAL_STATES and not self.ownership.has_kind(task_id, "result"):
            self.ownership.add_message(
                task_id, f"state:{raw_state}", "result",
                f"task reached {_short_state(raw_state)} with no text reply")
        return self.ownership.queued(task_id)

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
            if not self.ownership.mark_cancel_requested(task_id, now):
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
            if self.ownership.abandon(task_id, now):
                logger.info("A2A: abandoned unowned task %s after the TTL", task_id)

    def _reconcile_local_task(self, task_id: str, raw_state: str, text: str,
                              context_id: str, peer: str) -> None:
        """Update the shared task record from an authoritative terminal payload."""
        store = protocol.TaskStore(self.store.db_path)
        record = store.get(task_id)
        if record is None:
            store.create(task_id, context_id or task_id, peer, direction="outbound",
                         ownership="remote", remote_state=raw_state)
        store.complete(task_id, raw_state, text)
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
