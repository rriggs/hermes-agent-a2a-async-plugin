"""Session ownership of A2A tasks, and the message backlog that waits for an owner.

Requirements this module implements:

* A task is either **unowned** or **owned by exactly one session**. Ownership belongs to the
  session, not to any individual await: cancelling a watch stops polling but leaves the owner in
  place; only an explicit release, or the end of the session, gives ownership up.
* Awaiting an unowned task acquires it implicitly. Awaiting a task owned by another session is
  refused.
* While unowned, every message the task produces is queued here. Nothing polls an unowned task;
  the backlog is pulled from the peer when a session acquires or awaits it, and delivered to that
  session once, in order.
* Adoption is final. A session that released a task and finds it owned by another gets a "lost"
  outcome on resume, never the task back.
* A task left unowned is cancelled remotely after ``unowned_cancel_after_s`` (default 1 day) and
  abandoned after ``unowned_ttl_s`` (default 7 days). Both are configurable.

All ownership transitions are single UPDATE statements with the owner in the WHERE clause, so two
live sessions racing for the same task resolve to one winner at the SQLite level.
"""

from __future__ import annotations

import contextlib
import sqlite3
import time
from typing import Iterable, Optional

from . import protocol

# State buckets used by the lifecycle timers.
TERMINAL_REMOTE_STATES = frozenset(protocol.TERMINAL_STATES)


class OwnershipStore:
    """Profile-scoped ownership + message queue for A2A task records."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path or protocol.TaskStore._resolve_db_path(None)

    # -- connection -------------------------------------------------------
    @contextlib.contextmanager
    def _session(self):
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
        con = sqlite3.connect(self.db_path, timeout=10)
        con.execute("PRAGMA journal_mode=WAL")
        con.row_factory = sqlite3.Row
        version = int(con.execute("PRAGMA user_version").fetchone()[0])
        if version != protocol.A2A_TASK_SCHEMA_VERSION:
            protocol.TaskStore._ensure_db(con)
        return con

    # -- ownership --------------------------------------------------------
    def acquire(self, task_id: str, session_id: str, now: Optional[float] = None) -> bool:
        """Take ownership of ``task_id`` for ``session_id``; False when another session owns it."""
        if not task_id or not session_id:
            return False
        now = time.time() if now is None else now
        with self._session() as con:
            cur = con.execute(
                """UPDATE a2a_tasks
                      SET owner_session_id=?, owned_at=?, released_at=0, unowned_since=0,
                          -- Keep the releasing session on record when someone ELSE adopts, so
                          -- that session learns on resume that the task is lost (final). A
                          -- session re-acquiring its own released task clears the record.
                          released_by_session_id=CASE WHEN released_by_session_id=? THEN ''
                                                      ELSE released_by_session_id END
                    WHERE task_id=? AND (owner_session_id='' OR owner_session_id=?)""",
                (session_id, now, session_id, task_id, session_id))
            return cur.rowcount == 1

    def release(self, task_id: str, session_id: str, now: Optional[float] = None) -> bool:
        """Give up ownership; the task becomes adoptable by any session."""
        if not task_id or not session_id:
            return False
        now = time.time() if now is None else now
        with self._session() as con:
            cur = con.execute(
                """UPDATE a2a_tasks
                      SET owner_session_id='', released_by_session_id=?, released_at=?,
                          unowned_since=?
                    WHERE task_id=? AND owner_session_id=?""",
                (session_id, now, now, task_id, session_id))
            return cur.rowcount == 1

    def release_all(self, session_id: str, now: Optional[float] = None) -> int:
        """Release every task this session owns, including ones with a live watch."""
        if not session_id:
            return 0
        now = time.time() if now is None else now
        with self._session() as con:
            cur = con.execute(
                """UPDATE a2a_tasks
                      SET owner_session_id='', released_by_session_id=?, released_at=?,
                          unowned_since=?
                    WHERE owner_session_id=?""",
                (session_id, now, now, session_id))
            return cur.rowcount

    def reclaim(self, session_id: str, now: Optional[float] = None) -> tuple[list[str], list[str]]:
        """Re-acquire what ``session_id`` released before it ended.

        Returns ``(reclaimed, lost)``: tasks still unowned (now ours again) and tasks another
        session adopted while we were away (final -- they are not coming back).
        """
        if not session_id:
            return [], []
        now = time.time() if now is None else now
        with self._session() as con:
            rows = con.execute(
                """SELECT task_id, owner_session_id FROM a2a_tasks
                    WHERE released_by_session_id=?""",
                (session_id,)).fetchall()
        reclaimed: list[str] = []
        lost: list[str] = []
        for row in rows:
            task_id, owner = row["task_id"], str(row["owner_session_id"] or "")
            if owner and owner != session_id:
                lost.append(task_id)
                continue
            if self.acquire(task_id, session_id, now):
                reclaimed.append(task_id)
                continue
            # Lost the race to another session between the SELECT and the UPDATE.
            lost.append(task_id)
        with self._session() as con:
            for task_id in reclaimed + lost:
                con.execute(
                    "UPDATE a2a_tasks SET released_by_session_id='' WHERE task_id=?",
                    (task_id,))
        return reclaimed, lost

    def owner(self, task_id: str) -> str:
        with self._session() as con:
            row = con.execute(
                "SELECT owner_session_id FROM a2a_tasks WHERE task_id=?", (task_id,)).fetchone()
        return str(row["owner_session_id"] or "") if row else ""

    def owners(self, task_ids: Iterable[str]) -> dict[str, str]:
        ids = [str(t) for t in task_ids if t]
        if not ids:
            return {}
        marks = ",".join("?" for _ in ids)
        with self._session() as con:
            rows = con.execute(
                f"SELECT task_id, owner_session_id FROM a2a_tasks WHERE task_id IN ({marks})",
                ids).fetchall()
        return {row["task_id"]: str(row["owner_session_id"] or "") for row in rows}

    # -- message backlog --------------------------------------------------
    def add_message(self, task_id: str, message_id: str, kind: str, text: str,
                    now: Optional[float] = None) -> bool:
        """Record one peer message; True when it was new (idempotent per message id)."""
        if not task_id or not message_id:
            return False
        now = time.time() if now is None else now
        with self._session() as con:
            cur = con.execute(
                """INSERT OR IGNORE INTO a2a_task_messages
                       (task_id, message_id, kind, text, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (task_id, message_id, kind or "progress", text or "", now))
            return cur.rowcount == 1

    def queued(self, task_id: str) -> list[dict]:
        """Undelivered messages for one task, oldest first."""
        with self._session() as con:
            rows = con.execute(
                """SELECT * FROM a2a_task_messages
                    WHERE task_id=? AND delivered_at IS NULL
                    ORDER BY created_at, message_id""",
                (task_id,)).fetchall()
        return [dict(r) for r in rows]

    def has_kind(self, task_id: str, kind: str) -> bool:
        with self._session() as con:
            row = con.execute(
                "SELECT 1 FROM a2a_task_messages WHERE task_id=? AND kind=? LIMIT 1",
                (task_id, kind)).fetchone()
        return row is not None

    def mark_delivered(self, task_id: str, message_ids: Iterable[str], session_id: str,
                       now: Optional[float] = None) -> int:
        ids = [str(m) for m in message_ids if m]
        if not ids:
            return 0
        now = time.time() if now is None else now
        marks = ",".join("?" for _ in ids)
        with self._session() as con:
            cur = con.execute(
                f"""UPDATE a2a_task_messages
                       SET delivered_to_session=?, delivered_at=?
                     WHERE task_id=? AND message_id IN ({marks}) AND delivered_at IS NULL""",
                (session_id, now, task_id, *ids))
            return cur.rowcount

    # -- unowned lifecycle timers ----------------------------------------
    def unowned_due(self, now: Optional[float] = None, *,
                    cancel_after_s: float, ttl_s: float) -> tuple[list[dict], list[dict]]:
        """Split overdue unowned tasks into ``(to_cancel, to_abandon)``.

        A task is cancelled once it has been unowned for ``cancel_after_s`` (the remote work stops
        costing someone else), and abandoned after ``ttl_s``. Both are measured from the moment the
        last owner released it, never from creation, so an actively awaited task is never touched.
        """
        now = time.time() if now is None else now
        with self._session() as con:
            rows = con.execute(
                """SELECT task_id, peer, remote_state, state, unowned_since, cancel_requested_at
                     FROM a2a_tasks
                    WHERE owner_session_id='' AND released_by_session_id!=''
                      AND unowned_since<=?
                    ORDER BY unowned_since""",
                (now - min(cancel_after_s, ttl_s),)).fetchall()
        cancel, abandon = [], []
        for row in rows:
            rec = dict(row)
            age = now - float(rec["unowned_since"] or 0.0)
            terminal = (str(rec["remote_state"] or "") in TERMINAL_REMOTE_STATES
                        or str(rec["state"] or "") in TERMINAL_REMOTE_STATES)
            if terminal:
                # A finished task has nothing to cancel; it just ages out.
                if age >= ttl_s:
                    abandon.append(rec)
                continue
            if age >= ttl_s:
                abandon.append(rec)
            elif age >= cancel_after_s and not rec["cancel_requested_at"]:
                cancel.append(rec)
        return cancel, abandon

    def mark_cancel_requested(self, task_id: str, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        with self._session() as con:
            con.execute(
                "UPDATE a2a_tasks SET cancel_requested_at=? WHERE task_id=?", (now, task_id))

    def abandon(self, task_id: str, now: Optional[float] = None) -> None:
        """Terminally retire an unowned task record and its queued messages."""
        now = time.time() if now is None else now
        with self._session() as con:
            con.execute(
                """UPDATE a2a_tasks
                      SET state=?, remote_state=?, reply=?, completed_at=?
                    WHERE task_id=?""",
                (protocol.STATE_CANCELED, protocol.STATE_CANCELED,
                 "abandoned after the unowned-task TTL", now, task_id))
            con.execute("DELETE FROM a2a_task_messages WHERE task_id=?", (task_id,))
