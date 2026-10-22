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
import json
import sqlite3
import time
from typing import Any, Iterable, Optional

from . import protocol

# State buckets used by the lifecycle timers.
TERMINAL_REMOTE_STATES = frozenset(protocol.TERMINAL_STATES)


def publication_names_task(metadata_json: Any, task_id: str) -> bool:
    """True when a publication row names ``task_id``.

    A publication names its task either as "task_id" or inside a "tasks" list; the list holds the
    production "peer/task" form. Metadata is parsed in Python rather than with ``json_extract`` so
    one malformed row cannot make a release or a cancel fail (and strand the task's ownership).
    """
    try:
        meta = json.loads(metadata_json) if metadata_json else {}
    except (TypeError, ValueError):
        return False
    if not isinstance(meta, dict):
        return False
    if str(meta.get("task_id") or "") == task_id:
        return True
    return any(str(listed) == task_id or str(listed).endswith("/" + task_id)
               for listed in (meta.get("tasks") or []))


class OwnershipConflict(RuntimeError):
    """Another session owns the task; acquisition is atomic, so nothing is claimed."""

    def __init__(self, task_id: str, owner: str) -> None:
        super().__init__(f"task {task_id} is owned by another session")
        self.task_id = task_id
        self.owner = owner


class RevisionConflict(RuntimeError):
    """A message moved to a revision the caller did not read; nothing was acknowledged.

    Raised from ``consume_if_owned`` under the write lock, before any task-state or delivery
    write, so a caller that catches it can re-read and return the newer content instead.
    """

    def __init__(self, task_id: str, message_ids: list[str]) -> None:
        self.task_id = task_id
        self.message_ids = list(message_ids)
        super().__init__(
            f"task {task_id}: revision changed for {', '.join(self.message_ids)}")


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

    def unowned_count_readonly(self) -> int:
        """Released-but-unowned tasks (the cancel/TTL timers' work), read without migrating."""
        import os
        if not self.db_path or not os.path.exists(self.db_path):
            return 0
        try:
            con = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=5)
            try:
                if con.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='a2a_tasks'"
                ).fetchone() is None:
                    return 0
                row = con.execute(
                    """SELECT COUNT(*) FROM a2a_tasks
                        WHERE owner_session_id='' AND released_by_session_id!=''""").fetchone()
            finally:
                con.close()
        except sqlite3.Error:
            return 0
        return int(row[0]) if row else 0

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
            if cur.rowcount == 1:
                # Same transaction as the ownership change: a publication cannot slip through
                # between "released" and "its undelivered events were dropped".
                self._drop_publications(con, task_id)
            return cur.rowcount == 1

    def release_all(self, session_id: str, now: Optional[float] = None) -> int:
        """Release every task this session owns, including ones with a live watch."""
        if not session_id:
            return 0
        now = time.time() if now is None else now
        with self._session() as con:
            owned = [row["task_id"] for row in con.execute(
                "SELECT task_id FROM a2a_tasks WHERE owner_session_id=?", (session_id,))]
            cur = con.execute(
                """UPDATE a2a_tasks
                      SET owner_session_id='', released_by_session_id=?, released_at=?,
                          unowned_since=?
                    WHERE owner_session_id=?""",
                (session_id, now, now, session_id))
            for task_id in owned:
                self._drop_publications(con, task_id)
            return cur.rowcount

    def release_many(self, task_ids: Optional[Iterable[str]], session_id: str,
                     now: Optional[float] = None) -> tuple[list[str], list[str], int]:
        """Release a set of tasks atomically; returns ``(released, refused, watches_stopped)``.

        ``task_ids=None`` releases every task the session owns, selected under the same write
        lock. Every explicit task must currently be owned by ``session_id``; if any is not,
        nothing changes and every id is refused: a partial release would leave the operator
        unsure which tasks moved. Watch cleanup runs in this same transaction, so a task the
        session reacquires after the commit keeps its new watch and pending delivery.
        """
        if not session_id:
            return [], [], 0
        now = time.time() if now is None else now
        with self._session() as con:
            if not con.in_transaction:
                con.execute("BEGIN IMMEDIATE")
            if task_ids is None:
                # Snapshot inside the lock: a task acquired after this point keeps its ownership
                # and must not have its watch or pending delivery removed by this release.
                ids = [str(row["task_id"]) for row in con.execute(
                    """SELECT task_id FROM a2a_tasks WHERE owner_session_id=?
                        ORDER BY created_at, task_id""", (session_id,))]
            else:
                ids = list(dict.fromkeys(str(t) for t in task_ids if t))
            if not ids:
                return [], [], 0
            marks = ",".join("?" for _ in ids)
            rows = con.execute(
                f"SELECT task_id, owner_session_id FROM a2a_tasks WHERE task_id IN ({marks})",
                (*ids,)).fetchall()
            owners = {str(r["task_id"]): str(r["owner_session_id"] or "") for r in rows}
            refused = [task_id for task_id in ids if owners.get(task_id) != session_id]
            if refused:
                return [], refused, 0
            con.execute(
                f"""UPDATE a2a_tasks
                      SET owner_session_id='', released_by_session_id=?, released_at=?,
                          unowned_since=?
                    WHERE task_id IN ({marks})""",
                (session_id, now, now, *ids))
            for task_id in ids:
                self._drop_publications(con, task_id)
            from . import watches as watches_mod
            stopped = watches_mod.WatchStore.cleanup_for_released_tasks(
                con, session_id, ids, now)
            return ids, [], stopped

    def transfer_many(self, task_ids: Iterable[str], from_session: str, to_session: str,
                      now: Optional[float] = None) -> tuple[list[str], list[str], int]:
        """Move ownership of a set of tasks atomically; returns ``(transferred, refused,
        watches_stopped)``.

        All tasks must be owned by ``from_session`` and the two sessions must differ. The
        ownership change and the removal of the previous owner's pending publications for those
        tasks commit together; undelivered peer messages stay queued for the new owner.
        """
        ids = list(dict.fromkeys(str(t) for t in task_ids if t))
        if not ids or not from_session or not to_session or from_session == to_session:
            return [], ids, 0
        now = time.time() if now is None else now
        marks = ",".join("?" for _ in ids)
        with self._session() as con:
            if not con.in_transaction:
                con.execute("BEGIN IMMEDIATE")
            rows = con.execute(
                f"SELECT task_id, owner_session_id FROM a2a_tasks WHERE task_id IN ({marks})",
                (*ids,)).fetchall()
            owners = {str(r["task_id"]): str(r["owner_session_id"] or "") for r in rows}
            refused = [task_id for task_id in ids
                       if owners.get(task_id) != from_session]
            if refused:
                return [], refused, 0
            con.execute(
                f"""UPDATE a2a_tasks
                      SET owner_session_id=?, owned_at=?, released_at=0, unowned_since=0,
                          released_by_session_id=''
                    WHERE task_id IN ({marks})""",
                (to_session, now, *ids))
            for task_id in ids:
                self._drop_publications(con, task_id)
            from . import watches as watches_mod
            stopped = watches_mod.WatchStore.cleanup_for_released_tasks(
                con, from_session, ids, now)
            return ids, [], stopped

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

    @staticmethod
    def _drop_publications(con: sqlite3.Connection, task_id: str) -> None:
        rows = con.execute(
            "SELECT event_id, metadata_json FROM a2a_watch_publications"
            " WHERE published_at IS NULL").fetchall()
        for row in rows:
            if publication_names_task(row["metadata_json"], task_id):
                con.execute("DELETE FROM a2a_watch_publications WHERE event_id=?",
                            (row["event_id"],))

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

    def owned_ids(self, session_id: str) -> list[str]:
        """Task ids currently owned by ``session_id``, oldest first."""
        if not session_id:
            return []
        with self._session() as con:
            rows = con.execute(
                """SELECT task_id FROM a2a_tasks WHERE owner_session_id=?
                    ORDER BY created_at, task_id""", (session_id,)).fetchall()
        return [str(row["task_id"]) for row in rows]

    # -- message backlog --------------------------------------------------
    def add_message(self, task_id: str, message_id: str, kind: str, text: str,
                    now: Optional[float] = None, *, update_existing: bool = False) -> bool:
        """Record one peer message; True when it was new (idempotent per message id).

        ``update_existing`` is for artifacts, whose id is stable while their content changes
        between snapshots: a changed body updates the row in place and re-queues it (a delivered
        working revision must not hide the final one), an identical body is a no-op. The
        lifecycle classification is part of the revision: INPUT_REQUIRED -> COMPLETED can carry
        the same artifact id and text, and the stale kind must not survive it (Amy round 3, P2).
        """
        if not task_id or not message_id:
            return False
        now = time.time() if now is None else now
        with self._session() as con:
            if update_existing:
                cur = con.execute(
                    """INSERT INTO a2a_task_messages
                           (task_id, message_id, kind, text, created_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(task_id, message_id) DO UPDATE SET
                           kind=excluded.kind,
                           text=excluded.text,
                           delivered_at=NULL,
                           delivered_to_session=''
                       WHERE a2a_task_messages.text <> excluded.text
                          OR a2a_task_messages.kind <> excluded.kind""",
                    (task_id, message_id, kind or "progress", text or "", now))
            else:
                cur = con.execute(
                    """INSERT OR IGNORE INTO a2a_task_messages
                           (task_id, message_id, kind, text, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (task_id, message_id, kind or "progress", text or "", now))
            return cur.rowcount == 1

    def record_messages(self, task_id: str,
                        messages: Iterable[tuple[str, str, str, bool]],
                        *, session_id: str = "",
                        require_owner: bool = False,
                        state: str = "", reply: str = "") -> Optional[list[dict]]:
        """Apply one observation's message upserts under the ownership write lock.

        ``messages`` is ``(message_id, kind, text, update_existing)`` per peer message. Recording
        is the first write a poller does, so an ownership check only at acknowledgement time is
        too late: a release and adoption that commit in between would let the old owner overwrite
        the adopter's queued answer before being refused. ``BEGIN IMMEDIATE`` here takes the same
        lock ``release``/``acquire`` take, and the owner is read inside it.

        ``state``/``reply`` fold the task-record transition into the same transaction: a stale
        poll must not terminalize a task another session has adopted, and a crash between the
        observation and ``TaskStore.complete`` must not lose the peer's final text.

        Returns the still-undelivered rows, or ``None`` when this caller may not record: another
        session owns the task, or ``require_owner`` is set and the task is unowned. A session-less
        caller (``session_id=""``, ``require_owner=False``) may record an unowned task's backlog;
        it never records into a session's task.
        """
        entries = [entry for entry in messages if entry and entry[0]]
        now = time.time()
        with self._session() as con:
            if not con.in_transaction:
                con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT owner_session_id FROM a2a_tasks WHERE task_id=?",
                (task_id,)).fetchone()
            owner = str(row["owner_session_id"] or "") if row else ""
            if owner != str(session_id or ""):
                if owner or require_owner:
                    return None
            if state:
                self._apply_state_transition(con, task_id, state, reply, now)
            for message_id, kind, text, update_existing in entries:
                if update_existing:
                    con.execute(
                        """INSERT INTO a2a_task_messages
                               (task_id, message_id, kind, text, created_at)
                           VALUES (?, ?, ?, ?, ?)
                           ON CONFLICT(task_id, message_id) DO UPDATE SET
                               kind=excluded.kind,
                               text=excluded.text,
                               delivered_at=NULL,
                               delivered_to_session=''
                           WHERE a2a_task_messages.text <> excluded.text
                              OR a2a_task_messages.kind <> excluded.kind""",
                        (task_id, message_id, kind or "progress", text or "", now))
                else:
                    con.execute(
                        """INSERT OR IGNORE INTO a2a_task_messages
                               (task_id, message_id, kind, text, created_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        (task_id, message_id, kind or "progress", text or "", now))
            rows = con.execute(
                """SELECT * FROM a2a_task_messages
                    WHERE task_id=? AND delivered_at IS NULL
                    ORDER BY created_at, message_id""",
                (task_id,)).fetchall()
            return [dict(r) for r in rows]

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

    @staticmethod
    def _apply_state_transition(con: sqlite3.Connection, task_id: str, state: str,
                                reply: str, now: float) -> None:
        """Fold a task state/reply transition into the caller's transaction.

        Mirrors ``TaskStore.complete``/``set_state``: a terminal row cannot be overwritten, so a
        later poll cannot rewrite (or un-terminalize) an answer already recorded. One narrow
        escape hatch: a terminal row whose reply is EMPTY may be replaced by a terminal report
        that carries text. Callers must hold the ownership write lock first.
        """
        terminal_marks = ",".join("?" for _ in TERMINAL_REMOTE_STATES)
        if state in TERMINAL_REMOTE_STATES:
            con.execute(
                f"""UPDATE a2a_tasks
                      SET state=?,
                          remote_state=CASE WHEN direction='outbound' THEN ?
                                            ELSE remote_state END,
                          reply=?, completed_at=?
                    WHERE task_id=?
                      AND (state NOT IN ({terminal_marks})
                           OR (reply='' AND ?<>''))""",
                (state, state, reply or "", now, task_id,
                 *TERMINAL_REMOTE_STATES, reply or ""))
        else:
            con.execute(
                f"""UPDATE a2a_tasks
                      SET state=?,
                          remote_state=CASE WHEN direction='outbound' THEN ?
                                            ELSE remote_state END
                    WHERE task_id=? AND state NOT IN ({terminal_marks})""",
                (state, state, task_id, *TERMINAL_REMOTE_STATES))

    def consume_if_owned(self, task_id: str, session_id: str,
                         message_ids: list[str], *, state: Optional[str] = None,
                         reply: str = "",
                         expected_revisions: Optional[dict[str, tuple[str, str]]] = None) -> bool:
        """Mark backlog delivered in ONE transaction, fenced on live ownership.

        The blocking await records and acknowledges its results as separate statements; a release
        plus an adoption in between would otherwise let the former owner consume the new owner's
        backlog. ``False`` means ownership changed and NOTHING was marked.

        ``expected_revisions`` maps every message id about to be marked to the ``(text, kind)``
        the caller actually returned. They are verified under the same write lock, BEFORE any
        state/reply write or delivery mark; a moved revision raises ``RevisionConflict`` and
        rolls the whole transaction back, so the newer content stays queued (Amy round 3, P1).

        ``state``/``reply`` fold the task-record transition into the same transaction. Without it
        a crash between the acknowledgement and ``TaskStore.complete`` consumed the only durable
        copy of the peer's final text (local review of PR #9, D1).
        """
        now = time.time()
        with self._session() as con:
            # Take the write lock BEFORE reading ownership. Under sqlite3's default deferred
            # transactions the SELECT starts no transaction at all, so a release+adoption could
            # commit between this check and the marks below (Amy's PR #9 finding 1). Release and
            # acquire are UPDATEs, so they need the same lock and cannot interleave now.
            if not con.in_transaction:
                con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT owner_session_id FROM a2a_tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None or str(row[0] or "") != str(session_id or ""):
                return False
            if expected_revisions is not None:
                ids = [str(m) for m in message_ids]
                marks = ",".join("?" for _ in ids)
                current = {
                    str(r["message_id"]): (str(r["text"] or ""),
                                           str(r["kind"] or "progress"))
                    for r in con.execute(
                        f"""SELECT message_id, text, kind FROM a2a_task_messages
                             WHERE task_id=? AND message_id IN ({marks})""",
                        (task_id, *ids)).fetchall()
                } if ids else {}
                stale = [message_id for message_id in ids
                         if current.get(message_id) != expected_revisions.get(message_id)]
                if stale:
                    # The caller is holding content the row no longer has. Acknowledge nothing:
                    # the transaction rolls back in _session(), leaving the newest revision
                    # queued for the caller's re-read.
                    raise RevisionConflict(task_id, stale)
            if state:
                self._apply_state_transition(con, task_id, state, reply, now)
            for message_id in message_ids:
                # The ownership condition is repeated on the UPDATE itself so the mark stays
                # fenced even if a caller ever runs this inside an already-open transaction.
                con.execute(
                    """UPDATE a2a_task_messages SET delivered_at=?, delivered_to_session=?
                        WHERE task_id=? AND message_id=? AND delivered_at IS NULL
                          AND EXISTS (SELECT 1 FROM a2a_tasks
                                       WHERE task_id=? AND owner_session_id=?)""",
                    (now, session_id, task_id, message_id, task_id, session_id))
            return True

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
                """SELECT task_id, peer, remote_state, state, unowned_since, released_at,
                          cancel_requested_at
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

    def mark_cancel_requested(self, task_id: str, now: Optional[float] = None,
                              released_at: Optional[float] = None) -> bool:
        """Claim the auto-cancel iff the task is still unowned AND on the release we selected.

        Adoption and re-release between selection and action make the candidate stale: without
        the release-generation fence the sweep cancels a task that was released seconds ago.
        """
        now = time.time() if now is None else now
        with self._session() as con:
            cur = con.execute(
                """UPDATE a2a_tasks SET cancel_requested_at=?
                    WHERE task_id=? AND owner_session_id='' AND released_by_session_id!=''
                      AND (? IS NULL OR released_at=?)""",
                (now, task_id, released_at, released_at))
            return cur.rowcount == 1

    def abandon(self, task_id: str, now: Optional[float] = None,
                released_at: Optional[float] = None) -> bool:
        """Retire an unowned task and its backlog. False when it was adopted or re-released."""
        now = time.time() if now is None else now
        with self._session() as con:
            cur = con.execute(
                """UPDATE a2a_tasks
                      SET state=?, remote_state=?, reply=?, completed_at=?
                    WHERE task_id=? AND owner_session_id='' AND released_by_session_id!=''
                      AND (? IS NULL OR released_at=?)""",
                (protocol.STATE_CANCELED, protocol.STATE_CANCELED,
                 "abandoned after the unowned-task TTL", now, task_id, released_at, released_at))
            if cur.rowcount != 1:
                return False
            con.execute("DELETE FROM a2a_task_messages WHERE task_id=?", (task_id,))
            return True
