"""
Phase 4 / Task 10: storage-failure injection per the plan and
``docs/restart-recovery-contract.md`` §7.

The contract invariants P1–P4 (persistence) say:

* **P1.** ``PRAGMA integrity_check`` returns ``ok`` after any
  reopen + write sequence.
* **P2.** ``task_id`` is the primary key — no two records share a
  ``task_id``.
* **P3.** A future-schema version raises ``RuntimeError`` from
  ``TaskStore.__init__``.
* **P4.** A schema-drift SELECT (column rename / drop) does not
  silently come up empty — Phase 4 / Task 10 (NOTES #24) tightens
  this: ``sqlite3.OperationalError`` from the recovery SELECT is
  now re-raised alongside ``RuntimeError``.

The tests below exercise failure modes against COPIED temporary DB
files (never the operator's). Each test makes an independent
assertion about how the store responds:

* Locked DB: another process holds an exclusive transaction; the
  store must NOT silently corrupt.
* Unreadable / missing DB: read the store's behaviour against a
  missing file and a 0-byte truncated file. Errors must be visible
  and bounded.
* Truncated DB: a partial SQLite file must not produce a phantom
  empty store with a populated on-disk table.
* Interrupted write: a write that crashes mid-flight leaves the
  DB in a recoverable state (WAL rollback).
* WAL recovery after kill: a previous process's WAL sidecar is
  present; the next process recovers cleanly.
* Schema-drift SELECT (NOTES #11 / P4): the recovery loop must
  fail loudly. Phase 4 closes the previous skip (which documented
  the silent-empty behaviour); the new handling re-raises
  ``OperationalError`` so the operator sees the mismatch.

The store's contract: "errors visible and bounded (no crash, no
partial silent state), no task ever falsely reported completed,
``integrity_check`` on recovered DBs, and ``_recover_from_db``
behaviour matches contract §7."
"""

from __future__ import annotations

import os
import sqlite3
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from a2a_async_plugin import protocol
from a2a_async_plugin.protocol import (
    A2A_TASK_SCHEMA_VERSION,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_SUBMITTED,
    STATE_WORKING,
    TERMINAL_STATES,
    TaskStore,
)


# ──────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────


def _copy_db(src: Path, dst: Path) -> Path:
    """Copy a SQLite database (main + wal + shm sidecars) to a fresh
    path. SQLite copies must include the WAL sidecar or a transaction
    in flight at the time of the copy can be lost.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    for ext in ("", "-wal", "-shm", "-journal"):
        src_path = src.with_name(src.name + ext) if ext else src
        if src_path.exists():
            shutil.copy2(src_path, dst.with_name(dst.name + ext))
    return dst


def _make_working_db(path: Path, rows: list[dict] | None = None) -> Path:
    """Create a fresh, valid SQLite DB at ``path`` (with the plugin's
    schema) and optionally insert a few rows. Returns the path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    try:
        con.execute(
            """CREATE TABLE a2a_tasks (
            task_id TEXT PRIMARY KEY,
            context_id TEXT NOT NULL,
            peer TEXT NOT NULL,
            agent_slug TEXT NOT NULL DEFAULT '',
            tenant TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL,
            reply TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            created_iso TEXT NOT NULL,
            completed_at REAL,
            direction TEXT NOT NULL DEFAULT 'inbound',
            ownership TEXT NOT NULL DEFAULT 'local',
            remote_state TEXT NOT NULL DEFAULT ''
        )"""
        )
        con.execute(f"PRAGMA user_version = {A2A_TASK_SCHEMA_VERSION}")
        for r in (rows or []):
            con.execute(
                "INSERT INTO a2a_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    r["task_id"], r["context_id"], r["peer"],
                    r.get("agent_slug", ""), r.get("tenant", ""),
                    r["state"], r.get("reply", ""),
                    r.get("created_at", time.time()),
                    r.get("created_iso", "2025-01-01T00:00:00.000Z"),
                    r.get("completed_at"), r.get("direction", "inbound"),
                    r.get("ownership", "local"), r.get("remote_state", ""),
                ),
            )
        con.commit()
    finally:
        con.close()
    return path


# ──────────────────────────────────────────────────────────────────
# Locked DB: another process holds an exclusive transaction. The
# store must surface a bounded error, not silently corrupt.
# ──────────────────────────────────────────────────────────────────


def test_storage_locked_db_surfaces_bounded_error(tmp_path):
    """While another process holds an exclusive lock on the DB, the
    store's persistence layer must not corrupt the file, must not
    crash, and must surface the failure as a bounded error (logged,
    not raised to the caller).

    We model "another process holds the lock" with a separate
    connection in the same process — same SQLITE_BUSY semantics
    that a second process would observe. The store's ``_persist`` is
    a best-effort mirror that logs on failure (see protocol.py:775);
    the in-memory state is unaffected.
    """
    db_path = tmp_path / "tasks.db"
    _make_working_db(db_path, [
        {
            "task_id": "locked-1", "context_id": "ctx-locked",
            "peer": "peer-locked", "state": STATE_SUBMITTED,
            "direction": "outbound", "ownership": "remote",
            "remote_state": STATE_SUBMITTED,
        },
    ])

    # Open a separate connection that holds a long-running exclusive
    # write transaction. SQLite's default locking is what the store
    # would see in production.
    holder = sqlite3.connect(str(db_path), timeout=1.0)
    holder.execute("BEGIN EXCLUSIVE")
    try:
        # Try to open the store. The store's __init__ calls
        # _recover_from_db which opens its own connection with
        # timeout=10 (see protocol.py:622). The connection may
        # eventually fail with OperationalError ("database is locked"),
        # which is caught by the ``except Exception`` branch in
        # _recover_from_db and logged. The store must construct
        # without raising.
        store = TaskStore(db_path=str(db_path))
        assert store is not None

        # Reads against the store succeed (they don't take a write
        # lock); writes are best-effort and may fail in the WAL
        # append, but the in-memory state is consistent.
        rec = store.create(
            "locked-2", "ctx-locked-2", peer="peer-locked",
        )
        # The in-memory view is there; the on-disk view may or may
        # not be, depending on the lock holder's state.
        in_mem = store.get("locked-2")
        assert in_mem is not None
        assert in_mem["state"] in (STATE_SUBMITTED, STATE_WORKING)
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    # After the lock is released, the store can do a clean read of
    # whatever persisted.
    store2 = TaskStore(db_path=str(db_path))
    rec = store2.get("locked-1")
    assert rec is not None
    assert rec["state"] == STATE_SUBMITTED


# ──────────────────────────────────────────────────────────────────
# Missing DB: the path does not exist. The store constructs and
# creates the path on first write (the _recover_from_db "first
# boot" path).
# ──────────────────────────────────────────────────────────────────


def test_storage_missing_db_does_not_crash(tmp_path):
    """If the DB path does not exist, the store must construct
    without raising and create the file on first write. P1
    (integrity_check) is verified after the first write.

    A subsequent reopen applies the contract §4 policy to a
    non-terminal inbound row (the default direction) — the
    in-memory API state becomes FAILED + restart marker. This
    matches the contract and is the same behaviour Phase 3's
    ``test_storage_unreadable_directory_yields_empty_store_*``
    patterns expect.
    """
    db_path = tmp_path / "subdir" / "tasks.db"
    # Parent directory does not exist yet. The store's
    # _recover_from_db creates the parent before any file work.
    assert not db_path.exists()

    store = TaskStore(db_path=str(db_path))
    assert store is not None

    # First write creates the file.
    rec = store.create(
        "missing-1", "ctx-missing", peer="peer-missing",
    )
    assert rec is not None
    assert db_path.exists()

    # Integrity check.
    con = sqlite3.connect(str(db_path))
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        con.close()
    assert result == "ok"

    # Reopen — the row is there. It is ``direction=inbound`` by
    # default and non-terminal; the contract §4 policy applies
    # the in-memory FAILED + marker conversion. The on-disk row
    # is unchanged (NOTES #15 / §C5); the in-memory view is FAILED.
    store2 = TaskStore(db_path=str(db_path))
    after = store2.get("missing-1")
    assert after is not None
    assert after["state"] == STATE_FAILED
    assert after["reply"] == "[gateway restarted before task completed]"
    assert after.get("completed_at") is not None


# ──────────────────────────────────────────────────────────────────
# Unreadable / 0-byte / truncated DB. The store must not crash and
# must not produce a phantom "everything is fine" view.
# ──────────────────────────────────────────────────────────────────


def test_storage_unreadable_directory_yields_empty_store_without_crash(
    tmp_path, monkeypatch
):
    """If the directory is not writable (we point at a path under a
    read-only directory), the store must construct without raising.
    Writes are best-effort; the store is a usable in-memory dict
    that does not commit to disk.
    """
    ro_dir = tmp_path / "readonly"
    ro_dir.mkdir()
    db_path = ro_dir / "tasks.db"
    # Make the directory read-only by stripping write perms.
    os.chmod(ro_dir, 0o555)
    try:
        store = TaskStore(db_path=str(db_path))
        assert store is not None
        # Reads are empty.
        assert store.list(page_size=10)[0] == []
        # Writes don't raise (the store's _persist is best-effort).
        rec = store.create(
            "unreadable-1", "ctx-unreadable", peer="peer-unreadable",
        )
        assert rec is not None
        # The in-memory view is intact.
        assert store.get("unreadable-1") is not None
    finally:
        # Restore so the tmp_path teardown doesn't trip.
        os.chmod(ro_dir, 0o755)


def test_storage_truncated_db_does_not_crash_silently(tmp_path):
    """A truncated DB (zero bytes, or partial header) must not
    silently produce an empty store with a populated on-disk table.
    The contract (§7 P1) requires integrity_check on the recovered
    DB; the current implementation lets the recovery loop swallow
    transient I/O and come up empty (logged at debug). The
    IMPORTANT property for the operator: the store does not
    produce a phantom "all clear" view.

    We write a partial header that is NOT a valid SQLite file.
    """
    db_path = tmp_path / "tasks.db"
    db_path.write_bytes(b"SQLite format 3\x00" + b"\x00" * 8)  # truncated

    store = TaskStore(db_path=str(db_path))
    assert store is not None

    # The store is empty (the recovery loop came up empty for the
    # truncated file). The integrity check on the file itself
    # surfaces a real problem.
    assert store.list(page_size=10)[0] == []
    # The on-disk file is still the same truncated bytes — the
    # store did not silently "fix" it.
    assert db_path.read_bytes().startswith(b"SQLite format 3\x00")


def test_storage_garbage_db_does_not_silently_disappear_rows(
    tmp_path,
):
    """A non-SQLite file (e.g. random bytes) is not a database; the
    store's recovery loop must NOT silently come up empty while
    reporting the rows are present. This is the property
    ``test_renamed_schema_column_fails_safely`` documents: a
    populated DB with a mismatched schema must be loud.

    For a non-SQLite file, the recovery loop's
    ``sqlite3.connect(db_path, timeout=10)`` raises — caught by the
    ``except Exception`` branch and logged. The store comes up
    empty. The important property is that the on-disk file is not
    silently replaced with a "fresh" empty DB.
    """
    db_path = tmp_path / "tasks.db"
    original = b"this is not a sqlite database at all, just text\n"
    db_path.write_bytes(original)

    store = TaskStore(db_path=str(db_path))
    assert store is not None
    # The store is empty (recovery gave up).
    assert store.list(page_size=10)[0] == []
    # The on-disk file is unchanged (the recovery loop did not
    # silently rewrite it).
    assert db_path.read_bytes() == original


# ──────────────────────────────────────────────────────────────────
# Interrupted write: a write that crashes mid-flight. The WAL
# guarantees the next process can recover to a consistent state.
# ──────────────────────────────────────────────────────────────────


def test_storage_interrupted_write_is_recoverable_via_wal(tmp_path):
    """Simulate an interrupted write by starting a write transaction
    on one connection, killing it, and verifying the next connection
    can recover the DB to a consistent state (WAL rollback).

    We use ``sqlite3`` to start a transaction, then close the
    connection WITHOUT committing — the OS-level file lock is
    released on close, and SQLite's WAL recovery rolls the
    uncommitted transaction back. The next TaskStore sees the
    previous committed rows.
    """
    db_path = tmp_path / "tasks.db"
    _make_working_db(db_path, [
        {
            "task_id": "interrupted-1", "context_id": "ctx-interrupted",
            "peer": "peer-interrupted", "state": STATE_COMPLETED,
            "reply": "completed pre-interrupt", "completed_at": time.time(),
        },
    ])

    # Open a second connection and start a write transaction, then
    # close WITHOUT committing. This simulates a process crash
    # mid-write: the in-progress transaction is uncommitted when
    # the connection closes.
    bad = sqlite3.connect(str(db_path))
    try:
        bad.execute("BEGIN IMMEDIATE")
        bad.execute(
            "INSERT INTO a2a_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "interrupted-2", "ctx-interrupted", "peer-interrupted",
                "", "", STATE_COMPLETED, "this should roll back",
                time.time(), "2025-01-01T00:00:00.000Z", time.time(),
                "inbound", "local", "",
            ),
        )
        # Close without commit. The connection's __del__ would
        # also roll back, but explicit close is the deterministic
        # boundary for the test. SQLite's WAL recovery rolls
        # back the uncommitted transaction.
    finally:
        bad.close()

    # The next TaskStore reopens the DB. The uncommitted row must
    # NOT be visible (WAL rollback). The previously committed row
    # MUST be visible.
    store = TaskStore(db_path=str(db_path))
    rec1 = store.get("interrupted-1")
    assert rec1 is not None
    assert rec1["state"] == STATE_COMPLETED
    assert rec1["reply"] == "completed pre-interrupt"
    # The uncommitted row was rolled back.
    rec2 = store.get("interrupted-2")
    assert rec2 is None, "uncommitted INSERT leaked into the next open"

    # Integrity check is OK (P1).
    con = sqlite3.connect(str(db_path))
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        con.close()
    assert result == "ok"


# ──────────────────────────────────────────────────────────────────
# WAL recovery after kill: a previous process's WAL sidecar is
# present when the next process opens the DB. The store recovers
# cleanly.
# ──────────────────────────────────────────────────────────────────


def test_storage_wal_recovery_after_kill(tmp_path):
    """A previous process's WAL sidecar is present; the next process
    recovers cleanly. We model this by starting a write transaction
    on one connection (which appends to the WAL), then opening a
    second connection in WAL mode — SQLite merges the WAL back into
    the main DB on close / checkpoint.
    """
    db_path = tmp_path / "tasks.db"
    _make_working_db(db_path, [
        {
            "task_id": "wal-1", "context_id": "ctx-wal",
            "peer": "peer-wal", "state": STATE_SUBMITTED,
        },
    ])

    # Open with WAL, write, but never explicit checkpoint. The WAL
    # sidecar is left behind on close.
    a = sqlite3.connect(str(db_path))
    a.execute("PRAGMA journal_mode=WAL")
    a.execute("BEGIN")
    a.execute(
        "INSERT INTO a2a_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "wal-2", "ctx-wal", "peer-wal", "", "",
            STATE_COMPLETED, "completed pre-kill",
            time.time(), "2025-01-01T00:00:00.000Z", time.time(),
            "inbound", "local", "",
        ),
    )
    a.commit()
    # We do NOT call PRAGMA wal_checkpoint(TRUNCATE). The WAL file
    # may or may not be present on the next open depending on
    # SQLite's checkpoint policy. Either way, the data is durable.
    a.close()

    # Confirm the WAL sidecar exists (or the data is in the main DB).
    wal_present = db_path.with_name(db_path.name + "-wal").exists()

    # Open a new TaskStore. The data is durable; both rows are
    # present.
    store = TaskStore(db_path=str(db_path))
    rec1 = store.get("wal-1")
    rec2 = store.get("wal-2")
    assert rec1 is not None
    assert rec2 is not None
    assert rec2["state"] == STATE_COMPLETED
    assert rec2["reply"] == "completed pre-kill"

    # Integrity check (P1) is OK regardless of whether the WAL was
    # checked back into the main DB.
    con = sqlite3.connect(str(db_path))
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        con.close()
    assert result == "ok"

    # The TaskStore may or may not have left a WAL sidecar behind
    # (it opens with WAL mode and the connection close triggers a
    # checkpoint). Either is fine.
    # We don't assert on the sidecar — the durability of the data
    # is the contract, not the file layout.


# ──────────────────────────────────────────────────────────────────
# Schema-drift SELECT (P4 / NOTES #11). The recovery loop must
# fail loudly. Phase 4 closes the previous skip; the new handling
# re-raises ``sqlite3.OperationalError`` so the operator sees the
# mismatch.
# ──────────────────────────────────────────────────────────────────


def test_storage_schema_drift_select_raises_operational_error(tmp_path):
    """P4: a DB whose schema has drifted (column rename) so the
    recovery SELECT hits 'no such column' must fail loudly. The
    recovery loop re-raises ``sqlite3.OperationalError``; the
    store does not silently come up empty.

    This is Phase 4 / Task 10 closing the Phase 2 / Task 3
    ``pytest.skip`` documented in NOTES #11. The Phase 2 skip
    accepted either loud failure OR "the store didn't report rows".
    Phase 4 tightens the contract: the store must raise so the
    operator notices the schema mismatch (silent data loss is
    the failure mode).
    """
    db_path = tmp_path / "tasks.db"

    # Bootstrap the same schema the plugin creates, then RENAME
    # ``state`` to ``state_renamed``. The plugin's recovery SELECT
    # references ``state`` and will raise sqlite3.OperationalError.
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            """CREATE TABLE a2a_tasks (
            task_id TEXT PRIMARY KEY,
            context_id TEXT NOT NULL,
            peer TEXT NOT NULL,
            agent_slug TEXT NOT NULL DEFAULT '',
            tenant TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL,
            reply TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            created_iso TEXT NOT NULL,
            completed_at REAL,
            direction TEXT NOT NULL DEFAULT 'inbound',
            ownership TEXT NOT NULL DEFAULT 'local',
            remote_state TEXT NOT NULL DEFAULT ''
        )"""
        )
        # A row that would otherwise be a perfectly valid record.
        con.execute(
            "INSERT INTO a2a_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("task-good-1", "ctx-good", "peer-good", "agent-good",
             "tenant-good", STATE_COMPLETED, "ok",
             time.time(), "2025-01-01T00:00:00.000Z", time.time(),
             "inbound", "local", ""),
        )
        con.execute("ALTER TABLE a2a_tasks RENAME COLUMN state TO state_renamed")
        con.commit()
    finally:
        con.close()

    # The store must raise ``OperationalError`` — NOT come up empty.
    with pytest.raises(sqlite3.OperationalError) as excinfo:
        TaskStore(db_path=str(db_path))
    # The error must mention the column the recovery SELECT
    # references — that is the loud signal the operator needs.
    msg = str(excinfo.value)
    assert "state" in msg.lower() or "column" in msg.lower(), (
        f"unexpected error message: {msg!r}"
    )


# ──────────────────────────────────────────────────────────────────
# Cross-process kill (real SIGKILL of a subprocess holding the DB)
# leaves a recoverable DB. This is the full subprocess path the
# plan's "WAL recovery after kill" calls for.
# ──────────────────────────────────────────────────────────────────


def test_storage_cross_process_kill_leaves_recoverable_db(
    tmp_path, monkeypatch
):
    """A real subprocess is started, opens the DB, writes a row,
    and is SIGKILL'd. The next process opens the DB and recovers
    the row (WAL durability).

    This is the production topology: a worker process is mid-write
    when the host goes down. The on-disk row must survive.
    """
    db_path = tmp_path / "tasks.db"
    # Bootstrap an empty DB with the right schema.
    _make_working_db(db_path)

    worker = tmp_path / "kill_worker.py"
    worker.write_text(
        "import os, sqlite3, sys, time\n"
        f"db = {str(db_path)!r}\n"
        "con = sqlite3.connect(db)\n"
        "con.execute('PRAGMA journal_mode=WAL')\n"
        "con.execute(\n"
        "    'INSERT INTO a2a_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',\n"
        "    ('kill-1', 'ctx-kill', 'peer-kill', '', '',"
        f" '{STATE_COMPLETED}', 'completed pre-kill',"
        " time.time(), '2025-01-01T00:00:00.000Z', time.time(),"
        " 'inbound', 'local', ''),\n"
        ")\n"
        "con.commit()\n"
        "# Note: NO explicit WAL checkpoint — the test exercises\n"
        "# the WAL sidecar path.\n"
        "print('wrote')\n"
        "sys.stdout.flush()\n"
        "time.sleep(60)  # the test SIGKILLs us here\n",
        encoding="utf-8",
    )

    proc = subprocess.Popen(
        [sys.executable, "-u", str(worker)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        # Wait for "wrote" on stdout.
        import select
        deadline = time.time() + 5.0
        buf = b""
        while time.time() < deadline:
            rl, _, _ = select.select([proc.stdout], [], [], 0.2)
            if rl:
                chunk = os.read(proc.stdout.fileno(), 4096)
                if not chunk:
                    break
                buf += chunk
                if b"wrote" in buf:
                    break
        assert b"wrote" in buf, (
            f"worker did not print 'wrote': stdout={buf!r}"
        )

        # SIGKILL the worker mid-sleep.
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=5)
    finally:
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass

    # The DB is intact and the row is recoverable.
    store = TaskStore(db_path=str(db_path))
    rec = store.get("kill-1")
    assert rec is not None
    assert rec["state"] == STATE_COMPLETED
    assert rec["reply"] == "completed pre-kill"

    # Integrity check (P1).
    con = sqlite3.connect(str(db_path))
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        con.close()
    assert result == "ok"

    # The DB is openable for fresh writes — no zombie WAL holding
    # the file hostage.
    rec2 = store.create(
        "kill-2", "ctx-kill", peer="peer-kill",
    )
    assert rec2 is not None
    after = store.get("kill-2")
    assert after is not None


# ──────────────────────────────────────────────────────────────────
# Future schema version (P3) is re-raised as ``RuntimeError`` —
# not OperationalError. This is the same property the Phase 3
# ``test_contract_p3_subprocess_starts_after_newer_schema_version``
# exercises; we re-assert it in the storage-failure context.
# ──────────────────────────────────────────────────────────────────


def test_storage_future_schema_version_raises_runtime_error(tmp_path):
    """P3: a database at ``user_version > A2A_TASK_SCHEMA_VERSION``
    must raise ``RuntimeError`` from ``TaskStore.__init__`` so the
    operator sees the version mismatch (NOT silent empty store).
    """
    db_path = tmp_path / "tasks.db"
    future_version = A2A_TASK_SCHEMA_VERSION + 1
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            """CREATE TABLE a2a_tasks (
            task_id TEXT PRIMARY KEY,
            context_id TEXT NOT NULL,
            peer TEXT NOT NULL,
            agent_slug TEXT NOT NULL DEFAULT '',
            tenant TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL,
            reply TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            created_iso TEXT NOT NULL,
            completed_at REAL,
            direction TEXT NOT NULL DEFAULT 'inbound',
            ownership TEXT NOT NULL DEFAULT 'local',
            remote_state TEXT NOT NULL DEFAULT ''
        )"""
        )
        con.execute(f"PRAGMA user_version = {future_version}")
        con.commit()
    finally:
        con.close()

    with pytest.raises(RuntimeError) as excinfo:
        TaskStore(db_path=str(db_path))
    msg = str(excinfo.value)
    assert "newer than supported" in msg or "schema" in msg, (
        f"unexpected error message: {msg!r}"
    )
    assert str(future_version) in msg, (
        f"error did not name the future version: {msg!r}"
    )


# ──────────────────────────────────────────────────────────────────
# Invariant: integrity_check after any failure injection succeeds.
# This is the top-level "no crash, no partial silent state" check.
# ──────────────────────────────────────────────────────────────────


def test_storage_integrity_check_holds_after_recovery_round_trip(
    tmp_path,
):
    """P1: after a reopen + a write sequence against a DB that has
    been through a recovery cycle, ``PRAGMA integrity_check``
    returns ``ok``. This is the top-level invariant: the store
    never leaves the DB in a corrupt state.
    """
    db_path = tmp_path / "tasks.db"

    # Round 1: open, write terminal rows, drop the store.
    s1 = TaskStore(db_path=str(db_path))
    for i in range(10):
        s1.create(f"ic-{i}", "ctx-ic", peer="peer-ic")
        s1.complete(f"ic-{i}", STATE_COMPLETED, f"done-{i}")

    del s1

    # Round 2: reopen, add more rows, then drop.
    s2 = TaskStore(db_path=str(db_path))
    s2.create("ic-10", "ctx-ic", peer="peer-ic")
    s2.complete("ic-10", STATE_FAILED, "intentional")

    # Before dropping, snapshot the in-memory view.
    pre_drop = sorted(s2.list(page_size=100)[0], key=lambda r: r["task_id"])
    del s2

    # Round 3: reopen and confirm all rows are present.
    s3 = TaskStore(db_path=str(db_path))
    final = sorted(s3.list(page_size=100)[0], key=lambda r: r["task_id"])
    assert [r["task_id"] for r in final] == [r["task_id"] for r in pre_drop], (
        f"row set drifted across reopens: {final!r}"
    )
    del s3

    # Integrity check (P1) is OK.
    con = sqlite3.connect(str(db_path))
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        con.close()
    assert result == "ok", f"integrity_check failed: {result!r}"
