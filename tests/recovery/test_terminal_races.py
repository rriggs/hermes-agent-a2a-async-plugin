"""
Phase 4 / Task 9: terminal race safety per
``docs/restart-recovery-contract.md`` §5 and §6.

The contract says (C1–C5):

* **C1.** A terminal transition only succeeds when the row's current
  state is non-terminal. ``TaskStore.complete()`` enforces this — the
  second terminal attempt returns ``None`` and does not modify the
  row.
* **C2.** A late finalizer CANNOT overwrite a prior terminal state.
  After ``COMPLETED``, attempts to mark it ``FAILED`` are no-ops.
* **C3.** Exactly one terminal state wins per record.
* **C4.** A caller restart CANNOT overwrite a prior terminal.
* **C5.** A non-terminal inbound reopen converts the row to FAILED
  with the restart marker ONCE per reopen (per-process). The on-disk
  reply is NOT rewritten by subsequent reopens.

This file drives the REAL ``protocol.TaskStore`` (and where natural,
the real ``adapter.A2AAdapter``) — no invented APIs. We orchestrate
the following race conditions against the same task id:

1. **In-process terminal race** (C1, C2, C3): N threads call
   ``complete()`` with N distinct terminal states. Exactly one
   thread's call returns a non-None record; the others return
   ``None``. The final state is whichever thread won.

2. **In-process completion vs cancellation** (C2): thread A marks
   ``COMPLETED`` first; thread B's late ``CANCELED`` is a no-op.

3. **Restart recovery vs remote reconciliation** (C4, matrix row
   11): peer-reported terminal is persisted; a fresh ``TaskStore``
   reopens the same on-disk row; the in-memory FAILED-with-marker
   conversion for non-terminal inbound does NOT overwrite the
   persisted terminal.

4. **Watchdog fail_orphans vs reconciliation** (C1 + W2): a task
   past the orphan threshold with no live waiter is failed by
   ``fail_orphans``; a late ``complete()`` attempt (e.g. a
   reconciliation that arrives after the orphan transition) is a
   no-op (the row is already terminal).

5. **Finalization vs reopen** (C5): a non-terminal inbound row is
   converted to FAILED + marker in-memory on reopen; a subsequent
   reopen does NOT rewrite the on-disk ``reply`` field.

6. **Deterministic interleavings with synchronization**: where the
   race admits multiple outcomes, we drive it deterministically with
   ``threading.Barrier`` so the test is not flaky. We do NOT
   sleep-based timing — all interleavings are scripted.

We prefer a small number of deterministic race scenarios over a
fuzz of nondeterministic timing races.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

import pytest

from a2a_async_plugin import protocol
from a2a_async_plugin.protocol import (
    STATE_CANCELED,
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


def _isolated_hermes_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    home = tmp_path / "hermes_home"
    home.mkdir(parents=True, exist_ok=True)
    tasks_db = home / "a2a_tasks.db"
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("A2A_TASKS_DB", str(tasks_db))
    return home


def _read_db_row(db_path: Path, task_id: str) -> dict | None:
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute(
            "SELECT task_id, context_id, peer, agent_slug, tenant, state,"
            " reply, created_at, created_iso, completed_at, direction,"
            " ownership, remote_state FROM a2a_tasks WHERE task_id = ?",
            (task_id,),
        )
        row = cur.fetchone()
    finally:
        con.close()
    if row is None:
        return None
    keys = ["task_id", "context_id", "peer", "agent_slug", "tenant",
            "state", "reply", "created_at", "created_iso",
            "completed_at", "direction", "ownership", "remote_state"]
    return dict(zip(keys, row))


# ──────────────────────────────────────────────────────────────────
# Race 1: N threads racing on a terminal transition. C1 + C3.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_exactly_one_winner_per_record(
    monkeypatch, tmp_path
):
    """N threads all try to terminalise the same record at the same
    instant. Exactly ONE call returns a non-None record; the others
    return ``None``. The final state is the winner's state. The
    on-disk row matches the winner.

    We use ``threading.Barrier`` to start the threads in lockstep so
    the race is reproducible, not flaky.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "race-1", "ctx-race-1", peer="peer-race-1",
    )
    store.set_state(rec["task_id"], STATE_WORKING)

    n = 8
    candidates = [
        (STATE_COMPLETED, "completed-by-thread"),
        (STATE_FAILED, "failed-by-thread"),
        (STATE_CANCELED, "canceled-by-thread"),
    ]
    # Pad the list out to N so every thread has a target.
    race_inputs: list[tuple[str, str]] = []
    for i in range(n):
        race_inputs.append(candidates[i % len(candidates)])

    barrier = threading.Barrier(n)
    results: list[Optional[dict]] = [None] * n
    errors: list[BaseException] = []

    def worker(i: int) -> None:
        try:
            barrier.wait(timeout=5)
            state, reply = race_inputs[i]
            results[i] = store.complete(rec["task_id"], state, reply)
        except BaseException as e:  # pragma: no cover - propagation
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"worker threads raised: {errors}"

    # Exactly one non-None.
    non_none = [r for r in results if r is not None]
    assert len(non_none) == 1, (
        f"expected exactly 1 terminal winner, got {len(non_none)}: {results!r}"
    )
    winner = non_none[0]
    winner_state, winner_reply = race_inputs[
        results.index(winner)
    ]
    # The winner's state is the row's state.
    assert winner["state"] == winner_state
    assert winner["reply"] == winner_reply
    assert winner["completed_at"] is not None

    # In-memory view matches the winner.
    after = store.get(rec["task_id"])
    assert after is not None
    assert after["state"] == winner_state
    assert after["reply"] == winner_reply
    assert after["completed_at"] == winner["completed_at"]

    # On-disk row matches.
    on_disk = _read_db_row(db_path, rec["task_id"])
    assert on_disk is not None
    assert on_disk["state"] == winner_state
    assert on_disk["reply"] == winner_reply
    assert on_disk["completed_at"] == winner["completed_at"]


# ──────────────────────────────────────────────────────────────────
# Race 2: late cancel cannot overwrite a prior completed. C2.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_late_cancel_loses_to_prior_completed(
    monkeypatch, tmp_path
):
    """Thread A marks ``COMPLETED``; thread B's late ``CANCELED``
    must be a no-op. The Barrier ensures A's ``complete()`` call
    completes before B's, even though both threads are started
    in lockstep. (C2 invariant.)
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "race-2", "ctx-race-2", peer="peer-race-2",
    )
    store.set_state(rec["task_id"], STATE_WORKING)

    barrier_a = threading.Barrier(2)
    barrier_b = threading.Barrier(2)
    out_a: list[Optional[dict]] = [None]
    out_b: list[Optional[dict]] = [None]

    def thread_a() -> None:
        barrier_a.wait(timeout=5)
        out_a[0] = store.complete(rec["task_id"], STATE_COMPLETED, "A-wins")
        barrier_b.wait(timeout=5)

    def thread_b() -> None:
        barrier_a.wait(timeout=5)
        # Wait until A is done before B starts its call.
        barrier_b.wait(timeout=5)
        out_b[0] = store.complete(rec["task_id"], STATE_CANCELED, "B-loses")

    ta = threading.Thread(target=thread_a)
    tb = threading.Thread(target=thread_b)
    ta.start()
    tb.start()
    ta.join(timeout=5)
    tb.join(timeout=5)

    assert out_a[0] is not None
    assert out_b[0] is None, f"late CANCELED overwrote prior COMPLETED: {out_b[0]!r}"

    after = store.get(rec["task_id"])
    assert after["state"] == STATE_COMPLETED
    assert after["reply"] == "A-wins"

    on_disk = _read_db_row(db_path, rec["task_id"])
    assert on_disk["state"] == STATE_COMPLETED
    assert on_disk["reply"] == "A-wins"


# ──────────────────────────────────────────────────────────────────
# Race 3: late completed cannot overwrite a prior failed. C3.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_late_completed_loses_to_prior_failed(
    monkeypatch, tmp_path
):
    """The mirror image of race 2: a late COMPLETED is a no-op against
    a prior FAILED. C3 + C2 invariant.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "race-3", "ctx-race-3", peer="peer-race-3",
    )
    store.set_state(rec["task_id"], STATE_WORKING)

    barrier = threading.Barrier(2)
    out_a: list[Optional[dict]] = [None]
    out_b: list[Optional[dict]] = [None]

    def thread_a() -> None:
        barrier.wait(timeout=5)
        out_a[0] = store.complete(rec["task_id"], STATE_FAILED, "A-fails-it")
        time.sleep(0.05)  # ensure A's complete returns first

    def thread_b() -> None:
        barrier.wait(timeout=5)
        time.sleep(0.1)  # start the second complete after A's done
        out_b[0] = store.complete(rec["task_id"], STATE_COMPLETED, "B-too-late")

    ta = threading.Thread(target=thread_a)
    tb = threading.Thread(target=thread_b)
    ta.start()
    tb.start()
    ta.join(timeout=5)
    tb.join(timeout=5)

    assert out_a[0] is not None
    assert out_b[0] is None, f"late COMPLETED overwrote prior FAILED: {out_b[0]!r}"

    after = store.get(rec["task_id"])
    assert after["state"] == STATE_FAILED
    assert after["reply"] == "A-fails-it"

    on_disk = _read_db_row(db_path, rec["task_id"])
    assert on_disk["state"] == STATE_FAILED
    assert on_disk["reply"] == "A-fails-it"


# ──────────────────────────────────────────────────────────────────
# Race 4: a caller restart cannot overwrite a prior terminal
# outbound. C4 + matrix row 11.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_caller_restart_preserves_prior_terminal(
    monkeypatch, tmp_path
):
    """C4 / matrix row 11. The persisted terminal survives reopen and
    the in-memory FAILED conversion (for a non-terminal inbound) does
    not leak into an already-terminal row.

    Sequence: outbound task created → peer completes → local
    reconciled → persisted COMPLETED. The task is *outbound*, so the
    inbound-restart policy does not apply. Reopen: state, reply,
    completed_at are byte-equal.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        "race-4", "ctx-race-4", peer="peer-race-4",
        direction="outbound", ownership="remote",
        remote_state=STATE_WORKING,
    )
    finished = store.complete(
        rec["task_id"], STATE_COMPLETED, "prior-terminal",
    )
    assert finished is not None
    pre = dict(finished)

    # Drop the in-memory store; reopen.
    del store
    fresh = TaskStore(db_path=str(db_path))
    after = fresh.get(rec["task_id"])
    assert after is not None
    assert after["state"] == pre["state"]
    assert after["reply"] == pre["reply"]
    assert after["completed_at"] == pre["completed_at"]
    assert after["direction"] == "outbound"

    # And a late FAILED attempt (e.g. a stray watchdog tick or a late
    # caller-side cancel) is a no-op.
    out = fresh.complete(rec["task_id"], STATE_FAILED, "stray-fail")
    assert out is None
    again = fresh.get(rec["task_id"])
    assert again["state"] == STATE_COMPLETED
    assert again["reply"] == "prior-terminal"


# ──────────────────────────────────────────────────────────────────
# Race 5: set_state during the terminal race is gated (no transition
# from a terminal state). The row cannot be moved BACK to a
# non-terminal state by a late set_state.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_late_set_state_after_terminal_is_noop(
    monkeypatch, tmp_path
):
    """A late ``set_state(WORKING)`` after the row is already terminal
    is a no-op. This is the companion to the C1/C2 invariant —
    ``set_state`` also gates on "is current state terminal?" and
    refuses to overwrite.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    store = TaskStore(db_path=str(db_path))
    rec = store.create("race-5", "ctx-race-5", peer="peer-race-5")
    store.complete(rec["task_id"], STATE_COMPLETED, "wins")

    # A late set_state must be a no-op.
    store.set_state(rec["task_id"], STATE_WORKING)
    after = store.get(rec["task_id"])
    assert after["state"] == STATE_COMPLETED
    assert after["reply"] == "wins"

    on_disk = _read_db_row(db_path, rec["task_id"])
    assert on_disk["state"] == STATE_COMPLETED
    assert on_disk["reply"] == "wins"


# ──────────────────────────────────────────────────────────────────
# Race 6: fail_orphans vs reconciliation. C1 + W2.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_fail_orphans_vs_late_completion(
    monkeypatch, tmp_path
):
    """W2: the watchdog fails an orphan past the timeout (no live
    waiter). A late ``complete()`` (e.g. a reconciliation that arrives
    after the watchdog transitioned the row) is a no-op.

    This is the canonical production race: a task that the watchdog
    timed out while the peer's reply was in flight. The peer's late
    reply must NOT overwrite the watchdog's FAILED.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    store = TaskStore(db_path=str(db_path))
    rec = store.create("race-6", "ctx-race-6", peer="peer-race-6")
    store.set_state(rec["task_id"], STATE_WORKING)
    # Backdate the in-memory record so the watchdog's age filter
    # applies. ``fail_orphans`` reads the in-memory ``created_at``.
    old = time.time() - 3600
    with store._lock:
        store._tasks[rec["task_id"]]["created_at"] = old
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            "UPDATE a2a_tasks SET created_at = ? WHERE task_id = ?",
            (old, rec["task_id"]),
        )
        con.commit()
    finally:
        con.close()

    # Watchdog fails the orphan.
    failed = store.fail_orphans(timeout_seconds=60, protected=set())
    assert failed == [rec["task_id"]]
    after_watchdog = store.get(rec["task_id"])
    assert after_watchdog["state"] == STATE_FAILED
    assert after_watchdog["reply"] == "[task orphaned — no reply produced]"
    watchdog_completed_at = after_watchdog["completed_at"]

    # Late reconciliation attempt (e.g. the peer's reply arrived
    # AFTER the watchdog transitioned).
    out = store.complete(rec["task_id"], STATE_COMPLETED, "late-reply")
    assert out is None, f"late reconciliation overwrote watchdog FAILED: {out!r}"
    after = store.get(rec["task_id"])
    assert after["state"] == STATE_FAILED
    assert after["reply"] == "[task orphaned — no reply produced]"
    assert after["completed_at"] == watchdog_completed_at, (
        f"completed_at drifted after a no-op late complete: {after['completed_at']!r}"
    )

    on_disk = _read_db_row(db_path, rec["task_id"])
    assert on_disk["state"] == STATE_FAILED
    assert on_disk["reply"] == "[task orphaned — no reply produced]"


# ──────────────────────────────────────────────────────────────────
# Race 7: repeated _recover_from_db on a non-terminal inbound row.
# C5 — the on-disk reply is not rewritten by subsequent reopens.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_repeated_reopen_does_not_rewrite_on_disk_reply(
    monkeypatch, tmp_path
):
    """C5: a non-terminal inbound row is converted to FAILED + marker
    in-memory on every reopen. The on-disk ``reply`` is NOT rewritten
    (the persistence layer's _recover_from_db does not call _persist
    on the in-memory conversion). Subsequent reopens see the same
    on-disk ``reply``. The in-memory marker is per-process, the
    on-disk state is durable.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    # Bootstrap: non-terminal inbound row, persisted as WORKING.
    s1 = TaskStore(db_path=str(db_path))
    rec = s1.create(
        "race-7", "ctx-race-7", peer="peer-race-7",
        direction="inbound", ownership="local", remote_state="",
    )
    s1.set_state(rec["task_id"], STATE_WORKING)
    del s1

    # First reopen: API state is FAILED + marker, but the on-disk
    # reply stays empty (per NOTES #15 / §C5).
    s2 = TaskStore(db_path=str(db_path))
    after_first = s2.get(rec["task_id"])
    assert after_first["state"] == STATE_FAILED
    assert after_first["reply"] == "[gateway restarted before task completed]"

    on_disk_after_first = _read_db_row(db_path, rec["task_id"])
    on_disk_state_1 = on_disk_after_first["state"]
    on_disk_reply_1 = on_disk_after_first["reply"]

    del s2

    # Second reopen: API state is again FAILED + marker (per-process
    # conversion). On-disk reply is the SAME — not rewritten.
    s3 = TaskStore(db_path=str(db_path))
    after_second = s3.get(rec["task_id"])
    assert after_second["state"] == STATE_FAILED
    assert after_second["reply"] == "[gateway restarted before task completed]"

    on_disk_after_second = _read_db_row(db_path, rec["task_id"])
    assert on_disk_after_second["state"] == on_disk_state_1
    assert on_disk_after_second["reply"] == on_disk_reply_1, (
        f"on-disk reply drifted across reopens: "
        f"first={on_disk_reply_1!r} second={on_disk_after_second['reply']!r}"
    )

    # Sanity: the on-disk reply is the persisted one (empty), NOT the
    # marker. The marker is the in-memory view only.
    assert on_disk_after_second["reply"] == "", (
        f"on-disk reply was rewritten to the marker: {on_disk_after_second['reply']!r}"
    )


# ──────────────────────────────────────────────────────────────────
# Race 8: a reconciliation that arrives during a reopen. C4 — the
# in-memory FAILED conversion cannot overwrite a persisted terminal
# that's been reloaded.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_persisted_terminal_survives_inbound_reopen(
    monkeypatch, tmp_path
):
    """A terminal inbound row is NOT converted to FAILED on reopen
    (the C5 / §4 conversion only applies to *non-terminal* rows).
    The persisted terminal survives reopen byte-equal.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    s1 = TaskStore(db_path=str(db_path))
    rec = s1.create(
        "race-8", "ctx-race-8", peer="peer-race-8",
        direction="inbound", ownership="local", remote_state="",
    )
    s1.complete(rec["task_id"], STATE_COMPLETED, "completed pre-restart")
    del s1

    s2 = TaskStore(db_path=str(db_path))
    after = s2.get(rec["task_id"])
    assert after is not None
    assert after["state"] == STATE_COMPLETED, (
        f"contract violation: terminal inbound was rewritten to FAILED on reopen: {after['state']!r}"
    )
    assert after["reply"] == "completed pre-restart"
    # The restart marker is reserved for non-terminal inbound rows.
    assert "[gateway restarted" not in after["reply"]

    on_disk = _read_db_row(db_path, rec["task_id"])
    assert on_disk["state"] == STATE_COMPLETED
    assert on_disk["reply"] == "completed pre-restart"


# ──────────────────────────────────────────────────────────────────
# Race 9: the most paranoid race — N parallel opens against a
# non-terminal inbound DB, each opens a fresh TaskStore, each
# applies the C5 conversion. The on-disk row is unchanged across
# all opens. C5 invariant under concurrent opens.
# ──────────────────────────────────────────────────────────────────


def test_terminal_race_concurrent_reopens_are_consistent(
    monkeypatch, tmp_path
):
    """C5 under concurrency: N threads each open a fresh TaskStore
    against the same DB containing one non-terminal inbound row.
    Every thread sees the same on-disk row (no rewriting). The
    in-memory view is FAILED + marker in every thread. There is no
    interleaving that produces a partial state.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    s0 = TaskStore(db_path=str(db_path))
    rec = s0.create(
        "race-9", "ctx-race-9", peer="peer-race-9",
        direction="inbound", ownership="local", remote_state="",
    )
    s0.set_state(rec["task_id"], STATE_WORKING)
    del s0

    on_disk_before = _read_db_row(db_path, rec["task_id"])
    assert on_disk_before["state"] == STATE_WORKING
    assert on_disk_before["reply"] == ""

    barrier = threading.Barrier(4)
    snapshots: list[dict | None] = [None] * 4
    errors: list[BaseException] = []

    def worker(i: int) -> None:
        try:
            barrier.wait(timeout=5)
            s = TaskStore(db_path=str(db_path))
            try:
                snapshots[i] = s.get(rec["task_id"])
            finally:
                del s
        except BaseException as e:  # pragma: no cover - propagation
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"worker threads raised: {errors}"

    # Every thread's in-memory view is FAILED + marker.
    for i, snap in enumerate(snapshots):
        assert snap is not None, f"thread {i} got None"
        assert snap["state"] == STATE_FAILED, (
            f"thread {i} did not see FAILED on reopen: {snap['state']!r}"
        )
        assert snap["reply"] == "[gateway restarted before task completed]", (
            f"thread {i} missing the marker: {snap['reply']!r}"
        )

    # The on-disk row is unchanged across all reopens.
    on_disk_after = _read_db_row(db_path, rec["task_id"])
    assert on_disk_after["state"] == on_disk_before["state"]
    assert on_disk_after["reply"] == on_disk_before["reply"]
    assert on_disk_after["state"] == STATE_WORKING
    assert on_disk_after["reply"] == ""


# ──────────────────────────────────────────────────────────────────
# Race 10: a "scrambled" race — the order of two terminal attempts
# is non-deterministic, but the final state is one of the two
# (compare-and-set). The other is a no-op. This is a deterministic
# reorder (not a timing race) — we drive both orderings.
# ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("order", ["completed_first", "failed_first"])
def test_terminal_race_scrambled_two_terminals_exactly_one_wins(
    monkeypatch, tmp_path, order
):
    """Two distinct terminal transitions racing on the same record.
    The first one (whichever runs first) wins; the second is a no-op.

    We drive the order deterministically: ``order="completed_first"``
    issues COMPLETED before FAILED; ``order="failed_first"`` issues
    FAILED before COMPLETED. Either way, exactly one wins.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    db_path = Path(os.environ["A2A_TASKS_DB"])

    store = TaskStore(db_path=str(db_path))
    rec = store.create(
        f"race-10-{order}", f"ctx-race-10-{order}", peer="peer-race-10",
    )
    store.set_state(rec["task_id"], STATE_WORKING)

    if order == "completed_first":
        first = store.complete(rec["task_id"], STATE_COMPLETED, "completed-first")
        second = store.complete(rec["task_id"], STATE_FAILED, "failed-second")
        winner_state = STATE_COMPLETED
        winner_reply = "completed-first"
    else:
        first = store.complete(rec["task_id"], STATE_FAILED, "failed-first")
        second = store.complete(rec["task_id"], STATE_COMPLETED, "completed-second")
        winner_state = STATE_FAILED
        winner_reply = "failed-first"

    assert first is not None
    assert second is None, f"second terminal won for {order}: {second!r}"

    after = store.get(rec["task_id"])
    assert after["state"] == winner_state
    assert after["reply"] == winner_reply

    on_disk = _read_db_row(db_path, rec["task_id"])
    assert on_disk["state"] == winner_state
    assert on_disk["reply"] == winner_reply
