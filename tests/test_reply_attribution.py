"""Reply attribution and one-peer-per-context binding.

Two defects are covered here.

1. Reply attribution. ``adapter.send()`` receives only the A2A context id, never
   the task id, so the original code settled "the oldest outstanding task for
   the context". That is only correct while one task occupies one turn. This
   fleet runs ``busy_input_mode: steer``, which injects a mid-run arrival's text
   into the turn already running, so that turn's single reply answers BOTH the
   task it started for and the task that arrived. Settling only the oldest left
   the steered peer unanswered. ``_settle_turn`` now settles every task the turn
   actually answered, and degrades to oldest-pending only when it holds no turn
   record at all (a late or out-of-band send).

2. One peer per context. An A2A context keys the agent session
   (``<ns>:a2a:dm:<context_id>`` — ``gateway.session.build_session_key`` drops
   the participant for a DM that carries a chat_id), the on-disk transcript, and
   the anti-loop turn counter. Two peers sharing a ``contextId`` would share all
   three, so the first inbound peer to use a context binds it and any other peer
   is refused rather than silently merged.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future

import pytest

from a2a_async_plugin import protocol
from a2a_async_plugin import adapter as adapter_mod


CTX = "ctx-attribution-1"


@pytest.fixture
def isolated_home(monkeypatch, tmp_path):
    """Sandbox HERMES_HOME so the task DB and audit log stay inside the test."""
    home = tmp_path / "hermes_home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("A2A_TASKS_DB", str(home / "a2a_tasks.db"))
    return home


@pytest.fixture
def store(isolated_home):
    return protocol.TaskStore()


def _make_adapter(store, monkeypatch) -> "adapter_mod.A2AAdapter":
    from gateway.config import PlatformConfig  # type: ignore

    config = PlatformConfig(enabled=True, extra={"port": 0, "agent_name": "attribution-test"})
    adp = adapter_mod.A2AAdapter(config)
    # The adapter's own store is replaced so each test owns its records.
    adp.tasks = store
    # No gateway runner: the standalone path, which is what unit tests exercise.
    adp.gateway_runner = None
    return adp


def _add_waiter(adp, task_id: str, context_id: str = CTX) -> Future:
    fut = adp._add_pending(task_id, context_id)
    adp.tasks.create(task_id, context_id, "peer-a")
    return fut


# ---------------------------------------------------------------------------
# _settle_turn: the mechanism
# ---------------------------------------------------------------------------


def test_settle_turn_resolves_trigger_and_folded_arrivals(isolated_home, monkeypatch):
    """One reply settles the turn's task and every arrival folded into that turn."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    first = _add_waiter(adp, "task-1")
    second = _add_waiter(adp, "task-2")

    adp._note_turn_start(CTX, "task-1")
    adp._note_arrival(CTX, "task-2", folded_into_turn=True)

    settled = adp._settle_turn(CTX, protocol.STATE_COMPLETED, "the answer")

    assert settled == 2
    assert_first = first.result(timeout=0)
    assert_second = second.result(timeout=0)
    assert assert_first == (protocol.STATE_COMPLETED, "the answer")
    # Same reply for both: their text was in the same model turn.
    assert assert_second == assert_first


def test_arrival_not_folded_gets_its_own_turn(isolated_home, monkeypatch):
    """Queue mode gives the arrival its own turn, so this reply must not settle it."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    first = _add_waiter(adp, "task-1")
    second = _add_waiter(adp, "task-2")

    adp._note_turn_start(CTX, "task-1")
    adp._note_arrival(CTX, "task-2", folded_into_turn=False)

    settled = adp._settle_turn(CTX, protocol.STATE_COMPLETED, "answer for task-1")

    assert settled == 1
    assert first.result(timeout=0) == (protocol.STATE_COMPLETED, "answer for task-1")
    assert not second.done(), "a queued arrival must wait for its own turn"


def test_settle_turn_returns_none_without_a_turn_record(isolated_home, monkeypatch):
    """No turn record => the caller falls back to oldest-pending resolution."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    _add_waiter(adp, "task-1")

    assert adp._settle_turn(CTX, protocol.STATE_COMPLETED, "late") is None


def test_settle_turn_is_not_reentrant_within_one_turn(isolated_home, monkeypatch):
    """A second send in the same turn must not settle a task the turn never saw."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    first = _add_waiter(adp, "task-1")
    queued = _add_waiter(adp, "task-2")

    adp._note_turn_start(CTX, "task-1")
    adp._settle_turn(CTX, protocol.STATE_COMPLETED, "first send")
    second = adp._settle_turn(CTX, protocol.STATE_COMPLETED, "second send")

    assert first.done()
    assert second is None, "the turn record is consumed by the first settle"
    assert not queued.done()


def test_arrivals_are_bounded(isolated_home, monkeypatch):
    """A runaway turn stops absorbing rather than growing the list without bound."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    adp._note_turn_start(CTX, "task-1")
    for i in range(adp._MAX_ARRIVALS_PER_TURN + 10):
        adp._note_arrival(CTX, f"task-extra-{i}", folded_into_turn=True)
    assert len(adp._arrived[CTX]) == adp._MAX_ARRIVALS_PER_TURN


# ---------------------------------------------------------------------------
# send(): the public path the gateway actually calls
# ---------------------------------------------------------------------------


def test_send_settles_every_task_the_turn_answered(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    first = _add_waiter(adp, "task-1")
    steered = _add_waiter(adp, "task-2")

    adp._note_turn_start(CTX, "task-1")
    adp._note_arrival(CTX, "task-2", folded_into_turn=True)

    asyncio.run(adp.send(CTX, "combined answer", metadata={"notify": True}))

    assert first.result(timeout=0) == (protocol.STATE_COMPLETED, "combined answer")
    assert steered.result(timeout=0) == (protocol.STATE_COMPLETED, "combined answer")


def test_send_ignores_non_final_sends(isolated_home, monkeypatch):
    """Progress/status sends must not satisfy the JSON-RPC caller."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    waiter = _add_waiter(adp, "task-1")
    adp._note_turn_start(CTX, "task-1")

    asyncio.run(adp.send(CTX, "partial", metadata={}))

    assert not waiter.done()


def test_send_falls_back_to_oldest_for_an_out_of_band_reply(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    waiter = _add_waiter(adp, "task-1")

    asyncio.run(adp.send(CTX, "late reply", metadata={"notify": True}))

    assert waiter.result(timeout=0) == (protocol.STATE_COMPLETED, "late reply")


# ---------------------------------------------------------------------------
# on_processing_start / on_processing_complete wiring
# ---------------------------------------------------------------------------


class _Event:
    def __init__(self, task_id: str, context_id: str = CTX):
        self.message_id = task_id
        self.source = type("Source", (), {"chat_id": context_id})()


def test_processing_start_captures_the_trigger_task(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    asyncio.run(adp.on_processing_start(_Event("task-9")))  # type: ignore[arg-type]
    assert adp._turn_tasks[CTX] == ["task-9"]


def test_processing_complete_settles_the_turn_on_failure(isolated_home, monkeypatch):
    """A failed turn answers nothing, so its folded arrivals fail with it."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    first = _add_waiter(adp, "task-1")
    steered = _add_waiter(adp, "task-2")

    adp._note_turn_start(CTX, "task-1")
    adp._note_arrival(CTX, "task-2", folded_into_turn=True)

    evt = _Event("task-1")
    asyncio.run(adp.on_processing_complete(evt, adapter_mod.ProcessingOutcome.FAILURE))  # type: ignore[arg-type]

    assert first.result(timeout=0)[0] == protocol.STATE_FAILED
    assert steered.result(timeout=0)[0] == protocol.STATE_FAILED


# ---------------------------------------------------------------------------
# One peer per context
# ---------------------------------------------------------------------------


def test_context_peer_is_empty_for_an_unused_context(store):
    assert store.context_peer(CTX) == ""


def test_context_peer_returns_the_first_inbound_peer(store):
    store.create("task-a", CTX, "peer-a")
    assert store.context_peer(CTX) == "peer-a"


def test_context_peer_survives_memory_trimming(store):
    """The durable arm answers after the in-memory store forgets a terminal row."""
    store.create("task-a", CTX, "peer-a")
    store.complete("task-a", protocol.STATE_COMPLETED, "done")
    store._tasks.clear()  # stand in for _trim_locked()
    assert store.context_peer(CTX) == "peer-a"


def test_context_peer_ignores_outbound_rows(store):
    """A context we called out on does not bind us to that peer's inbound use."""
    store.create("task-out", CTX, "peer-b", direction="outbound")
    assert store.context_peer(CTX) == ""


def test_prepare_task_rejects_a_second_peer_on_a_bound_context(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    params = lambda text: {"message": {"role": "ROLE_USER", "contextId": CTX,
                                       "parts": [{"text": text}]}}

    first, _pending = adp._prepare_task(params("hello from a"), "peer-a")
    # First contact binds the context. It may fail to dispatch (no loop in this
    # harness) — what matters is that the binding exists.
    assert adp.tasks.context_peer(CTX) == "peer-a"

    second, pending = adp._prepare_task(params("hello from b"), "peer-b")

    assert pending is None
    assert second is not None
    assert second["status"]["state"] == protocol.STATE_REJECTED
    reply = second["status"]["message"]["parts"][0]["text"]
    assert "different peer" in reply


def test_prepare_task_admits_the_bound_peer_again(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    params = {"message": {"role": "ROLE_USER", "contextId": CTX,
                          "parts": [{"text": "second turn"}]}}
    adp.tasks.create("task-a", CTX, "peer-a")

    task, _pending = adp._prepare_task(params, "peer-a")

    # Not rejected: the same peer continues its own context.
    assert task is None or task["status"]["state"] != protocol.STATE_REJECTED


def _enable_dispatch(adp, monkeypatch) -> None:
    """Let _prepare_task reach its arrival bookkeeping without a real event loop.

    The readiness guard requires both a loop and a message handler; the dispatch
    itself is stubbed out because this harness never runs a turn.
    """
    adp._loop = object()
    adp._message_handler = lambda event: None

    def _fake_submit(coro, loop):
        coro.close()
        return object()

    monkeypatch.setattr(adapter_mod.asyncio, "run_coroutine_threadsafe", _fake_submit)


def test_prepare_task_records_a_mid_turn_arrival_as_folded(isolated_home, monkeypatch):
    """The dispatch path must register the arrival before handing it to the gateway."""
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    _enable_dispatch(adp, monkeypatch)
    ids = iter(["task-1", "task-2"])
    monkeypatch.setattr(protocol, "new_task_id", lambda: next(ids))

    seen = {"n": 0}

    def _busy(event):
        seen["n"] += 1
        return seen["n"] > 1  # first contact is idle; the second lands mid-turn

    monkeypatch.setattr(adp, "_session_is_busy", _busy)
    monkeypatch.setattr(adp, "_arrival_folds_into_turn", lambda event: True)

    params = lambda text: {"message": {"role": "ROLE_USER", "contextId": CTX,
                                       "parts": [{"text": text}]}}
    adp._prepare_task(params("first"), "peer-a")
    adp._prepare_task(params("second"), "peer-a")

    assert adp._arrived[CTX] == ["task-2"], "only the mid-turn arrival is folded in"


def test_prepare_task_does_not_fold_when_the_arrival_gets_its_own_turn(isolated_home, monkeypatch):
    adp = _make_adapter(protocol.TaskStore(), monkeypatch)
    _enable_dispatch(adp, monkeypatch)
    ids = iter(["task-1", "task-2"])
    monkeypatch.setattr(protocol, "new_task_id", lambda: next(ids))

    seen = {"n": 0}

    def _busy(event):
        seen["n"] += 1
        return seen["n"] > 1

    monkeypatch.setattr(adp, "_session_is_busy", _busy)
    monkeypatch.setattr(adp, "_arrival_folds_into_turn", lambda event: False)

    params = lambda text: {"message": {"role": "ROLE_USER", "contextId": CTX,
                                       "parts": [{"text": text}]}}
    adp._prepare_task(params("first"), "peer-a")
    adp._prepare_task(params("second"), "peer-a")

    assert adp._arrived == {}
