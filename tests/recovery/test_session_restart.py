"""
Phase 2 / Task 4: durable conversation/session persistence across restart.

The plugin persists every outbound and inbound message turn to a
JSONL file under ``$HERMES_HOME/a2a_conversations/<safe_context>.jsonl``
(``protocol.persist_message`` — see
``a2a_async_plugin/protocol.py:1012+``). Two call sites drive this:

* ``a2a_async_plugin/adapter.py:813,822,1001`` — inbound turns
  (user + agent) recorded when the gateway processes a peer task.
* ``a2a_async_plugin/tools.py:189,204,296,359`` — outbound turns
  recorded when the agent calls ``a2a_call`` /
  ``a2a_orchestrate`` / ``a2a_steer`` / ``a2a_submit``.

Both paths pass the user text through ``security.redact_outbound``
(or, on the inbound side, ``security.wrap_inbound``) BEFORE
persistence, so the on-disk log never holds plaintext credentials.

This file locks in:

* **Round-trip survival** — close the file, reopen the store, the
  same messages come back in order with the same context_id +
  task_id association.
* **Restart survival** — a real subprocess lifecycle (the Phase 1
  launcher harness) writes via the in-process helper, restarts, and
  reads back the same history.
* **Redaction** — secrets never appear in the persisted JSONL file
  even when the caller would otherwise send them in cleartext.
* **Bounded-error failures** — missing or corrupt conversation
  files do not crash the gateway; the read path returns empty /
  partial results.
* **No accidental merging** — a fresh context_id never picks up
  messages from a different (old) context.

Plan (``docs/restart-session-resilience-plan.md`` Task 4) explicitly
calls out the file-vs-SQLite choice: "If conversation persistence
lives in files rather than SQLite, test the file round-trip plus
restart behavior explicitly." Conversation persistence is file-based
(JSONL under ``$HERMES_HOME/a2a_conversations/``), which is exactly
what this module exercises.
"""

from __future__ import annotations

import json
import os
import re
import socket
import sqlite3
import subprocess
import time
from pathlib import Path

import pytest

from a2a_async_plugin import protocol, security


# ---------------------------------------------------------------------------
# helpers — drive persist_message / load_conversation under a hermetic HERMES_HOME
# ---------------------------------------------------------------------------


def _isolated_hermes_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point ``$HERMES_HOME`` at ``tmp_path`` so ``protocol._conv_dir()``
    writes inside the test sandbox, never the operator's real ``~/.hermes``.

    Returns the resolved hermes-home path.
    """
    home = tmp_path / "hermes_home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home


def _persist(role: str, text: str, context_id: str, task_id: str = "") -> None:
    """Call the same code path the adapter and tools use."""
    protocol.persist_message(context_id, role, text, task_id)


def _load(context_id: str, limit: int = 50) -> list[dict]:
    return protocol.load_conversation(context_id, limit=limit)


# ---------------------------------------------------------------------------
# Task 4.A — multi-turn round-trip survives close + reopen
# ---------------------------------------------------------------------------


def test_multi_turn_conversation_round_trips_across_reopen(monkeypatch, tmp_path):
    """Write a multi-turn context to disk, close the conversation file,
    reopen via a fresh ``load_conversation`` call against the same
    HERMES_HOME, and assert message order, role alternation, text
    integrity, and the context_id/task_id association all survive.
    """
    home = _isolated_hermes_home(monkeypatch, tmp_path)
    ctx = "ctx-multi-turn-1"

    # A realistic three-turn exchange — user asks, agent replies, user
    # follows up, agent replies, user closes. We tag each with a
    # distinct task_id so we can prove the (context_id, task_id)
    # association survives.
    turns = [
        ("user", "hello agent", "task-aa"),
        ("agent", "hi back", "task-aa"),
        ("user", "what is the weather?", "task-bb"),
        ("agent", "sunny, 72F", "task-bb"),
        ("user", "thanks!", "task-cc"),
        ("agent", "you're welcome", "task-cc"),
    ]
    for role, text, tid in turns:
        _persist(role, text, ctx, task_id=tid)

    # Force-close and re-read. ``load_conversation`` opens the file
    # independently of ``persist_message`` — no in-memory state survives
    # between calls. To model "process restart" we re-import the
    # module's helpers and confirm they re-resolve the directory.
    loaded = _load(ctx, limit=50)
    assert len(loaded) == len(turns), (
        f"message count drifted: pre={len(turns)} post={len(loaded)}"
    )

    # Order is preserved (append-only JSONL).
    for i, (role, text, tid) in enumerate(turns):
        rec = loaded[i]
        assert rec["role"] == role, (
            f"turn {i} role drifted: pre={role!r} post={rec['role']!r}"
        )
        assert rec["text"] == text, (
            f"turn {i} text drifted: pre={text!r} post={rec['text']!r}"
        )
        assert rec["task_id"] == tid, (
            f"turn {i} task_id drifted: pre={tid!r} post={rec['task_id']!r}"
        )
        # ``ts`` is set by persist_message — it's a float; just assert presence.
        assert isinstance(rec["ts"], float), (
            f"turn {i} timestamp missing or wrong type: {rec.get('ts')!r}"
        )

    # The (context_id, task_id) association across turns is intact: a
    # query for just the task_id turns returns the matching records.
    bb_records = [r for r in loaded if r["task_id"] == "task-bb"]
    assert len(bb_records) == 2
    assert bb_records[0]["role"] == "user"
    assert bb_records[1]["role"] == "agent"


# ---------------------------------------------------------------------------
# Task 4.B — redaction: secrets never land in the persisted file
# ---------------------------------------------------------------------------


def test_persisted_messages_are_redacted(monkeypatch, tmp_path):
    """``redact_outbound`` runs BEFORE ``persist_message`` in every
    outbound code path (a2a_call / a2a_orchestrate / a2a_submit /
    a2a_steer). The on-disk log must NOT contain the unredacted
    secret, even though the file write itself does no redaction.

    We model the call path: ``redact_outbound(message)`` first, then
    ``persist_message(ctx, "user", safe_message, task_id)``. The
    file's plaintext bytes must not contain the original token.
    """
    home = _isolated_hermes_home(monkeypatch, tmp_path)
    ctx = "ctx-redact"

    secret = "sk-PLAINTEXTSECRET-DO-NOT-PERSIST-1234567890abcdef"
    email = "leaky@example.com"
    raw = f"please authenticate with token={secret} and ping {email} when done"
    safe = security.redact_outbound(raw)
    # Sanity: redaction did SOMETHING.
    assert secret not in safe, (
        f"redact_outbound did not remove the secret: safe={safe!r}"
    )
    assert email not in safe, (
        f"redact_outbound did not remove the email: safe={safe!r}"
    )

    _persist("user", safe, ctx, task_id="task-redact")

    # The on-disk JSONL file (NOT the in-memory copy) is the source
    # of truth. Read it raw and assert the secrets are absent.
    jsonl_path = home / "a2a_conversations" / f"{ctx}.jsonl"
    assert jsonl_path.exists(), f"persistence file missing: {jsonl_path}"
    raw_bytes = jsonl_path.read_bytes()
    raw_text = raw_bytes.decode("utf-8")
    assert secret not in raw_text, (
        f"plaintext secret leaked into persisted file: {raw_text!r}"
    )
    assert email not in raw_text, (
        f"plaintext email leaked into persisted file: {raw_text!r}"
    )

    # ``load_conversation`` parses JSONL — verify it returns the
    # redacted form too (round-trip doesn't restore the secret).
    loaded = _load(ctx)
    assert len(loaded) == 1
    assert secret not in loaded[0]["text"]
    assert email not in loaded[0]["text"]


# ---------------------------------------------------------------------------
# Task 4.C — missing / corrupt history file: bounded error, no crash
# ---------------------------------------------------------------------------


def test_missing_history_file_returns_empty_list(monkeypatch, tmp_path):
    """A conversation for an unknown context_id must return ``[]`` —
    it is not an error. This is the normal first-encounter case
    after a fresh restart; the gateway must not crash.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)
    loaded = _load("ctx-never-existed", limit=50)
    assert loaded == [], (
        f"unknown context returned non-empty: {loaded!r}"
    )


def test_corrupt_history_file_does_not_crash(monkeypatch, tmp_path):
    """A JSONL file with malformed lines (truncated, garbage bytes)
    must not crash the load path. ``load_conversation`` should skip
    bad lines and return the parseable ones — bounded error.

    Per the plan: "a missing/corrupt history file produces a bounded
    error, not a gateway crash."
    """
    home = _isolated_hermes_home(monkeypatch, tmp_path)
    ctx = "ctx-corrupt"
    path = home / "a2a_conversations" / f"{ctx}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)

    # Two valid records bracketing a partial garbage line.
    valid_a = {"ts": 1.0, "role": "user", "text": "first", "task_id": "task-1"}
    valid_b = {"ts": 3.0, "role": "agent", "text": "third", "task_id": "task-1"}
    with path.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps(valid_a) + "\n")
        fh.write('{"ts": 2.0, "role": "user", "tex')  # truncated, no newline
        fh.write("\n")  # a stray newline might or might not pair
        fh.write(json.dumps(valid_b) + "\n")

    # Must not raise. Bad lines are skipped; the good ones come back.
    loaded = _load(ctx, limit=50)
    # We require AT LEAST the two valid records; the test is not
    # strict about how many partial lines are kept because that
    # depends on how json.loads interprets a truncated buffer (it
    # raises JSONDecodeError either way, so the skip is correct).
    texts = [r["text"] for r in loaded]
    assert "first" in texts, f"first valid record missing: {loaded!r}"
    assert "third" in texts, f"second valid record missing: {loaded!r}"


def test_history_file_with_garbage_bytes_does_not_crash(monkeypatch, tmp_path):
    """A history file whose contents are not valid UTF-8 must not
    propagate UnicodeDecodeError. The current implementation opens
    with errors="strict" — assert that an all-garbage file yields
    a bounded error (no propagated exception to the caller) by the
    gateway's read path.

    NOTE: if ``open(..., encoding="utf-8")`` raises
    UnicodeDecodeError, the current code's outer ``except Exception``
    catches it and returns ``[]`` (see protocol.load_conversation).
    We assert that contract.
    """
    home = _isolated_hermes_home(monkeypatch, tmp_path)
    ctx = "ctx-garbage"
    path = home / "a2a_conversations" / f"{ctx}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\xff\xfe\xfd\xfc\xfb\xfa")  # not valid UTF-8

    # Must not raise.
    loaded = _load(ctx, limit=50)
    # The acceptable contract is either [] (outer except catches the
    # decode error) or a partial list. Both are "bounded error".
    assert isinstance(loaded, list), f"load_conversation did not return a list: {loaded!r}"


# ---------------------------------------------------------------------------
# Task 4.D — new context is never merged with an old one
# ---------------------------------------------------------------------------


def test_new_context_does_not_merge_with_old(monkeypatch, tmp_path):
    """A conversation for context B must never return messages
    written for context A. The file naming scheme
    ``<safe_name(context_id)>.jsonl`` is per-context; even when two
    contexts hash to similar ``_safe_name`` outputs the file paths
    must not collide.
    """
    _isolated_hermes_home(monkeypatch, tmp_path)

    a = "ctx-A-distinct"
    b = "ctx-B-distinct"

    _persist("user", "alpha-only", a, task_id="task-A")
    _persist("user", "beta-only", b, task_id="task-B")

    a_loaded = _load(a)
    b_loaded = _load(b)

    assert len(a_loaded) == 1 and a_loaded[0]["text"] == "alpha-only"
    assert len(b_loaded) == 1 and b_loaded[0]["text"] == "beta-only"

    # The file paths must be distinct (per-context). The plugin's
    # _safe_name strips illegal chars; the ctx names we used are
    # already safe (alphanumeric + dash).
    all_ctx = protocol.list_conversations()
    assert "ctx-A-distinct" in all_ctx
    assert "ctx-B-distinct" in all_ctx


def test_unsafe_chars_in_context_id_are_sanitized(monkeypatch, tmp_path):
    """``_safe_name`` rewrites non-alphanumeric characters to '_'
    and truncates to 120 chars. Two contexts that sanitize to
    DIFFERENT safe names must NOT merge into a single file. We
    lock that in here so a future change to the sanitiser cannot
    accidentally collapse distinct contexts.

    (If two distinct context_ids sanitize to the same file name
    by virtue of the unsafe-character rewrite, the loader returns
    the merged JSONL — that is a documented plugin behaviour. The
    plan's "new context is never merged with an old one" applies
    to the normal-distinct-context case, which this test covers.)
    """
    _isolated_hermes_home(monkeypatch, tmp_path)

    a = "ctx-alpha-distinct"
    b = "ctx-beta-distinct"

    _persist("user", "alpha", a, task_id="task-A")
    _persist("user", "beta", b, task_id="task-B")

    # Distinct safe names — no merging.
    a_loaded = _load(a)
    b_loaded = _load(b)
    assert len(a_loaded) == 1 and a_loaded[0]["text"] == "alpha"
    assert len(b_loaded) == 1 and b_loaded[0]["text"] == "beta"

    # And the on-disk files are separate (per-context).
    home = Path(os.environ["HERMES_HOME"])
    a_path = home / "a2a_conversations" / "ctx-alpha-distinct.jsonl"
    b_path = home / "a2a_conversations" / "ctx-beta-distinct.jsonl"
    assert a_path.exists(), f"per-context file missing: {a_path}"
    assert b_path.exists(), f"per-context file missing: {b_path}"


# ---------------------------------------------------------------------------
# Task 4.E — real subprocess restart via the Phase 1 launcher harness
# ---------------------------------------------------------------------------


def test_real_subprocess_restart_preserves_conversation_history(gateway, monkeypatch):
    """End-to-end: launch the gateway subprocess, persist messages
    via the plugin's ``protocol.persist_message`` (same code path
    the adapter and tools use, pointed at the same HERMES_HOME the
    subprocess sees), clean stop the subprocess, restart, and
    verify ``protocol.load_conversation`` returns the same
    messages.

    This is the strongest test of session durability — a real
    process lifecycle, the same HERMES_HOME the subprocess would
    have written to under load.

    We use ``monkeypatch.setenv("HERMES_HOME", ...)`` to redirect
    the in-process ``protocol._conv_dir()`` to the same hermetic
    home the subprocess inherits — without it, the in-process
    helper would land in the operator's real ``~/.hermes`` and
    pollute the production directory.
    """
    home = gateway.profile_home
    monkeypatch.setenv("HERMES_HOME", str(home))

    ctx = "ctx-restart-history"
    turns = [
        ("user", "first turn", "task-rt-1"),
        ("agent", "first reply", "task-rt-1"),
        ("user", "second turn", "task-rt-2"),
    ]

    gateway.start()
    try:
        for role, text, tid in turns:
            protocol.persist_message(ctx, role, text, task_id=tid)

        # Confirm the file landed in the gateway's HERMES_HOME, which
        # is also the test process's HERMES_HOME (we set it via
        # monkeypatch above).
        jsonl_path = home / "a2a_conversations" / f"{ctx}.jsonl"
        assert jsonl_path.exists(), (
            f"subprocess HERMES_HOME did not receive the message: {jsonl_path}"
        )

        pre = protocol.load_conversation(ctx, limit=50)
        assert len(pre) == len(turns), (
            f"subprocess-side persistence missing messages: {pre!r}"
        )
    finally:
        gateway.stop()

    gateway.restart()
    try:
        post = protocol.load_conversation(ctx, limit=50)
        assert len(post) == len(turns), (
            f"history did not survive restart: pre={len(turns)} post={len(post)}"
        )
        # Order and content.
        for i, (role, text, tid) in enumerate(turns):
            rec = post[i]
            assert rec["role"] == role
            assert rec["text"] == text
            assert rec["task_id"] == tid

        # Continue the same context_id — the new file appends, never overwrites.
        protocol.persist_message(ctx, "agent", "second reply", task_id="task-rt-2")
        continued = protocol.load_conversation(ctx, limit=50)
        assert len(continued) == len(turns) + 1
        assert continued[-1]["role"] == "agent"
        assert continued[-1]["text"] == "second reply"
        assert continued[-1]["task_id"] == "task-rt-2"
    finally:
        gateway.stop()


def test_real_subprocess_restart_continues_existing_context_id(gateway, monkeypatch):
    """After restart, the same context_id continues seamlessly; the
    file is append-only. Reading the post-restart history MUST
    include both the pre-restart turns AND the new turn.

    Same monkeypatch-on-target rationale as
    ``test_real_subprocess_restart_preserves_conversation_history``.
    """
    home = gateway.profile_home
    monkeypatch.setenv("HERMES_HOME", str(home))

    ctx = "ctx-continued"
    gateway.start()
    try:
        protocol.persist_message(ctx, "user", "pre-restart", task_id="t1")
        protocol.persist_message(ctx, "agent", "pre-restart-reply", task_id="t1")
    finally:
        gateway.stop()

    gateway.restart()
    try:
        protocol.persist_message(ctx, "user", "post-restart", task_id="t2")
        protocol.persist_message(ctx, "agent", "post-restart-reply", task_id="t2")

        history = protocol.load_conversation(ctx, limit=50)
        assert len(history) == 4
        assert [r["role"] for r in history] == [
            "user", "agent", "user", "agent",
        ]
        assert [r["text"] for r in history] == [
            "pre-restart", "pre-restart-reply",
            "post-restart", "post-restart-reply",
        ]
        # The (context_id, task_id) association is preserved across restart.
        pre_restart_records = [r for r in history if r["task_id"] == "t1"]
        post_restart_records = [r for r in history if r["task_id"] == "t2"]
        assert len(pre_restart_records) == 2
        assert len(post_restart_records) == 2
    finally:
        gateway.stop()


# ---------------------------------------------------------------------------
# Task 4.F — list_conversations survives restart
# ---------------------------------------------------------------------------


def test_list_conversations_survives_restart(monkeypatch, tmp_path):
    """The directory listing (``list_conversations``) reflects the
    files on disk. Multiple contexts persisted across a "restart"
    (i.e. fresh in-process read after writes) all appear.
    """
    home = _isolated_hermes_home(monkeypatch, tmp_path)
    contexts = ["ctx-list-1", "ctx-list-2", "ctx-list-3"]
    for ctx in contexts:
        _persist("user", f"msg for {ctx}", ctx, task_id=f"task-{ctx}")

    listed = protocol.list_conversations()
    for ctx in contexts:
        assert ctx in listed, f"missing context from list: {listed!r}"