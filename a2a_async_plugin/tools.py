"""
A2A client tools — let the Hermes agent talk to *other* agents as a peer.

Tools (registered in the ``a2a`` toolset):
  - a2a_discover(url)         -> fetch + summarize a peer's Agent Card
  - a2a_call(agent, message)  -> send a task to a peer, return its reply
  - a2a_list()                -> list configured peers + persisted conversations
  - a2a_history(context_id)   -> recall a persisted A2A conversation
  - a2a_orchestrate(...)      -> fan-out task to multiple peers by capability

Peers are resolved from config.yaml under ``a2a_agents``::

    a2a_agents:
      researcher:
        url: "http://localhost:9999"
        auth: { type: bearer, token: "sk-..." }
        timeout: 120
        capabilities: [web_search, research]

Transport is stdlib urllib (no a2a-sdk dependency). The wire format is the A2A
v1.0 JSON-RPC ``message/send`` method; replies from v0.3 peers still parse.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional, TypedDict

from . import protocol, security

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 120
_ORCHESTRATE_MAX_WORKERS = 6  # max parallel peers for fan-out

# Caller-side task records are opened lazily so merely discovering tools does not
# create the profile's task database.
def _task_store() -> protocol.TaskStore:
    return protocol.TaskStore()


# --------------------------------------------------------------------------
# Peer resolution
# --------------------------------------------------------------------------

def _load_config() -> dict:
    try:
        from hermes_cli.config import load_config
        return load_config() or {}
    except Exception:
        return {}


def _resolve_peer(agent: str) -> Optional[dict]:
    """Resolve a peer name to {url, auth, timeout, capabilities}, or treat ``agent`` as a URL."""
    if agent.startswith("http://") or agent.startswith("https://"):
        return {"url": agent, "auth": {}, "timeout": _DEFAULT_TIMEOUT, "capabilities": []}
    cfg = _load_config()
    peers = cfg.get("a2a_agents") or {}
    entry = peers.get(agent)
    if not entry:
        return None
    return {
        "url": entry.get("url", ""),
        "auth": entry.get("auth", {}) or {},
        "timeout": int(entry.get("timeout", _DEFAULT_TIMEOUT)),
        "capabilities": entry.get("capabilities", []) or [],
        "tenant": entry.get("tenant", ""),
    }


def _auth_header(auth: dict) -> dict:
    if auth and auth.get("type") == "bearer" and auth.get("token"):
        return {"Authorization": f"Bearer {auth['token']}"}
    return {}


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

def _http_get_json(url: str, headers: dict, timeout: int) -> dict:
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (configured peers)
        return json.loads(resp.read().decode("utf-8"))


def _http_post_json(url: str, body: dict, headers: dict, timeout: int) -> dict:
    data = json.dumps(body).encode("utf-8")
    hdrs = {"Content-Type": "application/json", "A2A-Version": protocol.PROTOCOL_VERSION, **headers}
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (configured peers)
        return json.loads(resp.read().decode("utf-8"))


def _card_url(base_url: str) -> str:
    # A2A v1.0 canonical discovery path. v0.2 used agent.json; servers may
    # still serve that as a legacy alias, but clients should prefer this.
    return base_url.rstrip("/") + "/.well-known/agent-card.json"


def _legacy_card_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/.well-known/agent.json"


def _fetch_card(base_url: str, headers: dict, timeout: int) -> dict:
    try:
        return _http_get_json(_card_url(base_url), headers, timeout)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    return _http_get_json(_legacy_card_url(base_url), headers, timeout)


def _select_jsonrpc_interface(card: Optional[dict]) -> Optional[dict]:
    if isinstance(card, dict):
        for iface in card.get("supportedInterfaces", []) or []:
            if isinstance(iface, dict) and iface.get("protocolBinding") == "JSONRPC" and iface.get("url"):
                return iface
    return None


def _rpc_url(base_url: str, card: Optional[dict]) -> str:
    """Prefer the card's JSONRPC interface (v1.0 supportedInterfaces), then the
    card's legacy top-level url, then the configured base."""
    iface = _select_jsonrpc_interface(card)
    if iface:
        return str(iface["url"])
    if isinstance(card, dict) and isinstance(card.get("url"), str) and card["url"]:
        return card["url"]
    return base_url.rstrip("/")


def _interface_tenant(card: Optional[dict], peer: dict) -> str:
    iface = _select_jsonrpc_interface(card)
    if iface and iface.get("tenant"):
        return str(iface["tenant"])
    return str(peer.get("tenant") or "")


# --------------------------------------------------------------------------
# Shared send path (used by a2a_call and a2a_orchestrate)
# --------------------------------------------------------------------------

def _short_state(state: str) -> str:
    """TASK_STATE_COMPLETED -> completed (also passes through v0.3 states)."""
    return state.replace("TASK_STATE_", "").replace("_", "-").lower() if state else ""


def _truthy(value: Any) -> bool:
    """Model-supplied booleans arrive as bool, "true"/"false", 1/0 or "yes"/"no"."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _send_task(agent_label: str, peer: dict, message: str, context_id: str) -> tuple[str, str, str]:
    """Send one message/send to a peer. Returns (reply_text, context_id, state).

    Raises urllib errors / ValueError for the caller to format. Handles
    outbound redaction, audit, persistence, and metrics.
    """
    base_url = peer.get("url", "")
    headers = _auth_header(peer.get("auth", {}) or {})
    timeout = int(peer.get("timeout", _DEFAULT_TIMEOUT))

    # Best-effort card fetch (to learn the rpc URL); non-fatal on failure.
    card = None
    try:
        card = _fetch_card(base_url, headers, min(timeout, 30))
    except Exception:
        pass

    ctx = context_id or protocol.new_context_id()
    safe_message = security.redact_outbound(message)
    # v1.0: contextId lives inside the Message, not at the params top level.
    rpc_body = {
        "jsonrpc": "2.0",
        "id": protocol.new_task_id(),
        "method": "SendMessage",
        "params": {
            "message": protocol.text_message(protocol.ROLE_USER, safe_message, context_id=ctx),
        },
    }

    tenant = _interface_tenant(card, peer)
    if tenant:
        rpc_body["params"]["tenant"] = tenant

    security.audit("outbound", agent_label, rpc_body["id"], safe_message)
    # Locally-generated turn: no peer attribution (GitHub issue #5 review
    # finding 1 — our outbound sends must NOT replay as untrusted content).
    protocol.persist_message(ctx, "user", safe_message, rpc_body["id"])
    protocol.metrics.outbound_total += 1

    resp = _http_post_json(_rpc_url(base_url, card), rpc_body, headers, timeout)
    if "error" in resp:
        err = resp["error"]
        raise ValueError(f"Peer '{agent_label}' returned an error: {err.get('message', err)}")

    result = resp.get("result", {})
    payload = protocol.unwrap_send_message_response(result)
    reply = _reply_text_from_result(payload)
    reply_ctx, state = ctx, ""
    if isinstance(payload, dict):
        reply_ctx = payload.get("contextId", ctx)
        state = (payload.get("status") or {}).get("state", "")
    # Peer-generated content: filter + attribute (issue #5, caller side).
    # The reply is the peer's text — the same injection channel the callee
    # side closed; persist it filtered with the peer's name.
    protocol.persist_message(reply_ctx, "agent", security.filter_inbound(reply), rpc_body["id"],
                             peer=agent_label)
    protocol.metrics.inbound_total += 1
    return reply, reply_ctx, state


def _reply_text_from_result(result: Any) -> str:
    result = protocol.unwrap_send_message_response(result)
    if not isinstance(result, dict):
        return str(result)
    # Artifacts first (final output), then status message (interim/clarify).
    for artifact in result.get("artifacts", []) or []:
        txt = protocol.extract_text(artifact)
        if txt:
            return txt
    status = result.get("status", {}) or {}
    msg = status.get("message")
    if msg:
        return protocol.extract_text(msg)
    # Bare message result (message/send may return a Message instead of a Task)
    return protocol.extract_text(result)


# --------------------------------------------------------------------------
# Async task flow (A2A v1.0 §3.1.1/§3.2.2): submit / poll / await / cancel
# --------------------------------------------------------------------------

def _rpc_request(peer: dict, method: str, params: dict, timeout: Optional[int] = None) -> dict:
    """One JSON-RPC round trip to a peer. Returns the full response object."""
    base_url = peer.get("url", "")
    headers = _auth_header(peer.get("auth", {}) or {})
    card = None
    try:
        card = _fetch_card(base_url, headers, min(int(peer.get("timeout", _DEFAULT_TIMEOUT)), 30))
    except Exception:
        pass
    body = {
        "jsonrpc": "2.0",
        "id": protocol.new_task_id(),
        "method": method,
        "params": params,
    }
    tenant = _interface_tenant(card, peer)
    if tenant and "tenant" not in body["params"]:
        body["params"]["tenant"] = tenant
    return _http_post_json(
        _rpc_url(base_url, card), body, headers,
        int(timeout if timeout is not None else peer.get("timeout", _DEFAULT_TIMEOUT)),
    )


def _peer_rpc_error(resp: dict, agent_label: str) -> str:
    err = resp.get("error") or {}
    code = err.get("code", "")
    return f"Error: peer '{agent_label}' returned {code}: {err.get('message', err)}"


def a2a_steer(args: dict, **_: Any) -> str:
    """Queue a non-blocking steering message on an existing A2A context."""
    agent = str(args.get("agent") or args.get("agent_name") or args.get("name") or "").strip()
    message = str(args.get("message") or args.get("text") or "").strip()
    context_id = str(args.get("context_id") or args.get("contextId") or "").strip()
    if not agent or not message or not context_id:
        return "Error: 'agent', 'message', and an existing 'context_id' are required."
    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return f"Error: unknown agent '{agent}'."
    safe_message = security.redact_outbound(message)
    params = {
        "message": protocol.text_message(protocol.ROLE_USER, safe_message, context_id=context_id),
        "configuration": {"returnImmediately": True},
    }
    try:
        resp = _rpc_request(peer, "SendMessage", params)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return f"Error: peer '{agent}' rejected auth (HTTP {e.code}). Check the configured token."
        return f"Error: steering '{agent}' failed — HTTP {e.code}."
    except Exception as e:
        return f"Error: steering '{agent}' failed — {e}."
    if "error" in resp:
        return _peer_rpc_error(resp, agent)
    payload = protocol.unwrap_send_message_response(resp.get("result", {}))
    if not isinstance(payload, dict):
        return f"Error: peer '{agent}' returned a non-task response to steering."
    status = payload.get("status") or {}
    state = str(status.get("state", ""))
    task_id = str(payload.get("id", ""))
    reply_ctx = str(payload.get("contextId", context_id))
    if not task_id:
        return f"Error: peer '{agent}' accepted the steering message but returned no task id."
    _task_store().create(task_id, reply_ctx, agent, direction="outbound", ownership="remote",
                         remote_state=state or protocol.STATE_SUBMITTED)
    protocol.persist_message(reply_ctx, "user", safe_message, task_id)  # local turn, no peer
    protocol.metrics.outbound_total += 1
    return (f"[{agent} · steering task {task_id} · context {reply_ctx} · "
            f"{_short_state(state) or 'submitted'}]\nSteering queued. Poll with "
            f"a2a_get_task(agent='{agent}', task_id='{task_id}') to observe delivery.")


def a2a_submit(args: dict, **_: Any) -> str:
    """Submit a task to a peer without blocking (SendMessage + returnImmediately).

    Returns the taskId/contextId for use with a2a_get_task / a2a_await /
    a2a_cancel. Requires an A2A v1.0 peer that honors returnImmediately; if
    the peer replies with a terminal task anyway, that reply is returned.
    """
    agent = str(args.get("agent") or args.get("agent_name") or args.get("name") or "").strip()
    message = str(args.get("message") or args.get("text") or args.get("task") or "").strip()
    context_id = str(args.get("context_id") or args.get("contextId") or "").strip()
    if not agent or not message:
        return "Error: both 'agent' and 'message' are required."

    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return f"Error: unknown agent '{agent}'. Configure it under 'a2a_agents' in config.yaml or pass a full http(s):// URL."

    ctx = context_id or protocol.new_context_id()
    safe_message = security.redact_outbound(message)
    params = {
        "message": protocol.text_message(protocol.ROLE_USER, safe_message, context_id=ctx),
        "configuration": {"returnImmediately": True},
    }
    try:
        resp = _rpc_request(peer, "SendMessage", params)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return f"Error: peer '{agent}' rejected auth (HTTP {e.code}). Check the configured token."
        return f"Error: submit to '{agent}' failed — HTTP {e.code}."
    except Exception as e:
        return f"Error: submit to '{agent}' failed — {e}."
    if "error" in resp:
        return _peer_rpc_error(resp, agent)

    payload = protocol.unwrap_send_message_response(resp.get("result", {}))
    if not isinstance(payload, dict):
        return f"Error: peer '{agent}' returned a non-task response to an async submit."
    status = payload.get("status") or {}
    state = str(status.get("state", ""))
    task_id = str(payload.get("id", ""))
    reply_ctx = str(payload.get("contextId", ctx))

    # Peer ignored returnImmediately and answered synchronously — hand back
    # the completed reply rather than a dangling task id.
    if state in ("completed", "TASK_STATE_COMPLETED", "failed", "TASK_STATE_FAILED",
                 "canceled", "TASK_STATE_CANCELED", "rejected", "TASK_STATE_REJECTED"):
        text = _reply_text_from_result(payload) or "(no text reply)"
        return f"[{agent} · context {reply_ctx} · {_short_state(state)}]\n{text}"

    if not task_id:
        return f"Error: peer '{agent}' accepted the message but returned no task id."

    _task_store().create(
        task_id, reply_ctx, agent, direction="outbound", ownership="remote",
        remote_state=state or protocol.STATE_SUBMITTED,
    )
    protocol.persist_message(reply_ctx, "user", safe_message, task_id)  # local turn, no peer
    protocol.metrics.outbound_total += 1
    return (
        f"[{agent} · task {task_id} · context {reply_ctx} · "
        f"{_short_state(state) or 'submitted'}]\n"
        f"Task accepted asynchronously. Poll with a2a_get_task(agent='{agent}', "
        f"task_id='{task_id}') or block on a2a_await."
    )


def a2a_get_task(args: dict, **_: Any) -> str:
    """Fetch a task's current state from a peer (GetTask). Non-blocking."""
    agent = str(args.get("agent") or args.get("agent_name") or args.get("name") or "").strip()
    task_id = str(args.get("task_id") or args.get("taskId") or args.get("id") or "").strip()
    if not agent or not task_id:
        return "Error: both 'agent' and 'task_id' are required."
    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return f"Error: unknown agent '{agent}'."
    try:
        resp = _rpc_request(peer, "GetTask", {"id": task_id}, timeout=30)
    except Exception as e:
        return f"Error: GetTask on '{agent}' failed — {e}."
    if "error" in resp:
        # Any JSON-RPC error object is an RPC-level failure, NOT evidence
        # that the peer's task failed. -32601 MethodNotFound (v0.3 peers
        # speak tasks/get), -32001 TaskNotFound (peer GC'd the task or has
        # not yet persisted it), internal errors and future codes all land
        # here. Terminalizing the local row FAILED from this branch
        # poisoned the record permanently (complete() is idempotent-on-
        # terminal), so a later successful poll could never reconcile it —
        # violating the restart-recovery contract (§2 row 5: non-terminal
        # outbound rows reconcile via subsequent GetTask). Leave the state
        # untouched and let reconciliation settle it. Mirrors a2a_poll's
        # error path (tools.py:438), which already returns without
        # terminalizing. See GitHub issue #4.
        return _peer_rpc_error(resp, agent)
    payload = resp.get("result", {})
    if not isinstance(payload, dict):
        return f"Error: unexpected GetTask response from '{agent}'."
    status = payload.get("status") or {}
    raw_state = str(status.get("state", ""))
    state = _short_state(raw_state) or "unknown"
    if raw_state in protocol.TERMINAL_STATES:
        _task_store().complete(task_id, raw_state, _reply_text_from_result(payload))
    else:
        _task_store().set_state(task_id, raw_state or protocol.STATE_WORKING)
    lines = [f"[{agent} · task {payload.get('id', task_id)} · {state}]"]
    msg = status.get("message")
    if msg:
        txt = protocol.extract_text(msg)
        if txt:
            lines.append(txt[:2000])
    for artifact in payload.get("artifacts", []) or []:
        txt = protocol.extract_text(artifact)
        if txt:
            lines.append(f"[artifact] {txt[:4000]}")
    return "\n".join(lines)


_CTX: Any = None
_WATCH_MANAGER: Any = None


def _plugin_home(ctx: Any) -> Optional[str]:
    """The home of the profile this plugin was loaded for (``None`` for a default profile)."""
    home = getattr(getattr(ctx, "_manager", None), "home_path", None)
    return str(home) if home else None


def _plugin_db_path(ctx: Any) -> Optional[str]:
    """The task database of the profile this plugin was loaded for, or None for the default.

    ``PluginContext._manager.home_path`` is the profile's home; the ambient ``HERMES_HOME`` of a
    multiplexed gateway is the LAUNCH profile's, so a watcher started from a secondary profile's
    session would otherwise write its watches into the wrong profile.
    """
    home = getattr(getattr(ctx, "_manager", None), "home_path", None)
    return os.path.join(str(home), "a2a_tasks.db") if home else None


def _watch_manager(ctx: Any = None):
    """The process-wide watch poller, created against the plugin context it was registered with."""
    global _WATCH_MANAGER
    if _WATCH_MANAGER is None:
        from . import watches
        ctx = ctx if ctx is not None else _CTX
        _WATCH_MANAGER = watches.WatchManager(
            ctx, store=watches.WatchStore(_plugin_db_path(ctx)),
            profile_home=_plugin_home(ctx))
    return _WATCH_MANAGER


def _completion_capable(ctx: Any) -> bool:
    """Whether this Hermes runtime exposes the durable plugin completion publisher."""
    return callable(getattr(ctx, "capture_completion_target", None)) and callable(
        getattr(ctx, "publish_completion", None))


def _background_tasks(args: dict) -> list[tuple[str, str]]:
    """Normalise ``tasks=[{agent, task_id}]`` / legacy ``agent`` + ``task_id`` into pairs."""
    raw = args.get("tasks")
    entries: list[tuple[str, str]] = []
    if isinstance(raw, list) and raw:
        for entry in raw:
            if not isinstance(entry, dict):
                raise ValueError("each entry in 'tasks' must be an object with 'agent' and 'task_id'.")
            agent = str(entry.get("agent") or entry.get("agent_name") or entry.get("name") or "").strip()
            task_id = str(entry.get("task_id") or entry.get("taskId") or entry.get("id") or "").strip()
            if not agent or not task_id:
                raise ValueError("each entry in 'tasks' needs a non-empty 'agent' and 'task_id'.")
            entries.append((agent, task_id))
    else:
        agent = str(args.get("agent") or args.get("agent_name") or args.get("name") or "").strip()
        task_id = str(args.get("task_id") or args.get("taskId") or args.get("id") or "").strip()
        if not agent or not task_id:
            raise ValueError("background mode needs 'agent' and 'task_id', or a 'tasks' list.")
        entries.append((agent, task_id))
    seen: set[tuple[str, str]] = set()
    unique = []
    for entry in entries:
        if entry not in seen:
            seen.add(entry)
            unique.append(entry)
    return unique


def _start_background_watch(args: dict, session_id: Optional[str]) -> str:
    """``a2a_await(background=True)``: bind a delivery target, persist the watch, return its id."""
    try:
        entries = _background_tasks(args)
    except ValueError as exc:
        return f"Error: {exc}"
    unknown = [agent for agent, _task in entries if not (_resolve_peer(agent) or {}).get("url")]
    if unknown:
        return (f"Error: unknown agent '{unknown[0]}'. Configure it under 'a2a_agents' in "
                f"config.yaml or pass a full http(s):// URL.")
    # Ownership first: only unowned (or already ours) tasks may be watched. A refusal here names
    # the task instead of starting a watch that could never deliver.
    for agent_name, task_id in entries:
        ok, refusal = _acquire_task(task_id, session_id or "", agent_name)
        if not ok:
            return refusal
    ctx = _CTX
    if not _completion_capable(ctx):
        return (
            "Error: background a2a_await needs a Hermes runtime with the plugin completion "
            "publisher (PluginContext.capture_completion_target / publish_completion). "
            "Use blocking a2a_await, or poll with a2a_get_task, on this runtime.")
    try:
        target = ctx.capture_completion_target(session_id=session_id or None)
    except Exception as exc:  # CompletionTargetError / CompletionDeliveryUnsupported
        if type(exc).__name__ == "CompletionDeliveryUnsupported":
            return (
                "Error: this session cannot receive a later turn (one-shot, cron, Kanban/worker, "
                "or a stateless API session), so a background watch has nowhere to deliver its "
                "result. Use blocking a2a_await or poll with a2a_get_task.")
        return f"Error: could not bind a background watch to this session — {exc}"
    manager = _watch_manager(ctx)
    try:
        watch = manager.add(session_id=session_id or "", target=target, tasks=entries)
    except Exception as exc:
        logger.exception("A2A: background watch creation failed")
        return f"Error: could not start the background watch — {exc}"

    # Anything the task produced while nobody owned it is delivered now, through the target just
    # captured for this session.
    delivered = 0
    for agent_name, task_id in entries:
        queued = _queued_backlog(task_id)
        accepted = _publish_queued(ctx, task_id, agent_name, session_id or "", target, queued)
        if accepted:
            # Only what the host accepted: a refused message stays queued for the next await.
            _mark_backlog_delivered(
                task_id, [m for m in queued if m["message_id"] in accepted], session_id or "")
            delivered += len(accepted)

    watch_id = str(watch.get("watch_id") or "")
    ttl_hours = manager.ttl_s / 3600.0
    lines = [
        f"[watch {watch_id} · {len(entries)} task(s) · background]",
        "Watching:",
        *(f"  - {peer} · task {task_id}" for peer, task_id in entries),
        *([f"Delivered {delivered} queued message(s) from before this session owned the task."]
          if delivered else []),
        "",
        f"The watch polls every {int(manager.interval)}s (with backoff on transient peer errors) "
        f"and expires after {ttl_hours:.0f}h. Continue with other work: when a task reaches a "
        f"terminal state its result is delivered to this conversation as a new turn. Do not poll "
        f"these tasks yourself. a2a_watch_cancel(watch_id='{watch_id}') stops local watching "
        f"without cancelling remote work.",
    ]
    return "\n".join(lines)




# ---------------------------------------------------------------------------
# Task ownership (protocol: a task is unowned or owned by exactly one session)
# ---------------------------------------------------------------------------

def _ownership(ctx: Any = None):
    from . import ownership
    ctx = ctx if ctx is not None else _CTX
    return ownership.OwnershipStore(_plugin_db_path(ctx))


def _ensure_task_record(task_id: str, peer: str, context_id: str = "",
                        remote_state: str = "") -> dict:
    """Create the durable task record when the caller only knows the id (adoption)."""
    store = _task_store()
    record = store.get(task_id)
    if record is None:
        record = store.create(task_id, context_id or task_id, peer, direction="outbound",
                              ownership="remote", remote_state=remote_state)
    return record


def _acquire_task(task_id: str, session_id: str, peer: str) -> tuple[bool, str]:
    """Acquire ``task_id`` for this session. Returns (ok, error message)."""
    _ensure_task_record(task_id, peer)
    if not session_id:
        return True, ""  # no bound session (one-shot caller): ownership is not tracked
    store = _ownership()
    owner = store.owner(task_id)
    if owner and owner != session_id:
        return False, (f"Error: a2a task {task_id} is owned by another session. "
                       f"Only unowned tasks can be adopted; ask the owning session to release it.")
    if not store.acquire(task_id, session_id):
        return False, (f"Error: a2a task {task_id} is owned by another session. "
                       f"Only unowned tasks can be adopted.")
    _ensure_maintenance()
    return True, ""


def _queued_backlog(task_id: str) -> list[dict]:
    try:
        return _ownership().queued(task_id)
    except Exception:
        logger.debug("A2A: backlog read failed", exc_info=True)
        return []


def _mark_backlog_delivered(task_id: str, messages: list[dict], session_id: str) -> None:
    if not messages or not session_id:
        return
    try:
        _ownership().mark_delivered(task_id, [m["message_id"] for m in messages], session_id)
    except Exception:
        logger.debug("A2A: backlog delivery marker failed", exc_info=True)


def _ensure_maintenance(ctx: Any = None) -> None:
    """The auto-cancel/TTL timers run in the watch poller; start it after an ownership change."""
    try:
        _watch_manager(ctx).start()
    except Exception:
        logger.debug("A2A: could not start the maintenance poller", exc_info=True)


def _backlog_lines(messages: list[dict]) -> list[str]:
    """Render queued peer messages for an inline (blocking await) return, or for a tool result."""
    lines: list[str] = []
    for msg in messages:
        kind = str(msg.get("kind") or "progress")
        text = str(msg.get("text") or "").strip()
        if not text:
            continue
        label = {"result": "result", "progress": "progress", "input-required": "needs input",
                 "error": "error"}.get(kind, kind)
        lines.append(f"[queued {label}] {text}")
    return lines


def _record_observation(task_id: str, payload: dict, peer: str) -> list[dict]:
    """Store every new peer message on the task; returns the ones still queued (all of them)."""
    from . import ownership
    store = ownership.OwnershipStore(_plugin_db_path(_CTX))
    state = str((payload.get("status") or {}).get("state") or "")
    context_id = str(payload.get("contextId") or "")
    _ensure_task_record(task_id, peer, context_id, state)
    try:
        status_msg = (payload.get("status") or {}).get("message") or {}
        message_id = str(status_msg.get("messageId") or status_msg.get("message_id") or "")
        status_text = _text_of_message(status_msg)
        if status_text:
            kind = "input-required" if state == protocol.STATE_INPUT_REQUIRED else "progress"
            store.add_message(task_id, message_id or f"status:{_digest(status_text)}", kind, status_text)
        for index, artifact in enumerate(payload.get("artifacts") or []):
            text = _text_of_message(artifact)
            if not text:
                continue
            artifact_id = str(artifact.get("artifactId") or artifact.get("artifact_id")
                              or f"artifact:{index}:{_digest(text)}")
            store.add_message(task_id, artifact_id, "result", text)
        if state in protocol.TERMINAL_STATES and not any(
                m["kind"] == "result" for m in store.queued(task_id)):
            store.add_message(task_id, f"state:{state}", "result",
                              f"task reached {_short_state(state)} with no text reply")
    except Exception:
        logger.debug("A2A: message recording failed", exc_info=True)
    return store.queued(task_id)


def _text_of_message(message: Any) -> str:
    if not isinstance(message, dict):
        return ""
    parts = message.get("parts")
    if isinstance(parts, list):
        texts = [str(part.get("text") or "") for part in parts
                 if isinstance(part, dict) and part.get("kind", "text") in ("text", "")]
        joined = "\n".join(t for t in texts if t).strip()
        if joined:
            return joined
    return str(message.get("text") or "").strip()


def _digest(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:16]


def _publish_queued(ctx: Any, task_id: str, peer: str, session_id: str, target: Any,
                    messages: list[dict]) -> list[str]:
    """Publish undelivered peer messages; returns the message ids the host accepted."""
    published: list[str] = []
    for msg in messages:
        text = str(msg.get("text") or "").strip()
        if not text:
            continue
        kind = str(msg.get("kind") or "progress")
        content = _peer_message_content(peer, task_id, kind, text)
        try:
            ctx.publish_completion(
                target=target, event_id=f"a2a:{task_id}:{msg['message_id']}", content=content,
                metadata={"source": "a2a", "peer": peer, "task_id": task_id,
                          "state": kind, "message_id": msg["message_id"]})
        except Exception as exc:
            logger.warning("A2A: could not publish %s/%s: %s", task_id, msg["message_id"], exc)
            continue
        published.append(str(msg["message_id"]))
    return published


# The core publisher refuses content over 32,000 characters. Bound every notification below that
# with room for the header: an oversized result must arrive truncated, not retry forever.
MAX_NOTIFICATION_CHARS = 24_000


def _bound(text: str, limit: int = MAX_NOTIFICATION_CHARS) -> str:
    body = str(text or "")
    if len(body) <= limit:
        return body
    return body[:limit] + f"\n…[{len(body) - limit} characters truncated]"


def _peer_message_content(peer: str, task_id: str, kind: str, text: str) -> str:
    if kind == "result":
        head = f"A2A task {task_id} on peer '{peer}' returned a result."
    elif kind == "input-required":
        head = (f"A2A task {task_id} on peer '{peer}' needs input before it can finish. "
                f"Reply with a2a_submit using the task's context_id.")
    elif kind == "error":
        head = f"A2A task {task_id} on peer '{peer}' reported a problem."
    else:
        head = f"A2A task {task_id} on peer '{peer}' sent a progress note."
    return f"{head}\n\n{_bound(text)}"


def a2a_await(args: dict, session_id: Optional[str] = None, **_: Any) -> str:
    """Block until a submitted task reaches a terminal state, or watch it in the background.

    Blocking (default): polls GetTask every ``interval`` seconds (default 5) until the task is
    terminal or ``timeout`` (default 600s) elapses. With ``background=true`` the watch is
    persisted and polled off-turn; its result arrives later as a new turn in this session.
    """
    if _truthy(args.get("background")):
        return _start_background_watch(args, session_id)
    agent = str(args.get("agent") or args.get("agent_name") or args.get("name") or "").strip()
    task_id = str(args.get("task_id") or args.get("taskId") or args.get("id") or "").strip()
    try:
        timeout = max(5, min(int(args.get("timeout") or 600), 3600))
    except (TypeError, ValueError):
        timeout = 600
    try:
        interval = max(1, min(int(args.get("interval") or 5), 60))
    except (TypeError, ValueError):
        interval = 5
    if not agent or not task_id:
        return "Error: both 'agent' and 'task_id' are required."
    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return f"Error: unknown agent '{agent}'."

    # Ownership: a session-level fact. This call acquires an unowned task, refuses one owned by
    # another session, and keeps ownership after the await returns.
    ok, refusal = _acquire_task(task_id, session_id or "", agent)
    if not ok:
        return refusal
    backlog = _queued_backlog(task_id)
    preface = _backlog_lines(backlog)

    deadline = time.time() + timeout
    last_state = ""
    while True:
        try:
            resp = _rpc_request(peer, "GetTask", {"id": task_id}, timeout=30)
        except Exception as e:
            _mark_backlog_delivered(task_id, backlog, session_id or "")
            prefix = ("\n".join(preface) + "\n\n") if preface else ""
            return prefix + f"Error: poll of '{agent}' failed — {e}."
        if "error" in resp:
            _mark_backlog_delivered(task_id, backlog, session_id or "")
            prefix = ("\n".join(preface) + "\n\n") if preface else ""
            return prefix + _peer_rpc_error(resp, agent)
        payload = resp.get("result", {})
        if isinstance(payload, dict):
            status = payload.get("status") or {}
            state = str(status.get("state", ""))
            if state != last_state:
                logger.debug("A2A await: task %s now %s", task_id, state)
                last_state = state
            short = _short_state(state)
            queued_now = _record_observation(task_id, payload, agent)
            if short in ("completed", "failed", "canceled", "rejected", "input-required"):
                results = [str(m.get("text") or "").strip() for m in queued_now
                           if str(m.get("kind")) == "result" and str(m.get("text") or "").strip()]
                text = "\n\n".join(results) if results else (
                    _reply_text_from_result(payload) or "(no text reply)")
                if state in protocol.TERMINAL_STATES:
                    _task_store().complete(task_id, state, text)
                else:
                    _task_store().set_state(task_id, state)
                # Persist the terminal peer reply: filtered + peer-attributed
                # (issue #5 caller side — review finding 1). Without this the
                # a2a_await result never reached the transcript, and the peer's
                # text would enter later context only via the tool result,
                # unframed.
                reply_ctx = str(payload.get("contextId", "")) if isinstance(payload, dict) else ""
                if reply_ctx and text:
                    protocol.persist_message(reply_ctx, "agent", security.filter_inbound(text),
                                             task_id, peer=agent)
                # Everything the peer said is in this return value: mark it delivered so a later
                # background await does not repeat it.
                _mark_backlog_delivered(task_id, queued_now, session_id or "")
                header = f"[{agent} · task {payload.get('id', task_id)} · {short}]"
                body = text
                if short == "input-required":
                    ctx = str(payload.get("contextId", ""))
                    body += f"\n\n(Peer needs input — reply via a2a_submit with context_id '{ctx}'.)"
                progress = _backlog_lines(
                    [m for m in queued_now if str(m.get("kind")) != "result"])
                prefix = "\n".join(preface + progress)
                return f"{prefix}\n\n{header}\n{body}" if prefix else f"{header}\n{body}"
        if time.time() >= deadline:
            observed = _queued_backlog(task_id)
            _mark_backlog_delivered(task_id, observed, session_id or "")
            prefix = "\n".join(preface + _backlog_lines(observed))
            note = (f"[{agent} · task {task_id} · still working after {timeout}s]\n"
                    f"Call a2a_await again or check a2a_get_task.")
            return f"{prefix}\n\n{note}" if prefix else note
        time.sleep(interval)


def a2a_watch_cancel(args: dict, session_id: Optional[str] = None, **_: Any) -> str:
    """Stop a background watch. Local polling only: the remote task keeps running."""
    watch_id = str(args.get("watch_id") or args.get("watchId") or args.get("id") or "").strip()
    if not watch_id:
        return "Error: 'watch_id' is required."
    try:
        prior = _watch_manager().cancel(watch_id, session_id=session_id or "")
    except Exception as exc:
        return f"Error: {exc}"
    if prior is None:
        return f"Error: unknown watch '{watch_id}'."
    if prior != "active":
        return (f"[watch {watch_id} · {prior}] Nothing to stop: the watch is already {prior}. "
                f"Remote tasks are unchanged.")
    return (f"[watch {watch_id} · canceled] Local polling stopped. The remote tasks keep running; "
            f"use a2a_cancel(agent=..., task_id=...) to request remote cancellation.")




def a2a_acquire_task_ownership(args: dict, session_id: Optional[str] = None, **_: Any) -> str:
    """Take ownership of unowned A2A tasks so their messages are delivered to this session.

    Only tasks that are unowned can be adopted: a task owned by another session is refused. The
    queued messages are delivered when this session awaits the task (``a2a_await``).
    """
    if not session_id:
        return "Error: no session is bound to this call, so ownership cannot be recorded."
    try:
        entries = _background_tasks(args)
    except ValueError as exc:
        return f"Error: {exc}"
    store = _ownership()
    lines: list[str] = []
    for agent_name, task_id in entries:
        peer = _resolve_peer(agent_name)
        if not peer or not peer.get("url"):
            lines.append(f"  - {agent_name} / {task_id}: unknown peer")
            continue
        _ensure_task_record(task_id, agent_name)
        owner = store.owner(task_id)
        if owner == session_id:
            lines.append(f"  - {agent_name} / {task_id}: already owned by this session")
            continue
        if owner:
            lines.append(f"  - {agent_name} / {task_id}: REFUSED — owned by another session")
            continue
        if store.acquire(task_id, session_id):
            lines.append(f"  - {agent_name} / {task_id}: acquired")
        else:
            lines.append(f"  - {agent_name} / {task_id}: REFUSED — claimed by another session")
    return "A2A task ownership:\n" + "\n".join(lines)


def a2a_release_task_ownership(args: dict, session_id: Optional[str] = None, **_: Any) -> str:
    """Give up ownership of A2A tasks. They become adoptable by any session; polling stops.

    This is separate from ``a2a_watch_cancel``: cancelling a watch stops polling but keeps
    ownership. Release is what makes a task adoptable elsewhere (a handoff to another session).
    """
    if not session_id:
        return "Error: no session is bound to this call."
    store = _ownership()
    if _truthy(args.get("all")):
        released_watches = 0
        try:
            released_watches = _watch_manager().cancel_session(session_id)
        except Exception:
            logger.debug("A2A: stopping watches on release failed", exc_info=True)
        count = store.release_all(session_id)
        _ensure_maintenance()
        return (f"Released {count} task(s) owned by this session; stopped {released_watches} "
                f"local watch(es). The remote tasks keep running and are now adoptable.")
    try:
        entries = _background_tasks(args)
    except ValueError as exc:
        return f"Error: {exc}"
    lines: list[str] = []
    task_ids: list[str] = []
    for agent_name, task_id in entries:
        if store.release(task_id, session_id):
            task_ids.append(task_id)
            lines.append(f"  - {agent_name} / {task_id}: released")
        else:
            lines.append(f"  - {agent_name} / {task_id}: not released (not owned by this session)")
    if task_ids:
        try:
            _watch_manager().cancel_session(session_id, task_ids=task_ids)
        except Exception:
            logger.debug("A2A: stopping released watches failed", exc_info=True)
        _ensure_maintenance()
    return "A2A task ownership:\n" + "\n".join(lines)


# ---------------------------------------------------------------------------
# Session lifecycle: release on end, reclaim on resume
# ---------------------------------------------------------------------------

_PENDING_NOTES: list[str] = []


def _take_notes() -> str:
    """Take queued ownership notes for the next tool result (fallback when no live agent)."""
    global _PENDING_NOTES
    if not _PENDING_NOTES:
        return ""
    notes, _PENDING_NOTES = list(_PENDING_NOTES), []
    return "\n".join(notes) + "\n\n"


def _ownership_note(reclaimed: list[str], lost: list[str]) -> str:
    parts = ["A2A task ownership changed while this session was away:"]
    if reclaimed:
        parts.append("  reclaimed: " + ", ".join(reclaimed))
        parts.append("  Their results are queued; a2a_await each task id to receive them.")
    if lost:
        parts.append("  lost (adopted by another session): " + ", ".join(lost))
    return "\n".join(parts)


def _on_session_start(ctx: Any, payload: dict) -> None:
    session_id = str(payload.get("session_id") or "")
    if not session_id:
        return
    try:
        reclaimed, lost = _ownership().reclaim(session_id)
    except Exception:
        logger.debug("A2A: ownership reclaim failed", exc_info=True)
        return
    if not reclaimed and not lost:
        return
    note = _ownership_note(reclaimed, lost)
    queued = getattr(ctx, "queue_context_note", None)
    if callable(queued):
        try:
            if queued(note, key="a2a-ownership"):
                logger.info("A2A: ownership note queued for session %s", session_id)
                return
        except Exception:
            logger.debug("A2A: queue_context_note failed", exc_info=True)
    _PENDING_NOTES.append(note)


def _on_session_end(ctx: Any, payload: dict, *, boundary: str = "reset") -> None:
    # /new and /reset name the OUTGOING session as old_session_id; session_id is the new one.
    session_id = str(payload.get("old_session_id") or payload.get("session_id") or "")
    if not session_id:
        return
    # A finalize is not always an end: on a gateway-hosted local chat (and a TUI tab) it fires
    # when the CLIENT detaches after a turn, while the session and its background work continue.
    # Only the interactive CLI's finalize means "this session is over". Explicit /new and /reset
    # always release.
    platform = str(payload.get("platform") or "")
    if boundary == "finalize" and platform != "cli":
        logger.debug("A2A: session %s finalized on %s; ownership retained", session_id, platform or "?")
        return
    try:
        _watch_manager(ctx).cancel_session(session_id)
    except Exception:
        logger.debug("A2A: stopping watches on session end failed", exc_info=True)
    try:
        released = _ownership(ctx).release_all(session_id)
    except Exception:
        logger.debug("A2A: ownership release on session end failed", exc_info=True)
        return
    if released:
        logger.info("A2A: released %d task(s) owned by ended session %s", released, session_id)


def _install_session_hooks(ctx: Any) -> None:
    """Register session-boundary hooks.

    These are lifecycle hooks (``invoke_hook``), not plugin-bus events: ``ctx.subscribe`` listens on
    ``ctx.emit`` traffic and never receives them, so a plugin that subscribes here would keep
    polling for sessions that ended.
    """
    register = getattr(ctx, "register_hook", None)
    if not callable(register):
        logger.debug("A2A: runtime has no register_hook; task ownership will not auto-release")
        return

    def start(**payload: Any) -> None:
        _on_session_start(ctx, payload)

    def reset(**payload: Any) -> None:
        _on_session_end(ctx, payload, boundary="reset")

    def finalize(**payload: Any) -> None:
        _on_session_end(ctx, payload, boundary="finalize")

    for event, handler in (("on_session_start", start), ("on_session_reset", reset),
                           ("on_session_finalize", finalize)):
        try:
            register(event, handler)
        except Exception:
            logger.debug("A2A: could not register %s", event, exc_info=True)


def a2a_cancel(args: dict, **_: Any) -> str:
    """Request cancellation of a previously submitted task (CancelTask)."""
    agent = str(args.get("agent") or args.get("agent_name") or args.get("name") or "").strip()
    task_id = str(args.get("task_id") or args.get("taskId") or args.get("id") or "").strip()
    if not agent or not task_id:
        return "Error: both 'agent' and 'task_id' are required."
    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return f"Error: unknown agent '{agent}'."
    try:
        resp = _rpc_request(peer, "CancelTask", {"id": task_id}, timeout=30)
    except Exception as e:
        return f"Error: CancelTask on '{agent}' failed — {e}."
    if "error" in resp:
        return _peer_rpc_error(resp, agent)
    payload = resp.get("result", {})
    state = ""
    if isinstance(payload, dict):
        state = _short_state(str((payload.get("status") or {}).get("state", "")))
    return f"[{agent} · task {task_id} · {state or 'cancel requested'}]"


# --------------------------------------------------------------------------
# Tool handlers
# --------------------------------------------------------------------------

def a2a_discover(args: dict, **_: Any) -> str:
    """Fetch and summarize the Agent Card at ``url``."""
    url = str(args.get("url") or "").strip()
    if not url:
        return "Error: 'url' is required (e.g. http://localhost:9999)."
    try:
        card = _fetch_card(url, {}, _DEFAULT_TIMEOUT)
    except urllib.error.HTTPError as e:
        return f"Error: discovery failed — HTTP {e.code} from {url}."
    except Exception as e:
        return f"Error: could not reach {url} — {e}."

    name = card.get("name", "?")
    desc = card.get("description", "")
    caps = card.get("capabilities", {}) or {}
    skills = card.get("skills", []) or []
    auth = "yes" if card.get("security") else "no"
    ifaces = card.get("supportedInterfaces", []) or []
    proto = ", ".join(
        f"{i.get('protocolBinding', '?')} v{i.get('protocolVersion', '?')}"
        for i in ifaces if isinstance(i, dict)
    ) or f"v{card.get('protocolVersion', '?')} (pre-1.0 card)"
    lines = [
        f"Agent: {name}",
        f"Description: {desc}",
        f"URL: {_rpc_url(url, card)}",
        f"Protocol: {proto}",
        f"Streaming: {bool(caps.get('streaming'))}  Push: {bool(caps.get('pushNotifications'))}  Auth required: {auth}",
        f"Skills ({len(skills)}):",
    ]
    for s in skills[:20]:
        lines.append(f"  - {s.get('name', s.get('id', '?'))}: {s.get('description', '')}")
    return "\n".join(lines)


def a2a_call(args: dict, **_: Any) -> str:
    """Send a task to a peer agent and return its reply.

    ``agent`` is a configured peer name (from ``a2a_agents``) or a direct URL.
    ``context_id`` continues a prior exchange (multi-turn) when provided.
    """
    # Accept common aliases models reach for (observed live: 'agent_name').
    agent = str(args.get("agent") or args.get("agent_name") or args.get("name") or "").strip()
    message = str(args.get("message") or args.get("text") or args.get("task") or "").strip()
    context_id = str(args.get("context_id") or args.get("contextId") or "").strip()
    if not agent or not message:
        return "Error: both 'agent' and 'message' are required."

    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return (
            f"Error: unknown agent '{agent}'. Configure it under 'a2a_agents' in "
            f"config.yaml or pass a full http(s):// URL."
        )

    try:
        reply, reply_ctx, state = _send_task(agent, peer, message, context_id)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return f"Error: peer '{agent}' rejected auth (HTTP {e.code}). Check the configured token."
        if e.code == 429:
            return f"Error: peer '{agent}' rate limited us (HTTP 429). Retry later."
        return f"Error: call to '{agent}' failed — HTTP {e.code}."
    except ValueError as e:
        return str(e)
    except Exception as e:
        return f"Error: call to '{agent}' failed — {e}."

    header = f"[{agent} · context {reply_ctx}"
    if state:
        header += f" · {_short_state(state)}"
    header += "]"
    body = reply or "(no text reply)"
    if state == protocol.STATE_INPUT_REQUIRED:
        body += (
            "\n\n(The peer needs more input — answer by calling a2a_call again "
            f"with context_id '{reply_ctx}'.)"
        )
    return f"{header}\n{body}"


def a2a_list(args: dict | None = None, session_id: Optional[str] = None, **_: Any) -> str:
    """List configured peers, conversations, metrics, and durable task records.

    Task records are local snapshots. Remote state changes only when another A2A
    operation explicitly queries the peer.
    """
    args = args or {}
    cfg = _load_config()
    peers = cfg.get("a2a_agents") or {}
    kind = str(args.get("kind") or "all").lower()
    state_filter = str(args.get("state") or "all").lower()
    direction_filter = str(args.get("direction") or "all").lower()
    peer_filter = str(args.get("peer") or "").strip()
    try:
        limit = max(1, min(int(args.get("limit") or 50), 100))
    except (ValueError, TypeError):
        limit = 50
    if kind not in ("all", "peers", "conversations", "tasks"):
        return "Error: 'kind' must be one of: all, peers, conversations, tasks."
    if state_filter not in ("all", "working", "terminal"):
        return "Error: 'state' must be one of: all, working, terminal."
    if direction_filter not in ("all", "inbound", "outbound"):
        return "Error: 'direction' must be one of: all, inbound, outbound."
    lines = []
    if kind in ("all", "peers"):
        if peers:
            lines.append(f"Configured peers ({len(peers)}):")
            for name, entry in peers.items():
                auth = (entry.get("auth") or {}).get("type", "none")
                caps = entry.get("capabilities", [])
                cap_str = f" caps: {', '.join(caps)}" if caps else ""
                lines.append(f"  - {name}: {entry.get('url', '?')} (auth: {auth}){cap_str}")
        else:
            lines.append("No peers configured. Add them under 'a2a_agents' in config.yaml.")

    if kind == "peers":
        return "\n".join(lines) if lines else "No peers configured. Add them under 'a2a_agents' in config.yaml."

    convos = protocol.list_conversations()
    if kind in ("all", "conversations") and convos:
        lines.append("")
        lines.append(f"Persisted conversations ({len(convos)}) — recall with a2a_history:")
        for c in convos[:25]:
            lines.append(f"  - {c}")

    if kind in ("all", "tasks"):
        records, _, _ = protocol.TaskStore().list(page_size=limit, with_total=True)
        records = [r for r in records if not peer_filter or r.get("peer") == peer_filter]
        if state_filter != "all":
            records = [r for r in records if (r.get("state") in protocol.TERMINAL_STATES) == (state_filter == "terminal")]
        if direction_filter != "all":
            records = [r for r in records if r.get("direction") == direction_filter]
        lines.append("")
        lines.append(f"Tasks ({len(records)}):")
        visible = records[:limit]
        try:
            owners = _ownership().owners([r.get("task_id", "") for r in visible])
        except Exception:
            owners = {}
        for r in visible:
            state = str(r.get("state", "")).replace("TASK_STATE_", "").lower()
            task_id = r.get("task_id")
            owner = owners.get(task_id, "")
            if not owner:
                owned = "unowned"
            elif session_id and owner == session_id:
                owned = "owned-by-this-session"
            else:
                owned = "owned-by-another-session"
            lines.append(f"  - {task_id} context={r.get('context_id')} peer={r.get('peer')} "
                         f"direction={r.get('direction', 'inbound')} ownership={r.get('ownership', 'local')} "
                         f"owner={owned} state={state}")
        if not records:
            lines.append("  (none)")

    if kind == "tasks":
        return _take_notes() + ("\n".join(lines) if lines else "No A2A tasks found.")

    # Show metrics snapshot
    m = protocol.metrics.snapshot()
    lines.append("")
    lines.append(f"Metrics: {m['inbound_total']} in / {m['outbound_total']} out, "
                 f"{m['tasks_completed']} completed, {m['tasks_failed']} failed, "
                 f"{m['streams_started']} streams, {m['push_sent']} push sent, "
                 f"{m['anti_loop_triggers']} anti-loop, {m['rate_limit_triggers']} rate-limited, "
                 f"avg {m['avg_latency_ms']}ms")

    return _take_notes() + "\n".join(lines)


def a2a_history(args: dict, **_: Any) -> str:
    """Recall a persisted A2A conversation by context_id.

    This is how prior A2A exchanges survive compaction/restarts: every
    turn is written to the profile-scoped a2a_conversations/<context>.jsonl
    and can be reloaded here. Peer-generated turns are framed as untrusted
    remote content on replay (GitHub issue #5); locally-generated turns
    (our own sends and replies) replay unframed.
    """
    context_id = str(args.get("context_id") or args.get("contextId") or "").strip()
    if not context_id:
        return "Error: 'context_id' is required (see a2a_list for known conversations)."
    try:
        limit = max(1, min(int(args.get("limit") or 50), 200))
    except (ValueError, TypeError):
        limit = 50
    messages = protocol.load_conversation(context_id, limit=limit)
    if not messages:
        return f"No persisted conversation for context '{context_id}'."
    lines = [f"Conversation {context_id} (last {len(messages)} messages):"]
    for m in messages:
        role = m.get("role", "?")
        text = (m.get("text") or "").strip()
        if len(text) > 1000:
            text = text[:1000] + " …[truncated]"
        if m.get("peer"):
            # Remote turn (peer explicit): frame it. Applies to inbound
            # callee turns AND peer replies on the caller side — the peer
            # field is the source of truth for origin, not the role.
            # GitHub issue #5 + review finding 1.
            lines.append(f"[peer:{m['peer']} · untrusted remote content] {text}")
        elif m.get("origin") == "local":
            # Locally-generated turn (our outbound sends on the caller
            # side, our replies on the callee side): replay unframed.
            lines.append(f"[{role}] {text}")
        elif role == "user":
            # Legacy callee-side inbound record persisted before issue #5
            # shipped — no origin marker, no peer. In A2A sessions the
            # only [user] turns the callee adapter writes are remote by
            # construction; frame with an unknown peer (fail-safe).
            lines.append(f"[peer:unknown · untrusted remote content] {text}")
        else:
            # Legacy [agent] record: callee replies were locally generated
            # by construction; replay unframed.
            lines.append(f"[{role}] {text}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# a2a_orchestrate: capability-based routing with fan-out
# --------------------------------------------------------------------------

def _match_peers_by_capability(capability: str) -> list[tuple[str, dict]]:
    """Find configured peers that advertise the given capability."""
    cfg = _load_config()
    peers = cfg.get("a2a_agents") or {}
    matches = []
    for name, entry in peers.items():
        caps = entry.get("capabilities", []) or []
        if capability in caps or capability == "*":
            matches.append((name, entry))
    return matches


def _call_peer_sync(agent_name: str, peer_entry: dict, message: str, context_id: str = "") -> tuple[str, str]:
    """Call a single peer synchronously. Returns (agent_name, reply_text)."""
    try:
        peer = {
            "url": peer_entry.get("url", ""),
            "auth": peer_entry.get("auth", {}) or {},
            "timeout": int(peer_entry.get("timeout", _DEFAULT_TIMEOUT)),
        }
        reply, _ctx, _state = _send_task(agent_name, peer, message, context_id)
        return (agent_name, reply or "(no reply)")
    except Exception as e:
        return (agent_name, f"Error: {e}")


def a2a_orchestrate(args: dict, **_: Any) -> str:
    """Fan-out a task to multiple peer agents by capability.

    Modes:
      - ``all``: send to all peers matching the capability, return all replies.
      - ``first``: send to all matching peers, return the first successful reply.
      - ``best``: send to all, return the longest successful reply (a coarse
        detail heuristic — use ``all`` when you want to judge yourself).

    Configured peers advertise capabilities in config.yaml::

      a2a_agents:
        researcher:
          url: "http://localhost:9991"
          capabilities: [web_search, research]
        coder:
          url: "http://localhost:9992"
          capabilities: [code, debug]
    """
    capability = str(args.get("capability") or "").strip()
    message = str(args.get("message") or args.get("task") or "").strip()
    mode = str(args.get("mode") or "all").strip().lower()
    context_id = str(args.get("context_id") or "").strip()

    if not message:
        return "Error: 'message' is required."
    if not capability:
        return "Error: 'capability' is required (or use '*' for all peers)."

    matches = _match_peers_by_capability(capability)
    if not matches:
        return f"Error: no configured peers advertise capability '{capability}'."

    if mode not in ("all", "first", "best"):
        mode = "all"

    # Fan-out
    results: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(len(matches), _ORCHESTRATE_MAX_WORKERS)) as pool:
        futures = {
            pool.submit(_call_peer_sync, name, entry, message, context_id): name
            for name, entry in matches
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results.append(fut.result())
                if mode == "first" and not results[-1][1].startswith("Error:"):
                    # Got a good reply; cancel peers that haven't started yet.
                    for f in futures:
                        f.cancel()
                    break
            except Exception as e:
                results.append((name, f"Error: {e}"))

    # Sort results by peer name for deterministic output
    results.sort(key=lambda r: r[0])
    successes = [(name, reply) for name, reply in results if not reply.startswith("Error:")]

    def _all_failed() -> str:
        lines = ["All peers failed:"]
        for name, reply in results:
            lines.append(f"  {name}: {reply}")
        return "\n".join(lines)

    if mode == "best":
        if not successes:
            return _all_failed()
        best = max(successes, key=lambda r: len(r[1]))
        return f"[best: {best[0]}]\n{best[1]}"
    elif mode == "first":
        if not successes:
            return _all_failed()
        name, reply = successes[0]
        return f"[first: {name}]\n{reply}"
    else:  # mode == "all"
        lines = [f"Orchestrated '{capability}' to {len(matches)} peer(s):"]
        for name, reply in results:
            lines.append(f"\n--- {name} ---")
            lines.append(reply)
        return "\n".join(lines)


# --------------------------------------------------------------------------
# Tool schemas + registration
# --------------------------------------------------------------------------

_FunctionSchema = TypedDict("_FunctionSchema", {"name": str, "description": str, "parameters": dict[str, Any]}, total=False)
_ToolSchema = TypedDict("_ToolSchema", {"type": str, "function": _FunctionSchema}, total=False)
_SCHEMAS: dict[str, _ToolSchema] = {
    "a2a_discover": {
        "type": "function",
        "function": {
            "name": "a2a_discover",
            "description": (
                "Fetch and summarize another agent's A2A Agent Card from a URL "
                "(its name, description, capabilities, and skills). Use this to "
                "find out what a remote agent can do before calling it."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "Base URL of the remote A2A agent, e.g. http://localhost:9999"},
                },
                "required": ["url"],
            },
        },
    },
    "a2a_call": {
        "type": "function",
        "function": {
            "name": "a2a_call",
            "description": (
                "Send a natural-language task to a remote A2A agent and BLOCK "
                "until it replies. The agent is a peer (any A2A-compliant "
                "framework), not a sub-agent you control. Pass 'context_id' "
                "from a previous reply to continue a multi-turn exchange. "
                "For long-running jobs (more than a minute or so), use "
                "a2a_submit instead so you are not parked waiting. The remote "
                "agent starts in ITS OWN default working directory, not yours; "
                "if the task involves files or repos, state the absolute path "
                "expectations explicitly in the message."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Configured peer name (from a2a_agents) or a full http(s):// URL."},
                    "message": {"type": "string", "description": "The task / message to send the peer, in natural language."},
                    "context_id": {"type": "string", "description": "Optional: context id from a prior reply, to continue the conversation."},
                },
                "required": ["agent", "message"],
            },
        },
    },
    "a2a_list": {
        "type": "function",
        "function": {
            "name": "a2a_list",
            "description": "List configured A2A peers, persisted conversations, durable tasks, and metrics. Filters are local and read-only.",
            "parameters": {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "enum": ["all", "peers", "conversations", "tasks"]},
                    "state": {"type": "string", "enum": ["all", "working", "terminal"]},
                    "direction": {"type": "string", "enum": ["all", "inbound", "outbound"]},
                    "peer": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "include_reply": {"type": "boolean"},
                },
            },
        },
    },
    "a2a_history": {
        "type": "function",
        "function": {
            "name": "a2a_history",
            "description": (
                "Recall a persisted A2A conversation transcript by context_id "
                "(survives restarts and context compaction). Use a2a_list to "
                "see known context ids."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "context_id": {"type": "string", "description": "Context id of the conversation to recall."},
                    "limit": {"type": "integer", "description": "Max messages to return (default 50, max 200)."},
                },
                "required": ["context_id"],
            },
        },
    },
    "a2a_orchestrate": {
        "type": "function",
        "function": {
            "name": "a2a_orchestrate",
            "description": (
                "Fan-out a task to multiple peer agents by capability. Peers are "
                "matched from config.yaml a2a_agents.*.capabilities. Modes: 'all' "
                "(return all replies), 'first' (first successful), 'best' (longest "
                "successful reply)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "capability": {"type": "string", "description": "Capability to match (e.g. 'research', 'code') or '*' for all peers."},
                    "message": {"type": "string", "description": "The task to send to all matching peers."},
                    "mode": {"type": "string", "enum": ["all", "first", "best"], "description": "How to aggregate results. Default: 'all'."},
                    "context_id": {"type": "string", "description": "Optional: shared context id for all peers."},
                },
                "required": ["capability", "message"],
            },
        },
    },
    "a2a_steer": {
        "type": "function",
        "function": {
            "name": "a2a_steer",
            "description": "Queue a non-blocking steering message on an existing A2A context. Returns a distinct task id to poll with a2a_get_task.",
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string"},
                    "message": {"type": "string"},
                    "context_id": {"type": "string"},
                },
                "required": ["agent", "message", "context_id"],
            },
        },
    },
    "a2a_submit": {
        "type": "function",
        "function": {
            "name": "a2a_submit",
            "description": (
                "Submit a task to a remote A2A agent WITHOUT waiting for it to "
                "finish (async, non-blocking). Returns a task_id immediately. "
                "Typical workflow for a long job: a2a_submit, do other useful "
                "work in the meantime, then a2a_await (or poll with "
                "a2a_get_task) to collect the result; a2a_cancel aborts it. "
                "Do NOT call a2a_await immediately after submitting unless you "
                "intend to park on this job — that is what a2a_call does. Use "
                "this instead of a2a_call whenever the job may take more than "
                "a minute. The remote agent starts in ITS OWN default working "
                "directory, not yours; if the task involves files or repos, "
                "state the absolute path expectations explicitly in the message."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Configured peer name (from a2a_agents) or a full http(s):// URL."},
                    "message": {"type": "string", "description": "The task / message to send the peer, in natural language."},
                    "context_id": {"type": "string", "description": "Optional: context id from a prior reply, to continue the conversation."},
                },
                "required": ["agent", "message"],
            },
        },
    },
    "a2a_get_task": {
        "type": "function",
        "function": {
            "name": "a2a_get_task",
            "description": (
                "Check the current state of a previously submitted async task "
                "(non-blocking snapshot). Returns state plus any status "
                "message or artifacts produced so far."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Configured peer name or full http(s):// URL the task was submitted to."},
                    "task_id": {"type": "string", "description": "Task id returned by a2a_submit."},
                },
                "required": ["agent", "task_id"],
            },
        },
    },
    "a2a_await": {
        "type": "function",
        "function": {
            "name": "a2a_await",
            "description": (
                "Wait for a previously submitted async task to reach a terminal "
                "state. Blocking by default (polls the peer; returns the final "
                "result, times out after 'timeout' seconds). With background=true "
                "it returns immediately with a watch_id: the watch keeps polling "
                "and the result is delivered back to this session as a new turn "
                "when the task finishes, so you can keep working instead of "
                "waiting. Background mode also accepts 'tasks' to watch several "
                "tasks at once."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Configured peer name or full http(s):// URL the task was submitted to."},
                    "task_id": {"type": "string", "description": "Task id returned by a2a_submit."},
                    "background": {"type": "boolean", "description": "Watch in the background and deliver the result to this session later (default false: block)."},
                    "tasks": {
                        "type": "array",
                        "description": "Background mode only: several tasks to watch, each {agent, task_id}.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "agent": {"type": "string"},
                                "task_id": {"type": "string"},
                            },
                            "required": ["agent", "task_id"],
                        },
                    },
                    "timeout": {"type": "integer", "description": "Blocking mode: max seconds to wait (default 600, max 3600)."},
                    "interval": {"type": "integer", "description": "Blocking mode: seconds between polls (default 5). Background polls every A2A_WATCH_INTERVAL (default 5)."},
                },
                "required": [],
            },
        },
    },
    "a2a_watch_cancel": {
        "type": "function",
        "function": {
            "name": "a2a_watch_cancel",
            "description": (
                "Stop a background a2a_await watch. This cancels local polling "
                "only: the remote task keeps running. Use a2a_cancel when you "
                "also want to ask the peer to cancel the work."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "watch_id": {"type": "string", "description": "Watch id returned by a2a_await(background=true)."},
                },
                "required": ["watch_id"],
            },
        },
    },
    "a2a_acquire_task_ownership": {
        "type": "function",
        "function": {
            "name": "a2a_acquire_task_ownership",
            "description": (
                "Take ownership of unowned A2A tasks so their results and progress notes are "
                "delivered to this session. Only unowned tasks can be adopted; a task owned by "
                "another session is refused. Awaiting a task acquires it implicitly. The queued "
                "backlog is delivered when you a2a_await the task."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Configured peer name or full http(s):// URL."},
                    "task_id": {"type": "string", "description": "Task id to acquire."},
                    "tasks": {
                        "type": "array",
                        "description": "Several tasks: [{agent, task_id}, ...].",
                        "items": {
                            "type": "object",
                            "properties": {"agent": {"type": "string"}, "task_id": {"type": "string"}},
                            "required": ["agent", "task_id"],
                        },
                    },
                },
                "required": [],
            },
        },
    },
    "a2a_release_task_ownership": {
        "type": "function",
        "function": {
            "name": "a2a_release_task_ownership",
            "description": (
                "Give up ownership of A2A tasks so another session can adopt them. This stops "
                "local polling for them but does not cancel the remote work. Use all=true to "
                "release every task this session owns. a2a_watch_cancel alone keeps ownership."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Configured peer name or full http(s):// URL."},
                    "task_id": {"type": "string", "description": "Task id to release."},
                    "tasks": {
                        "type": "array",
                        "description": "Several tasks: [{agent, task_id}, ...].",
                        "items": {
                            "type": "object",
                            "properties": {"agent": {"type": "string"}, "task_id": {"type": "string"}},
                            "required": ["agent", "task_id"],
                        },
                    },
                    "all": {"type": "boolean", "description": "Release every task this session owns."},
                },
                "required": [],
            },
        },
    },
    "a2a_cancel": {
        "type": "function",
        "function": {
            "name": "a2a_cancel",
            "description": (
                "Request cancellation of a previously submitted async task on "
                "a remote peer. Best-effort: the peer may not interrupt "
                "in-flight work."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Configured peer name or full http(s):// URL the task was submitted to."},
                    "task_id": {"type": "string", "description": "Task id returned by a2a_submit."},
                },
                "required": ["agent", "task_id"],
            },
        },
    },
}

_HANDLERS = {
    "a2a_discover": a2a_discover,
    "a2a_call": a2a_call,
    "a2a_list": a2a_list,
    "a2a_history": a2a_history,
    "a2a_orchestrate": a2a_orchestrate,
    "a2a_steer": a2a_steer,
    "a2a_submit": a2a_submit,
    "a2a_get_task": a2a_get_task,
    "a2a_await": a2a_await,
    "a2a_watch_cancel": a2a_watch_cancel,
    "a2a_cancel": a2a_cancel,
    "a2a_acquire_task_ownership": a2a_acquire_task_ownership,
    "a2a_release_task_ownership": a2a_release_task_ownership,
}


def _a2a_tools_available() -> bool:
    """check_fn for the outbound client tools: serve them ONLY when the
    operator has opted into A2A somehow — peers configured under
    ``a2a_agents`` in config.yaml, or the inbound platform enabled
    (a peer-reachable Hermes plausibly dials back).

    Maintainer-directed (#95681): these registered unconditionally, so
    every session on every install paid ~561 tok/call for tools whose
    only possible output without config is 'no peers configured'. A2A is
    unrelated to Bot Mode (bots talk over gateway RPCs) — for most
    installs this toolset is foreign-agent plumbing they never enabled.
    Config adds mid-session surface at the next compaction (#97073).
    """
    cfg = {}
    try:
        cfg = _load_config()
        if cfg.get("a2a_agents"):
            return True
    except Exception:  # noqa: BLE001
        pass
    try:
        import os as _os

        if _os.getenv("A2A_PORT"):
            return True
        platforms = cfg.get("platforms") or {}
        a2a_cfg = platforms.get("a2a") or {}
        if isinstance(a2a_cfg, dict) and a2a_cfg.get("enabled"):
            return True
    except Exception:  # noqa: BLE001
        pass
    return False


def register_tools(ctx) -> None:
    """Register all A2A client tools in the shared ``a2a`` toolset."""
    global _CTX
    _CTX = ctx
    for name in _SCHEMAS:
        function_schema = _SCHEMAS[name]["function"]
        ctx.register_tool(
            name=name,
            toolset="a2a",
            schema=function_schema,
            handler=_HANDLERS[name],
            description=function_schema["description"],
            emoji="\U0001f9e9",
            check_fn=_a2a_tools_available,
        )
    _install_session_hooks(ctx)
    _resume_watches(ctx)


def _resume_watches(ctx) -> None:
    """Resume watches persisted by a previous process, and stop polling on unload.

    Only a profile that already has a watch database is touched: plugin discovery in a fresh
    profile must not create one. The manager leases each watch before polling, so several
    processes loaded on one profile never poll the same watch concurrently.
    """
    try:
        from . import watches
        manager = _watch_manager(ctx)
        manager.ctx = ctx  # a plugin reload hands us a fresh PluginContext
        # Read-only: never creates or migrates the task database. Counts live watches AND
        # undelivered publications, because an outbox row must survive a restart too.
        pending = manager.rehydrate()
        if pending:
            logger.info("A2A: resumed %d background watch/publication row(s)", pending)
        register_unload = getattr(ctx, "on_unload", None)
        if callable(register_unload):
            register_unload(manager.stop)
    except Exception:
        logger.debug("A2A: background watch resume failed", exc_info=True)
