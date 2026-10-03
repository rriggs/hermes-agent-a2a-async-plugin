"""Minimal A2A-compliant echo server using the official a2a-sdk.

Spawned as a child process by tests/interoperability/test_a2a_python_sdk.py
to exercise the plugin's outbound client. Stdlib + a2a-sdk only — no
external service, no model provider, no other framework. Bound to a
loopback port the test picks.

The server:
* serves the v1.0 Agent Card at /.well-known/agent-card.json
* accepts ``SendMessage`` (PascalCase, v1.0 §5.3/§9.4) AND the
  legacy ``message/send`` alias
* returns a synthesised Task in the same v1.0 envelope the plugin
  uses (``{ task | message }`` oneof)
* persists tasks in an InMemoryTaskStore so the test can also
  exercise ListTasks / GetTask / CancelTask

Stdout line 1 = READY <port>. The parent waits on a 0.5s poll loop
before issuing requests, so the read is purely advisory.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import socket
import sys
import time

import uvicorn
from starlette.applications import Starlette

from a2a.helpers import (
    get_message_text,
    new_task_from_user_message,
    new_text_message,
    new_text_part,
)
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import (
    AgentCard,
    AgentInterface,
    AgentSkill,
    TaskState,
)


class _EchoExecutor(AgentExecutor):
    """Echoes the user's text back as the agent's reply. Tasks are
    state-tracked through ``TaskUpdater`` so the plugin client can
    observe the same transitions it produces itself."""

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        if context.current_task:
            task = context.current_task
        else:
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(
            event_queue=event_queue, task_id=task.id, context_id=task.context_id,
        )
        await updater.update_status(
            state=TaskState.TASK_STATE_WORKING,
            message=new_text_message("Echoing..."),
        )
        query = get_message_text(context.message) or ""
        reply_text = f"echo: {query}"
        await updater.add_artifact(
            parts=[new_text_part(text=reply_text, media_type="text/plain")],
        )
        await updater.update_status(
            state=TaskState.TASK_STATE_COMPLETED,
            message=new_text_message("Done."),
        )

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        updater = TaskUpdater(
            event_queue=event_queue, task_id=context.task_id,
            context_id=context.context_id,
        )
        await updater.cancel()


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _run(port: int) -> int:
    card = AgentCard(
        name="sdk-echo",
        description="Phase 5 / Task 12 interoperability echo (a2a-sdk).",
        version="1.0.0",
        capabilities={},
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[AgentSkill(
            id="echo", name="echo",
            description="Echoes the user text back as the agent reply.",
            tags=["interoperability", "test"],
        )],
        supported_interfaces=[AgentInterface(
            url=f"http://127.0.0.1:{port}/",
            protocol_binding="JSONRPC",
            protocol_version="1.0",
        )],
    )
    handler = DefaultRequestHandler(
        agent_executor=_EchoExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=card,
    )
    app = Starlette(routes=[
        *create_agent_card_routes(agent_card=card),
        *create_jsonrpc_routes(request_handler=handler, rpc_url="/"),
    ])
    config = uvicorn.Config(
        app, host="127.0.0.1", port=port,
        log_level="warning", access_log=False,
    )
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve())
    # Print the ready line as soon as the server binds. uvicorn fires
    # 'started' before the socket accepts; we additionally poll the
    # loopback port for a connect-able accept() to avoid the parent
    # issuing the first request before the server is ready.
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            await asyncio.sleep(0.05)
    sys.stdout.write(f"READY {port}\n")
    sys.stdout.flush()
    try:
        # The parent never sends us a signal — we wait until uvicorn
        # shuts itself down (it doesn't, by default) or until a sentinel
        # marker file appears. We use a simple stop-event driven by
        # SIGTERM for cleanliness; the test process SIGKILLs us when
        # done.
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig_name in ("SIGTERM", "SIGINT"):
            sig = getattr(__import__("signal"), sig_name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
        await stop.wait()
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(serve_task, timeout=5.0)
        except asyncio.TimeoutError:
            serve_task.cancel()
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    args = p.parse_args()
    try:
        return asyncio.run(_run(args.port))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
