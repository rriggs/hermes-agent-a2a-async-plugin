"""Operator commands over A2A task ownership: list, release, transfer.

The shell command (``hermes a2a-tasks ...``) and the in-session command (``/a2a tasks ...``) are
thin adapters over these functions, so the two surfaces cannot drift. Nothing here is an agent
tool: releasing another session's ownership is an operator action, and it must work without a live
gateway or a bound session.
"""

from __future__ import annotations

import argparse
import logging
import os
import shlex
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from typing import Iterable, Optional

from . import ownership, protocol

EXIT_OK = 0
EXIT_NOTHING = 1
EXIT_USAGE = 2
EXIT_CONFLICT = 3
# The ownership transaction itself failed and rolled back; nothing changed. Kept distinct from a
# conflict so the operator knows to retry rather than investigate ownership.
EXIT_ERROR = 5

logger = logging.getLogger(__name__)

USAGE = (
    "usage: a2a-tasks list   [--session <id>] [--unowned] [--task <id> ...]\n"
    "       a2a-tasks release --session <id> (--all | --task <id> ...) [--yes]\n"
    "       a2a-tasks transfer --from <session> --to <session> [--task <id> ...] [--yes]"
)


@dataclass
class Result:
    """What one command did, in a form both adapters can print and exit on."""

    action: str
    exit_code: int
    text: str
    rows: list[dict] = field(default_factory=list)


class _Parser(argparse.ArgumentParser):
    """argparse exits on bad input; both adapters need to report it as text instead."""

    def error(self, message: str):  # noqa: D401 - argparse contract
        raise ValueError(message)


def _db_path(db_path: Optional[str] = None) -> str:
    return db_path or protocol.TaskStore._resolve_db_path(None)


def _read_only_conn(db_path: Optional[str]) -> Optional[sqlite3.Connection]:
    """Read the store without creating or migrating it (listing must not have side effects)."""
    path = _db_path(db_path)
    if not path or not os.path.exists(path):
        return None
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    return con


def _short_state(state: str) -> str:
    return str(state or "").replace("TASK_STATE_", "").lower() or "-"


def _age(timestamp: float) -> str:
    seconds = max(0, int(time.time() - float(timestamp or 0)))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def _pending_events(con: sqlite3.Connection) -> dict[str, set[str]]:
    """Undelivered outbox rows -> the task ids each one names (one event can name several)."""
    events: dict[str, set[str]] = {}
    try:
        rows = con.execute(
            "SELECT event_id, metadata_json FROM a2a_watch_publications"
            " WHERE published_at IS NULL"
        ).fetchall()
    except sqlite3.Error:
        return events
    for row in rows:
        events[str(row["event_id"])] = ownership.publication_task_ids(row["metadata_json"])
    return events


def _pending_counts(con: sqlite3.Connection) -> dict[str, int]:
    """Undelivered outbox rows per task, parsed in Python (metadata JSON is not queryable)."""
    counts: dict[str, int] = {}
    for task_ids in _pending_events(con).values():
        for task_id in task_ids:
            counts[task_id] = counts.get(task_id, 0) + 1
    return counts


def _rows_for(
    con: sqlite3.Connection,
    *,
    session: str = "",
    unowned: bool = False,
    task_ids: Iterable[str] = (),
) -> list[dict]:
    ids = [str(t) for t in task_ids if t]
    where = []
    params: list[object] = []
    if session:
        where.append("t.owner_session_id=?")
        params.append(session)
    if unowned:
        where.append("t.owner_session_id=''")
    if ids:
        where.append("t.task_id IN (%s)" % ",".join("?" for _ in ids))
        params.extend(ids)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    rows = con.execute(
        f"""SELECT t.task_id, t.peer, t.state, t.remote_state, t.owner_session_id,
                   t.owned_at, t.created_at,
                   (SELECT COUNT(*) FROM a2a_task_messages m
                     WHERE m.task_id=t.task_id AND m.delivered_at IS NULL) AS queued
              FROM a2a_tasks t{clause}
             ORDER BY t.created_at, t.task_id""",
        params,
    ).fetchall()
    pending = _pending_counts(con)
    out: list[dict] = []
    for row in rows:
        out.append({
            "task_id": str(row["task_id"]),
            "peer": str(row["peer"] or ""),
            "state": _short_state(row["remote_state"] or row["state"]),
            "owner": str(row["owner_session_id"] or ""),
            "queued": int(row["queued"] or 0),
            "pending": int(pending.get(str(row["task_id"]), 0)),
            "age": _age(row["owned_at"] or row["created_at"]),
        })
    return out


def _table(rows: list[dict]) -> str:
    header = (f"{'TASK':<30} {'PEER':<8} {'STATE':<10} {'OWNER':<34} "
              f"{'QUEUED':>6} {'PENDING':>7} {'AGE':>5}")
    lines = [header]
    for row in rows:
        lines.append(
            f"{row['task_id']:<30} {row['peer'][:8]:<8} {row['state'][:10]:<10} "
            f"{(row['owner'] or '-')[:34]:<34} {row['queued']:>6} {row['pending']:>7} "
            f"{row['age']:>5}"
        )
    return "\n".join(lines)


def _rows_for_ids(db_path: Optional[str], task_ids: Iterable[str]) -> list[dict]:
    con = _read_only_conn(db_path)
    if con is None:
        return []
    try:
        return _rows_for(con, task_ids=task_ids)
    except sqlite3.Error:
        return []
    finally:
        con.close()


def _owner_map(store: ownership.OwnershipStore, task_ids: Iterable[str]) -> dict[str, str]:
    ids = [str(t) for t in task_ids if t]
    try:
        return store.owners(ids)
    except Exception:
        return {}


def list_tasks(*, db_path: Optional[str] = None, session: str = "", unowned: bool = False,
               task_ids: Iterable[str] = ()) -> Result:
    """List tasks, optionally filtered by owner, unowned state, or exact ids."""
    if session and unowned:
        return Result("list", EXIT_USAGE, "Error: --session and --unowned are mutually exclusive.")
    con = _read_only_conn(db_path)
    if con is None:
        return Result("list", EXIT_NOTHING, "No A2A task store for this profile.")
    try:
        try:
            rows = _rows_for(con, session=session, unowned=unowned, task_ids=task_ids)
        except sqlite3.Error as exc:
            return Result("list", EXIT_USAGE, f"Error: cannot read the task store: {exc}")
    finally:
        con.close()
    if not rows:
        return Result("list", EXIT_NOTHING, "No matching A2A tasks.")
    return Result("list", EXIT_OK, _table(rows) + f"\n\n{len(rows)} task(s).", rows)


def release_tasks(*, db_path: Optional[str] = None, session: str = "",
                  task_ids: Iterable[str] = (), all_tasks: bool = False) -> Result:
    """Release one session's tasks; the tasks become adoptable and their watches stop."""
    if not session:
        return Result("release", EXIT_USAGE, "Error: --session is required.")
    store = ownership.OwnershipStore(db_path)
    requested = [str(t) for t in task_ids if t]
    if not all_tasks and not requested:
        return Result("release", EXIT_USAGE, "Error: pass --all or at least one --task.")
    # Count the deliveries release is about to drop: it removes them in the same transaction.
    # --all is selected under the release lock, not from an earlier snapshot; cleanup runs in
    # that same transaction, so a task reacquired afterwards keeps its new watch and delivery.
    try:
        released, refused, stopped, dropped_events = store.release_many(
            None if all_tasks else requested, session)
    except Exception as exc:
        logger.exception("A2A task admin: release failed for %s", session)
        return Result("release", EXIT_ERROR,
                      f"Nothing released: the ownership transaction failed and rolled back: {exc}")
    if refused:
        owners = _owner_map(store, refused)
        detail = ", ".join(f"{task_id} (owner: {owners.get(task_id) or 'unowned'})"
                           for task_id in refused)
        return Result("release", EXIT_CONFLICT,
                      f"Nothing released: not owned by {session}: {detail}.")
    if not released:
        return Result("release", EXIT_NOTHING, f"No tasks are owned by {session}.")
    # The deleted-outbox map is captured inside the transaction; nothing can arrive after it.
    dropped = len(dropped_events)
    lines = [f"Released {len(released)} task(s) owned by {session}; stopped "
             f"{stopped} watch(es); dropped {dropped} pending deliver(ies)."]
    rows = _rows_for_ids(db_path, released)
    by_id = {row["task_id"]: row for row in rows}
    for task_id in released:
        row = by_id.get(task_id, {})
        participation = sum(1 for named in dropped_events.values() if task_id in named)
        lines.append(f"  {task_id}  {row.get('peer', '-')}  {row.get('state', '-')}  "
                     f"{participation} pending delivery(ies) dropped")
        logger.info("A2A task admin: released task %s from %s", task_id, session)
    logger.info("A2A task admin: released %d task(s) from %s (watches=%d, dropped=%d)",
                len(released), session, stopped, dropped)
    return Result("release", EXIT_OK, "\n".join(lines), rows)


def transfer_tasks(*, db_path: Optional[str] = None, from_session: str = "", to_session: str = "",
                   task_ids: Iterable[str] = ()) -> Result:
    """Move ownership from one session to another; the new owner's backlog is untouched."""
    if not from_session or not to_session:
        return Result("transfer", EXIT_USAGE, "Error: --from and --to are required.")
    if from_session == to_session:
        return Result("transfer", EXIT_USAGE, "Error: --from and --to must differ.")
    store = ownership.OwnershipStore(db_path)
    requested = [str(t) for t in task_ids if t]
    ids = requested or store.owned_ids(from_session)
    if not ids:
        return Result("transfer", EXIT_NOTHING, f"No tasks are owned by {from_session}.")
    try:
        transferred, refused, stopped, dropped_events = store.transfer_many(
            ids, from_session, to_session)
    except Exception as exc:
        logger.exception("A2A task admin: transfer failed for %s", from_session)
        return Result("transfer", EXIT_ERROR,
                      f"Nothing transferred: the ownership transaction failed and rolled back: {exc}")
    if refused:
        owners = _owner_map(store, refused)
        detail = ", ".join(f"{task_id} (owner: {owners.get(task_id) or 'unowned'})"
                           for task_id in refused)
        return Result("transfer", EXIT_CONFLICT,
                      f"Nothing transferred: not owned by {from_session}: {detail}.")
    dropped = len(dropped_events)
    lines = [f"Transferred {len(transferred)} task(s) from {from_session} to {to_session}; "
             f"stopped {stopped} watch(es); dropped {dropped} pending deliver(ies). "
             f"The new owner's backlog is untouched."]
    rows = _rows_for_ids(db_path, transferred)
    for row in rows:
        lines.append(f"  {row['task_id']}  {row['peer']}  {row['state']}  -> {to_session}")
        logger.info("A2A task admin: transferred task %s from %s to %s",
                    row["task_id"], from_session, to_session)
    logger.info("A2A task admin: transferred %d task(s) from %s to %s (watches=%d, dropped=%d)",
                len(transferred), from_session, to_session, stopped, dropped)
    return Result("transfer", EXIT_OK, "\n".join(lines), rows)


def _add_subcommands(parser: argparse.ArgumentParser, *, cli: bool) -> None:
    """One definition of the command grammar, shared by the shell and slash adapters."""
    subs = parser.add_subparsers(dest="command")
    add_help = bool(cli)
    p_list = subs.add_parser("list", add_help=add_help,
                             help="List tasks and their owners." if cli else None)
    p_list.add_argument("--session", default="",
                        help="Only tasks owned by this session." if cli else None)
    p_list.add_argument("--unowned", action="store_true",
                        help="Only adoptable (unowned) tasks." if cli else None)
    p_list.add_argument("--task", action="append", default=[],
                        help="Exact task id (repeatable)." if cli else None)
    p_release = subs.add_parser("release", add_help=add_help,
                                help="Release a session's tasks." if cli else None)
    p_release.add_argument("--session", default="", required=cli,
                           help="Session whose tasks to release." if cli else None)
    group = p_release.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true",
                       help="Every task the session owns." if cli else None)
    group.add_argument("--task", action="append", default=[],
                       help="Exact task id (repeatable)." if cli else None)
    p_release.add_argument("--yes", action="store_true", help="Do not prompt." if cli else None)
    p_transfer = subs.add_parser("transfer", add_help=add_help,
                                 help="Move ownership between sessions." if cli else None)
    p_transfer.add_argument("--from", dest="from_session", required=True,
                            help="Current owner." if cli else None)
    p_transfer.add_argument("--to", dest="to_session", required=True,
                            help="New owner." if cli else None)
    p_transfer.add_argument("--task", action="append", default=[],
                            help="Exact task id (repeatable); default: all from --from."
                            if cli else None)
    p_transfer.add_argument("--yes", action="store_true", help="Do not prompt." if cli else None)


def _build_parser(prog: str) -> argparse.ArgumentParser:
    parser = _Parser(prog=prog, add_help=False)
    _add_subcommands(parser, cli=False)
    return parser


def _parse(tokens: list[str], prog: str) -> tuple[dict, Optional[str]]:
    """Parse command tokens; returns ``(values, error)`` instead of exiting."""
    parser = _build_parser(prog)
    try:
        args = parser.parse_args(tokens)
    except ValueError as exc:
        return {}, str(exc)
    if not getattr(args, "command", None):
        return {}, "a subcommand is required"
    return vars(args), None


def _confirm_or_refuse(yes: bool, prompt: str) -> tuple[bool, Optional[str]]:
    if yes:
        return True, None
    if sys.stdin.isatty():
        try:
            return input(prompt + " [y/N] ").strip().lower() in {"y", "yes"}, None
        except (EOFError, KeyboardInterrupt):
            return False, "cancelled"
    return False, "refusing without --yes"


def dispatch_cli(argv: list[str]) -> int:
    """`hermes a2a-tasks ...` entry point (tokens, for tests and direct calls)."""
    values, error = _parse(argv, "a2a-tasks")
    if error:
        print(USAGE, file=sys.stderr)
        print(f"error: {error}", file=sys.stderr)
        return EXIT_USAGE
    return _run(values)


def dispatch_namespace(args: argparse.Namespace) -> int:
    """argparse ``func`` entry point used by the real CLI."""
    return _run(vars(args))


def _run(values: dict) -> int:
    action = values["command"]
    if action == "list":
        result = list_tasks(session=values.get("session") or "",
                            unowned=values.get("unowned", False),
                            task_ids=values.get("task") or [])
    elif action == "release":
        if not values.get("session"):
            print(USAGE, file=sys.stderr)
            print("error: --session is required", file=sys.stderr)
            return EXIT_USAGE
        confirmed, refusal = _confirm_or_refuse(
            values.get("yes", False),
            f"Release tasks owned by {values.get('session')}?")
        if not confirmed:
            if refusal == "refusing without --yes":
                print("error: refusing without --yes", file=sys.stderr)
                return EXIT_USAGE
            print("cancelled")
            return EXIT_OK
        result = release_tasks(session=values.get("session") or "",
                               task_ids=values.get("task") or [],
                               all_tasks=values.get("all", False))
    elif action == "transfer":
        confirmed, refusal = _confirm_or_refuse(
            values.get("yes", False),
            f"Transfer tasks from {values.get('from_session')} to {values.get('to_session')}?")
        if not confirmed:
            if refusal == "refusing without --yes":
                print("error: refusing without --yes", file=sys.stderr)
                return EXIT_USAGE
            print("cancelled")
            return EXIT_OK
        result = transfer_tasks(from_session=values.get("from_session") or "",
                                to_session=values.get("to_session") or "",
                                task_ids=values.get("task") or [])
    else:  # pragma: no cover - argparse rejects unknown commands
        print(USAGE, file=sys.stderr)
        return EXIT_USAGE
    print(result.text)
    return result.exit_code


def slash_command(raw_args: str, session_id: str = "", **_: object) -> str:
    """`/a2a tasks ...` entry point. The bound session is the default target."""
    try:
        tokens = shlex.split(raw_args or "")
    except ValueError as exc:
        return f"{USAGE}\nerror: {exc}"
    if tokens[:1] != ["tasks"]:
        return USAGE
    values, error = _parse(tokens[1:], "/a2a tasks")
    if error:
        return f"{USAGE}\nerror: {error}"
    action = values["command"]
    current = str(session_id or "")
    if action == "list":
        # --unowned is its own filter; it must not inherit the bound session and collide with it.
        session = values.get("session") or ("" if values.get("unowned") else current)
        return list_tasks(session=session,
                          unowned=values.get("unowned", False),
                          task_ids=values.get("task") or []).text
    if action == "release":
        if not values.get("yes"):
            return "Refusing to release without --yes."
        return release_tasks(session=values.get("session") or current,
                             task_ids=values.get("task") or [],
                             all_tasks=values.get("all", False)).text
    if action == "transfer":
        if not values.get("yes"):
            return "Refusing to transfer without --yes."
        return transfer_tasks(from_session=values.get("from_session") or "",
                              to_session=values.get("to_session") or "",
                              task_ids=values.get("task") or []).text
    return USAGE


def register_cli(parser: argparse.ArgumentParser) -> None:
    """Wire `hermes a2a-tasks ...` into the top-level CLI parser."""
    parser.description = "Inspect, release, or transfer A2A task ownership."
    _add_subcommands(parser, cli=True)
    parser.set_defaults(func=dispatch_namespace)
