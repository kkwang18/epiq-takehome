import argparse
import json
import os
import signal
import sys

from content_intake.cli.lifecycle_cmd import _read_state, _write_state, _pid_alive


def add_kill_worker_parser(subparsers) -> None:
    p = subparsers.add_parser("kill-worker")
    p.add_argument("--index", type=int, default=None)


def run_kill_worker(args: argparse.Namespace) -> int:
    state = _read_state()
    if state is None:
        print("no environment is up", file=sys.stderr)
        return 1
    workers = state.get("workers", [])
    candidates = [w for w in workers if w["alive"] and _pid_alive(w["pid"])]
    if args.index is not None:
        candidates = [w for w in candidates if w["index"] == args.index]
    if not candidates:
        print("no live worker matches", file=sys.stderr)
        return 1
    target = candidates[0]
    os.kill(target["pid"], signal.SIGKILL)
    target["alive"] = False
    _write_state(state)
    print(json.dumps({"killed_index": target["index"], "pid": target["pid"]}))
    return 0
