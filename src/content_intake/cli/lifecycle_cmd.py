# src/content_intake/cli/lifecycle_cmd.py
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg

from content_intake.common import config
from content_intake.pipeline.db import connect, apply_schema, ensure_slots

REPO_ROOT = Path(__file__).resolve().parents[3]
STATE_FILE = REPO_ROOT / ".intake_state.json"
LOG_DIR = REPO_ROOT / ".intake_logs"


def _read_state() -> dict | None:
    if not STATE_FILE.exists():
        return None
    return json.loads(STATE_FILE.read_text())


def _write_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _wait_for(check, timeout=30.0, interval=0.5):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if check():
            return True
        time.sleep(interval)
    return False


def _connect_with_retry(timeout: float = 10.0, interval: float = 0.5):
    # A fresh Postgres volume (every `up` after a `down`, since `down` removes the
    # volume) goes through initdb -> shutdown -> restart-to-apply-config on the
    # official image. `pg_isready` can report ready in the brief window between the
    # first startup and that restart, so a bare connect() right after pg_isready
    # can still hit "server closed the connection unexpectedly". Retry the actual
    # connection, not just the readiness probe.
    start = time.monotonic()
    last_error: Exception | None = None
    while time.monotonic() - start < timeout:
        try:
            return connect()
        except psycopg.OperationalError as e:
            last_error = e
            time.sleep(interval)
    raise last_error


def add_up_parser(subparsers) -> None:
    p = subparsers.add_parser("up")
    p.add_argument("--workers", type=int, default=4)


def run_up(args: argparse.Namespace) -> int:
    state = _read_state()
    if state is not None and _pid_alive(state.get("stub_pid", -1)) and _pid_alive(state.get("api_pid", -1)):
        print(json.dumps({"status": "already_running"}))
        return 0

    subprocess.run(["docker", "compose", "up", "-d", "postgres"], cwd=REPO_ROOT, check=True)
    ok = _wait_for(lambda: subprocess.run(
        ["docker", "exec", "intake-postgres", "pg_isready", "-U", "intake"],
        capture_output=True,
    ).returncode == 0)
    if not ok:
        print("Postgres did not become ready in time", file=sys.stderr)
        return 1

    conn = _connect_with_retry()
    apply_schema(conn)
    ensure_slots(conn, config.IN_FLIGHT_CAPACITY)
    conn.close()

    env = {**os.environ, "STUB_IN_FLIGHT_CAPACITY": str(config.IN_FLIGHT_CAPACITY)}
    LOG_DIR.mkdir(exist_ok=True)

    # These processes outlive this `up` invocation (they're detached and left running).
    # They must NOT inherit this process's stdout/stderr: when `up` itself is invoked
    # through subprocess.run(..., capture_output=True) (e.g. by the acceptance test's
    # CLI wrapper), the pipe fds created for that capture would be inherited here, and
    # the long-lived children would hold the write end open forever. That leaves the
    # calling process's communicate()/wait() blocked reading for EOF that never comes,
    # even after this `up` process has itself exited -- a real deadlock, not just a
    # missing debug log. Redirect each to its own log file instead.
    stub_log = open(LOG_DIR / "stub.log", "w")
    stub_proc = subprocess.Popen(
        [sys.executable, "-m", "content_intake.stub.run"],
        cwd=REPO_ROOT, env={**env, "PYTHONPATH": str(REPO_ROOT / "src")},
        stdout=stub_log, stderr=subprocess.STDOUT,
    )
    if not _wait_for(lambda: _http_ok(f"{config.STUB_BASE_URL}/healthz")):
        print("Stub did not become healthy in time", file=sys.stderr)
        return 1

    api_log = open(LOG_DIR / "api.log", "w")
    api_proc = subprocess.Popen(
        [sys.executable, "-m", "content_intake.pipeline.run_api"],
        cwd=REPO_ROOT, env={**env, "PYTHONPATH": str(REPO_ROOT / "src")},
        stdout=api_log, stderr=subprocess.STDOUT,
    )
    if not _wait_for(lambda: _http_ok(f"{config.API_BASE_URL}/v1/runs/00000000-0000-0000-0000-000000000000/status", accept_404=True)):
        print("Pipeline API did not become healthy in time", file=sys.stderr)
        return 1

    workers = []
    for i in range(args.workers):
        worker_log = open(LOG_DIR / f"worker-{i}.log", "w")
        proc = subprocess.Popen(
            [sys.executable, "-m", "content_intake.pipeline.worker", str(i)],
            cwd=REPO_ROOT, env={**env, "PYTHONPATH": str(REPO_ROOT / "src")},
            stdout=worker_log, stderr=subprocess.STDOUT,
        )
        workers.append({"index": i, "pid": proc.pid, "alive": True})

    _write_state({
        "postgres_container": "intake-postgres",
        "stub_pid": stub_proc.pid,
        "api_pid": api_proc.pid,
        "workers": workers,
    })
    print(json.dumps({"status": "up", "workers": len(workers)}))
    return 0


def _http_ok(url: str, accept_404: bool = False) -> bool:
    try:
        resp = httpx.get(url, timeout=1.0)
        return resp.status_code == 200 or (accept_404 and resp.status_code == 404)
    except httpx.HTTPError:
        return False


def add_down_parser(subparsers) -> None:
    subparsers.add_parser("down")


def run_down(args: argparse.Namespace) -> int:
    state = _read_state()
    if state is None:
        print(json.dumps({"status": "already_down"}))
        return 0

    pids = [state.get("stub_pid"), state.get("api_pid")] + [w["pid"] for w in state.get("workers", [])]
    for pid in pids:
        if pid and _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
    time.sleep(1)
    for pid in pids:
        if pid and _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass

    subprocess.run(["docker", "rm", "-f", "-v", "intake-postgres"], cwd=REPO_ROOT, capture_output=True)
    STATE_FILE.unlink(missing_ok=True)
    print(json.dumps({"status": "down"}))
    return 0


def add_reset_parser(subparsers) -> None:
    p = subparsers.add_parser("reset")
    p.add_argument("--workers", type=int, default=4)


def run_reset(args: argparse.Namespace) -> int:
    down_rc = run_down(argparse.Namespace())
    if down_rc != 0:
        return down_rc
    return run_up(argparse.Namespace(workers=args.workers))
