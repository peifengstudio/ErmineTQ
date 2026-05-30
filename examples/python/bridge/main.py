"""
ErmineTQ Python Bridge server.

Lifecycle:
  1. Removes any stale socket file.
  2. Registers this process as a worker via POST /api/workers/register.
  3. Starts listening on the Unix socket.
  4. For each connection from Go: reads newline-delimited JSON task requests,
     dispatches to the registered handler, writes newline-delimited JSON responses.

Run:
    cd examples/python
    uv run python bridge/main.py

    # Custom server / socket:
    ERMINETQ_URL=http://localhost:8081 \
    BRIDGE_SOCKET=/tmp/erminetq_bridge.sock \
    uv run python bridge/main.py
"""
from __future__ import annotations

import json
import logging
import os
import signal
import socket
import sys
import threading
from pathlib import Path

import httpx

# ── configuration ──────────────────────────────────────────────────────────────

ERMINETQ_URL = os.getenv("ERMINETQ_URL", "http://localhost:8081")
SOCKET_PATH  = os.getenv("BRIDGE_SOCKET", "/tmp/erminetq_bridge.sock")

# Make sure handlers package is importable when running from examples/python/
sys.path.insert(0, str(Path(__file__).parent.parent))
from handlers import HANDLERS  # noqa: E402

# ── logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bridge")


# ── registration ───────────────────────────────────────────────────────────────

def register() -> dict:
    """Register this Bridge as a Python worker with ErmineTQ."""
    payload = {
        "type":        "python",
        "task_types":  list(HANDLERS.keys()),
        "queue":       "default",
        "concurrency": 1,          # one call in-flight at a time (v0.1)
        "socket":      SOCKET_PATH,
    }
    resp = httpx.post(f"{ERMINETQ_URL}/api/workers/register", json=payload, timeout=10)
    resp.raise_for_status()
    worker = resp.json()
    logger.info("registered worker id=%s task_types=%s", worker.get("ID"), list(HANDLERS.keys()))
    return worker


# ── connection handler ─────────────────────────────────────────────────────────

def handle_connection(conn: socket.socket) -> None:
    """
    Handle one persistent connection from the Go Bridge client.
    Reads newline-terminated JSON requests, writes newline-terminated JSON responses.
    One request / response per exchange; the connection is reused across calls.
    """
    peer = conn.getpeername() if hasattr(conn, 'getpeername') else "go"
    logger.debug("connection from %s", peer)

    with conn, conn.makefile("rb") as reader:
        for raw_line in reader:
            raw_line = raw_line.strip()
            if not raw_line:
                continue

            try:
                req = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                logger.error("JSON decode error: %s", exc)
                _send(conn, {"error": f"invalid JSON: {exc}"})
                continue

            # Cancellation signal — best-effort, we can't interrupt a running handler.
            if req.get("type") == "cancel":
                logger.info("cancel signal for task_id=%s (best-effort)", req.get("task_id"))
                continue

            task_id   = req.get("task_id", "?")
            task_type = req.get("type", "?")
            payload   = req.get("payload") or {}

            logger.info("← task id=%s type=%s", task_id, task_type)

            handler = HANDLERS.get(task_type)
            if handler is None:
                resp = {"error": f"no handler for task type: {task_type!r}"}
            else:
                try:
                    result = handler(task_id, payload)
                    resp   = {"result": result}
                    logger.info("→ ok  id=%s", task_id)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("handler error id=%s type=%s", task_id, task_type)
                    resp = {"error": str(exc)}

            _send(conn, resp)


def _send(conn: socket.socket, obj: dict) -> None:
    """Write a single newline-terminated JSON object to the socket."""
    conn.sendall(json.dumps(obj).encode() + b"\n")


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # Remove stale socket from a previous run.
    Path(SOCKET_PATH).unlink(missing_ok=True)

    # Register with ErmineTQ (must be running first).
    try:
        register()
    except Exception as exc:
        logger.error(
            "Registration failed: %s\n"
            "Is ErmineTQ running at %s?\n"
            "Start it with: go run ./examples/go/server",
            exc, ERMINETQ_URL,
        )
        sys.exit(1)

    # Unix socket server.
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(SOCKET_PATH)
    srv.listen(10)
    logger.info("bridge listening  socket=%s", SOCKET_PATH)
    logger.info("press Ctrl+C to stop")

    def _shutdown(sig, frame):  # noqa: ANN001, ARG001
        logger.info("shutting down bridge...")
        srv.close()
        Path(SOCKET_PATH).unlink(missing_ok=True)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        try:
            conn, _ = srv.accept()
        except OSError:
            break  # socket closed by signal handler
        threading.Thread(
            target=handle_connection,
            args=(conn,),
            daemon=True,
            name=f"bridge-conn",
        ).start()


if __name__ == "__main__":
    main()
