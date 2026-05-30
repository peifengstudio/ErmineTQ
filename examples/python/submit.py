"""
Submit Python Bridge example tasks to ErmineTQ and wait for results.

Prerequisites:
  1. go run ./examples/go/server          (Go server on :8081)
  2. cd examples/python && uv run python bridge/main.py  (Python Bridge)

Usage:
  cd examples/python
  uv run python submit.py
  uv run python submit.py --addr http://localhost:8081
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

# ── configuration ──────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(__file__).parent.parent / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("submit")

# ── task definitions ───────────────────────────────────────────────────────────

def build_tasks() -> list[dict]:
    """Return the list of tasks to submit."""
    return [
        {
            "type":    "py_http_fetch",
            "payload": {"url": "https://httpbin.org/json", "timeout_secs": 10},
        },
        {
            "type":    "py_file_write",
            "payload": {
                "filename": "py_example_write.txt",
                "content":  f"Written by Python Bridge at {datetime.now().isoformat()}\n",
            },
        },
        {
            "type":    "py_file_read",
            "payload": {"path": "examples/output/py_example_write.txt"},
        },
        {
            "type":    "py_ollama_chat",
            "payload": {
                "model":  "qwen2.5:3b-instruct",
                "prompt": "What's the weather like in Shanghai today? (Answer briefly)",
            },
        },
    ]


# ── API helpers ────────────────────────────────────────────────────────────────

def submit_task(client: httpx.Client, task_type: str, payload: dict) -> str:
    resp = client.post("/api/tasks", json={"type": task_type, "payload": payload})
    resp.raise_for_status()
    task = resp.json()
    return task["ID"]


def get_task(client: httpx.Client, task_id: str) -> dict:
    resp = client.get(f"/api/tasks/{task_id}")
    resp.raise_for_status()
    return resp.json()


TERMINAL = {"succeeded", "dead", "cancelled"}


def wait_for_tasks(
    client: httpx.Client,
    task_ids: list[str],
    timeout_secs: int = 180,
) -> dict[str, dict]:
    """Poll until all task IDs are in a terminal state. Returns id → task."""
    results: dict[str, dict] = {}
    deadline = time.monotonic() + timeout_secs

    while len(results) < len(task_ids):
        if time.monotonic() > deadline:
            pending = [tid for tid in task_ids if tid not in results]
            raise TimeoutError(f"Tasks still pending after {timeout_secs}s: {pending}")

        time.sleep(1)
        for tid in task_ids:
            if tid in results:
                continue
            try:
                task = get_task(client, tid)
            except Exception as exc:
                logger.warning("poll error id=%s: %s", tid, exc)
                continue
            if task.get("Status", "").lower() in TERMINAL:
                results[tid] = task

    return results


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--addr", default="http://localhost:8081",
                        help="ErmineTQ server address (default: http://localhost:8081)")
    args = parser.parse_args()

    tasks = build_tasks()
    logger.info("connecting to %s", args.addr)

    with httpx.Client(base_url=args.addr, timeout=15) as client:
        # Submit all tasks.
        submitted: list[tuple[str, str]] = []  # (task_id, task_type)
        for t in tasks:
            try:
                tid = submit_task(client, t["type"], t["payload"])
                logger.info("submitted  type=%-20s id=%s", t["type"], tid)
                submitted.append((tid, t["type"]))
            except Exception as exc:
                logger.error("failed to submit %s: %s", t["type"], exc)

        if not submitted:
            logger.error("no tasks submitted — is the bridge running?")
            sys.exit(1)

        # Wait for completion.
        logger.info("waiting for %d tasks…", len(submitted))
        task_ids = [tid for tid, _ in submitted]
        id_to_type = {tid: tt for tid, tt in submitted}

        try:
            results = wait_for_tasks(client, task_ids)
        except TimeoutError as exc:
            logger.error("%s", exc)
            sys.exit(1)

    # Print summary.
    print()
    print("═" * 55)
    print("  Python Bridge example complete")
    print("═" * 55)
    for tid, tt in submitted:
        task = results.get(tid, {})
        status = task.get("Status", "unknown")
        print(f"  {tt:<24}  {status}")
    print()
    print(f"  Output files → {OUTPUT_DIR}/")
    print("═" * 55)

    # Write JSON summary.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = OUTPUT_DIR / "py_summary.json"
    summary = {
        tid: {
            "type":   id_to_type[tid],
            "status": results[tid].get("Status"),
        }
        for tid in task_ids
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("summary → %s", summary_path)


if __name__ == "__main__":
    main()
