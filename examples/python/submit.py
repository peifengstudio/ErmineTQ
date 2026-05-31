"""
Submit example Python worker tasks to ErmineTQ and wait for results.

Prerequisites:
  Terminal 1: make dev
  Terminal 2: cd examples/python && uv run python worker.py

Usage:
  cd examples/python
  uv run python submit.py
  uv run python submit.py --addr http://localhost:8080
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from erminetq import Client

OUTPUT_DIR = Path(__file__).parent.parent / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("submit")

TERMINAL = {"succeeded", "dead", "cancelled"}


def build_tasks() -> list[dict]:
    return [
        {
            "type": "py_http_fetch",
            "payload": {"url": "https://httpbin.org/json", "timeout_secs": 10},
        },
        {
            "type": "py_file_write",
            "payload": {
                "filename": "py_example_write.txt",
                "content": f"Written by ErmineTQ Python SDK at {datetime.now().isoformat()}\n",
            },
        },
        {
            "type": "py_file_read",
            "payload": {"path": "examples/output/py_example_write.txt"},
        },
        {
            "type": "py_ollama_chat",
            "payload": {
                "model": "qwen2.5:3b-instruct",
                "prompt": "What's the weather like in Shanghai today? (Answer briefly)",
            },
        },
    ]


def wait_for_tasks(
    client: Client,
    task_ids: list[str],
    timeout_secs: int = 180,
) -> dict[str, dict]:
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
                detail = client.get_task(tid)
                status = (detail.get("task") or detail).get("Status", "").lower()
                if status in TERMINAL:
                    results[tid] = detail
            except Exception as exc:
                logger.warning("poll error id=%s: %s", tid, exc)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--addr",
        default="http://localhost:8080",
        help="ErmineTQ server address (default: http://localhost:8080)",
    )
    args = parser.parse_args()

    logger.info("connecting to %s", args.addr)

    with Client(args.addr) as client:
        # Submit all tasks.
        submitted: list[tuple[str, str]] = []  # (task_id, task_type)
        for t in build_tasks():
            try:
                tid = client.submit(t["type"], t["payload"])
                logger.info("submitted  type=%-20s id=%s", t["type"], tid)
                submitted.append((tid, t["type"]))
            except Exception as exc:
                logger.error("failed to submit %s: %s", t["type"], exc)

        if not submitted:
            logger.error("no tasks submitted — is the server running?")
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
    print("  ErmineTQ Python SDK example complete")
    print("═" * 55)
    for tid, tt in submitted:
        task_detail = results.get(tid, {})
        task = task_detail.get("task") or task_detail
        status = task.get("Status", "unknown")
        print(f"  {tt:<24}  {status}")
    print()
    print(f"  Output files → {OUTPUT_DIR}/")
    print("═" * 55)

    # Write JSON summary.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = {
        tid: {"type": id_to_type[tid], "status": (results[tid].get("task") or results[tid]).get("Status")}
        for tid in task_ids
    }
    summary_path = OUTPUT_DIR / "py_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("summary → %s", summary_path)


if __name__ == "__main__":
    main()
