"""
ErmineTQ Python SDK — example worker.

Registers all handlers defined in examples/python/handlers/ with ErmineTQ
and begins polling for tasks.  Runs until SIGINT (Ctrl+C) or SIGTERM.

Usage:
    # Terminal 1
    make dev

    # Terminal 2
    cd examples/python
    uv run python worker.py

Environment variables:
    ERMINETQ_URL   server base URL  (default: http://localhost:8080)
    CONCURRENCY    worker threads   (default: 4)
"""
from __future__ import annotations

import logging
import os
import sys

from erminetq import Worker
from handlers import HANDLERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("example-worker")


def main() -> None:
    url = os.getenv("ERMINETQ_URL", "http://localhost:8080")
    concurrency = int(os.getenv("CONCURRENCY", "4"))

    logger.info("connecting to %s", url)

    worker = Worker(url, concurrency=concurrency, queue="default")

    for task_type, fn in HANDLERS.items():
        worker.register(task_type)(fn)

    try:
        worker.run()
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
