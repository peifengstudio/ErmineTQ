"""ErmineTQ pull worker — registers with the server and executes tasks."""
from __future__ import annotations

import logging
import signal
import threading
import time
from typing import Any, Callable

import httpx

HandlerFn = Callable[[dict], Any]

logger = logging.getLogger(__name__)

# Backoff parameters for empty-queue polling (seconds).
_POLL_MIN: float = 0.5
_POLL_MAX: float = 3.0
_POLL_FACTOR: float = 2.0


class Worker:
    """
    Pull worker that polls ErmineTQ for tasks and executes registered handlers.

    Concurrency is achieved by running one polling thread per worker slot.
    Each thread independently claims, executes, and reports a task before
    looping back to claim the next one.

    Backoff: 500 ms base → doubles on empty queue → caps at 3 s.
    Resets to base immediately when a task is claimed.

    Usage::

        worker = Worker("http://localhost:8080", concurrency=4)

        @worker.register("send_email")
        def send_email(payload: dict) -> dict:
            ...
            return {"sent": True}

        worker.run()   # blocks; SIGINT / SIGTERM triggers graceful drain
    """

    def __init__(
        self,
        url: str = "http://localhost:8080",
        *,
        concurrency: int = 4,
        queue: str = "default",
    ) -> None:
        self._url = url.rstrip("/")
        self._concurrency = concurrency
        self._queue = queue
        self._handlers: dict[str, HandlerFn] = {}
        self._worker_id: str | None = None
        self._stop = threading.Event()

    # ── handler registration ──────────────────────────────────────────────────

    def register(self, task_type: str) -> Callable[[HandlerFn], HandlerFn]:
        """Decorator: register a callable as the handler for *task_type*.

        The handler receives the task payload (dict) and must return a
        JSON-serialisable value (or None).  Raise any exception to mark the
        attempt as failed; the server applies retry / dead logic automatically.
        """
        def decorator(fn: HandlerFn) -> HandlerFn:
            self._handlers[task_type] = fn
            logger.debug("registered handler for %r", task_type)
            return fn
        return decorator

    # ── main entry point ──────────────────────────────────────────────────────

    def run(self) -> None:
        """Register with ErmineTQ then block until SIGINT / SIGTERM.

        On shutdown, waits for all in-flight tasks to finish before returning.
        """
        if not self._handlers:
            raise RuntimeError(
                "No handlers registered. Use @worker.register('task_type') first."
            )

        self._worker_id = self._do_register()
        logger.info(
            "worker started  id=%s  types=%s  concurrency=%d  queue=%r",
            self._worker_id,
            sorted(self._handlers),
            self._concurrency,
            self._queue,
        )

        # Install signal handlers on the main thread.
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._on_signal)

        # One polling/execution thread per concurrency slot.
        threads = [
            threading.Thread(target=self._poll_loop, daemon=True, name=f"eq-worker-{i}")
            for i in range(self._concurrency)
        ]
        for t in threads:
            t.start()

        self._stop.wait()
        logger.info("draining %d worker thread(s)…", self._concurrency)
        for t in threads:
            t.join()
        logger.info("worker stopped")

    # ── internals ─────────────────────────────────────────────────────────────

    def _on_signal(self, sig: int, _frame: object) -> None:
        logger.info("received signal %d — stopping after current tasks finish", sig)
        self._stop.set()

    def _do_register(self) -> str:
        resp = httpx.post(
            f"{self._url}/api/workers/register",
            json={
                "type": "python",
                "task_types": sorted(self._handlers),
                "queue": self._queue,
                "concurrency": self._concurrency,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["ID"]

    def _poll_loop(self) -> None:
        """Runs in its own thread: claim → execute → report → repeat."""
        client = httpx.Client(base_url=self._url, timeout=30)
        backoff = _POLL_MIN
        try:
            while not self._stop.is_set():
                try:
                    claimed = self._claim(client)
                except Exception as exc:
                    logger.warning("claim error: %s", exc)
                    self._sleep(backoff)
                    backoff = min(backoff * _POLL_FACTOR, _POLL_MAX)
                    continue

                if claimed is None:
                    # Queue empty — back off.
                    self._sleep(backoff)
                    backoff = min(backoff * _POLL_FACTOR, _POLL_MAX)
                    continue

                # Task acquired — reset backoff and execute.
                backoff = _POLL_MIN
                self._execute(client, claimed["task"], claimed["attempt_id"])
        finally:
            client.close()

    def _sleep(self, seconds: float) -> None:
        """Interruptible sleep: wakes early if stop is set."""
        self._stop.wait(timeout=seconds)

    def _claim(self, client: httpx.Client) -> dict | None:
        resp = client.post(
            "/api/worker/claim",
            json={
                "worker_id": self._worker_id,
                "task_types": sorted(self._handlers),
                "queue": self._queue,
            },
        )
        if resp.status_code == 204:
            return None
        resp.raise_for_status()
        return resp.json()  # {"task": {...}, "attempt_id": "..."}

    def _execute(self, client: httpx.Client, task: dict, attempt_id: str) -> None:
        task_type = task["Type"]
        payload = task.get("Payload") or {}
        task_id = task["ID"]

        handler = self._handlers.get(task_type)
        if handler is None:
            # Should not happen: claim only returns types we advertised.
            self._report_fail(client, attempt_id, f"no handler for {task_type!r}")
            return

        logger.info("← task  id=%s  type=%s", task_id, task_type)
        try:
            result = handler(payload)
            self._report_succeed(client, attempt_id, result)
            logger.info("→ ok    id=%s", task_id)
        except Exception as exc:
            logger.exception("handler raised  id=%s  type=%s", task_id, task_type)
            self._report_fail(client, attempt_id, str(exc))

    def _report_succeed(self, client: httpx.Client, attempt_id: str, result: Any) -> None:
        resp = client.post(
            f"/api/worker/attempts/{attempt_id}/succeed",
            json={"result": result},
        )
        if not resp.is_success:
            logger.error("succeed report failed: %d %s", resp.status_code, resp.text)

    def _report_fail(self, client: httpx.Client, attempt_id: str, error: str) -> None:
        resp = client.post(
            f"/api/worker/attempts/{attempt_id}/fail",
            json={"error": error},
        )
        if not resp.is_success:
            logger.error("fail report failed: %d %s", resp.status_code, resp.text)
