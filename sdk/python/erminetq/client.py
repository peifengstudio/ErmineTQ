"""ErmineTQ HTTP client — submit tasks and query state."""
from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx


class Client:
    """
    Thin wrapper around the ErmineTQ HTTP API.

    Usage::

        with Client("http://localhost:8080") as c:
            task_id = c.submit("send_email", {"to": "alice@example.com"})
            task    = c.get_task(task_id)
    """

    def __init__(self, url: str = "http://localhost:8080", *, timeout: float = 15.0) -> None:
        self._http = httpx.Client(base_url=url.rstrip("/"), timeout=timeout)

    # ── task submission ───────────────────────────────────────────────────────

    def submit(
        self,
        task_type: str,
        payload: Any = None,
        *,
        queue: str = "default",
        priority: int = 0,
        max_retries: int = 0,
        run_at: datetime | None = None,
    ) -> str:
        """Submit a task and return its ID."""
        body: dict[str, Any] = {
            "type": task_type,
            "payload": payload,
            "queue": queue,
            "priority": priority,
            "max_retries": max_retries,
        }
        if run_at is not None:
            body["run_at"] = run_at.isoformat()

        resp = self._http.post("/api/tasks", json=body)
        resp.raise_for_status()
        return resp.json()["ID"]

    # ── task queries ──────────────────────────────────────────────────────────

    def get_task(self, task_id: str) -> dict:
        """Return task detail including attempts and events."""
        resp = self._http.get(f"/api/tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()

    def list_tasks(
        self,
        *,
        status: str | None = None,
        task_type: str | None = None,
        queue: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict]:
        """List tasks with optional filters."""
        params: dict[str, Any] = {}
        if status is not None:
            params["status"] = status
        if task_type is not None:
            params["type"] = task_type
        if queue is not None:
            params["queue"] = queue
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        resp = self._http.get("/api/tasks", params=params)
        resp.raise_for_status()
        return resp.json()

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
