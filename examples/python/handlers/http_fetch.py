"""py_http_fetch — fetch a URL and save response metadata to output/."""
from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path

import httpx

OUTPUT_DIR = Path(__file__).parent.parent.parent / "output"
logger = logging.getLogger(__name__)


def handle_http_fetch(payload: dict) -> dict:
    """
    Payload: {"url": "https://httpbin.org/json", "timeout_secs": 10}
    Output:  examples/output/py_http_fetch_<uid>.json
    """
    url = payload.get("url")
    if not url:
        raise ValueError("url is required")
    timeout = payload.get("timeout_secs", 10)

    logger.info("fetching %s", url)
    start = time.monotonic()

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        resp = client.get(url, headers={"User-Agent": "ErmineTQ-example/0.1"})

    elapsed_ms = int((time.monotonic() - start) * 1000)

    output = {
        "url": url,
        "status_code": resp.status_code,
        "content_type": resp.headers.get("content-type", ""),
        "body_bytes": len(resp.content),
        "body_preview": resp.text[:500],
        "elapsed_ms": elapsed_ms,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"py_http_fetch_{uuid.uuid4().hex[:8]}.json"
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    logger.info("saved → %s (status=%d, %dms)", out_path, resp.status_code, elapsed_ms)
    return {"output_file": str(out_path), "status_code": resp.status_code}
