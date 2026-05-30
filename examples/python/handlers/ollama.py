"""py_ollama_chat — call a locally running Ollama instance via /api/chat."""
from __future__ import annotations

import logging
import time
from pathlib import Path

import httpx

OLLAMA_BASE     = "http://localhost:11434"
DEFAULT_MODEL   = "qwen2.5:3b-instruct"
OUTPUT_DIR      = Path(__file__).parent.parent.parent / "output"
logger          = logging.getLogger(__name__)


def handle_ollama_chat(task_id: str, payload: dict) -> dict:
    """
    Send a user message to local Ollama via /api/chat and save the response.

    Payload: {"model": "qwen2.5:3b-instruct", "prompt": "What's the weather like today?"}
    Output:  examples/output/py_ollama_<task_id>.txt

    If Ollama is not running the handler raises an exception, which causes
    ErmineTQ to retry the task according to the configured retry budget.
    Start Ollama with: ollama serve
    Pull model with:   ollama pull qwen2.5:3b-instruct
    """
    prompt = payload.get("prompt")
    if not prompt:
        raise ValueError("prompt is required")
    model = payload.get("model", DEFAULT_MODEL)

    request_body = {
        "model":   model,
        "messages": [{"role": "user", "content": prompt}],
        "stream":  False,
    }

    logger.info("calling Ollama  model=%s  prompt=%r", model, prompt[:80])
    start = time.monotonic()

    try:
        resp = httpx.post(
            f"{OLLAMA_BASE}/api/chat",
            json=request_body,
            timeout=120,
        )
        resp.raise_for_status()
    except httpx.ConnectError as exc:
        raise RuntimeError(
            f"Ollama not available at {OLLAMA_BASE}. "
            "Start it with: ollama serve"
        ) from exc

    elapsed_ms = int((time.monotonic() - start) * 1000)
    data = resp.json()

    response_text = data.get("message", {}).get("content", "")

    log_content = (
        f"=== py_ollama_chat ===\n"
        f"model:      {model}\n"
        f"prompt:     {prompt}\n"
        f"elapsed_ms: {elapsed_ms}\n\n"
        f"--- response ---\n{response_text}\n"
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"py_ollama_{task_id}.txt"
    out_path.write_text(log_content, encoding="utf-8")

    preview = response_text[:200] + ("…" if len(response_text) > 200 else "")
    logger.info("response saved → %s  (%dms)", out_path, elapsed_ms)

    return {
        "model":            model,
        "response_preview": preview,
        "elapsed_ms":       elapsed_ms,
        "output_file":      str(out_path),
    }
