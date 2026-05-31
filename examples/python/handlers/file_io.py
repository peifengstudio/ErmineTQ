"""py_file_write / py_file_read — simple file I/O handlers."""
from __future__ import annotations

import logging
import uuid
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent.parent.parent / "output"
logger = logging.getLogger(__name__)


def handle_file_write(payload: dict) -> dict:
    """
    Write (or append) text to a file under examples/output/.

    Payload: {"filename": "hello.txt", "content": "Hello!\\n", "append": false}
    Output:  examples/output/<filename>
    """
    filename = payload.get("filename")
    if not filename:
        raise ValueError("filename is required")
    content = payload.get("content", "")
    append = payload.get("append", False)

    # Safety: no path traversal
    out_path = (OUTPUT_DIR / filename).resolve()
    if not str(out_path).startswith(str(OUTPUT_DIR.resolve())):
        raise ValueError("filename must stay within examples/output/")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    out_path.open(mode, encoding="utf-8").write(content)

    bytes_written = len(content.encode())
    logger.info("wrote %d bytes → %s (mode=%s)", bytes_written, out_path, mode)
    return {"path": str(out_path), "bytes_written": bytes_written, "mode": mode}


def handle_file_read(payload: dict) -> dict:
    """
    Read a file and write a log copy to examples/output/.

    Payload: {"path": "examples/output/hello.txt", "max_bytes": 65536}
    """
    path_str = payload.get("path")
    if not path_str:
        raise ValueError("path is required")
    max_bytes = payload.get("max_bytes", 64 * 1024)

    file_path = Path(path_str)
    if not file_path.exists():
        raise FileNotFoundError(f"file not found: {file_path}")

    raw = file_path.read_bytes()
    truncated = len(raw) > max_bytes
    content = raw[:max_bytes].decode("utf-8", errors="replace")

    log_content = (
        f"=== py_file_read log ===\n"
        f"path:      {file_path}\n"
        f"size:      {len(raw)} bytes\n"
        f"truncated: {truncated}\n\n"
        f"--- content ---\n{content}"
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"py_file_read_{uuid.uuid4().hex[:8]}.txt"
    out_path.write_text(log_content, encoding="utf-8")

    logger.info("read %d bytes from %s → log at %s", len(raw), file_path, out_path)
    return {
        "path": str(file_path),
        "size_bytes": len(raw),
        "truncated": truncated,
        "output_file": str(out_path),
    }
