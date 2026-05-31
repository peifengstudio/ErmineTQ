"""
Example task handlers for the ErmineTQ Python SDK.

Each handler signature: fn(payload: dict) -> dict
"""
from .http_fetch import handle_http_fetch
from .file_io import handle_file_write, handle_file_read
from .ollama import handle_ollama_chat

# Maps task type → handler function.
HANDLERS: dict = {
    "py_http_fetch": handle_http_fetch,
    "py_file_write": handle_file_write,
    "py_file_read":  handle_file_read,
    "py_ollama_chat": handle_ollama_chat,
}
