"""
Handler registry for the Python Bridge.

Each handler is a callable:  fn(task_id: str, payload: dict) -> dict

Add new handlers here and they will automatically be announced to ErmineTQ
when the Bridge registers.
"""
from .http_fetch import handle_http_fetch
from .file_io import handle_file_write, handle_file_read
from .ollama import handle_ollama_chat

# Maps task type → handler function.
# The Bridge reads this dict to:
#   1. Advertise task_types to ErmineTQ on registration.
#   2. Route incoming tasks to the right handler.
HANDLERS: dict = {
    "py_http_fetch":  handle_http_fetch,
    "py_file_write":  handle_file_write,
    "py_file_read":   handle_file_read,
    "py_ollama_chat": handle_ollama_chat,
}
