import os
import sys

from IPython.core.magic import register_line_magic


@register_line_magic
def restart(*_):
    os.execv(sys.executable, [sys.executable, "-m", "IPython"] + sys.argv[1:])


def debugger(port: int = 5678, host: str = "127.0.0.1") -> None:
    """Attach VS Code via debugpy and stop on the next line."""
    import sys

    import debugpy

    if not getattr(debugger, "_listening", False):
        debugpy.listen((host, port))
        debugger._listening = True  # type: ignore[attr-defined]
        print(f"debugpy listening on {host}:{port}")

    if not debugpy.is_client_connected():
        print("Waiting for VS Code to attach…")
        debugpy.wait_for_client()
        print("Debugger attached.")

    if "debugpy._vendored" not in sys.modules:
        debugpy.breakpoint()
