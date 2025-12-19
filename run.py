import os
import multiprocessing
import sys

import uvicorn

from app.core.logging_config import configure_logging

if __name__ == "__main__":
    # Configure file logging BEFORE starting uvicorn (Windows service / PyInstaller friendly).
    configure_logging()

    # Import as object (not "module:attr") so PyInstaller can detect it reliably.
    from app.main import app_entry

    port = int(os.getenv("PORT", "8000"))
    reload = (os.getenv("RELOAD") or "0").strip() == "1"
    if getattr(sys, "frozen", False):
        # Uvicorn reload relies on file watchers/subprocesses and is not suitable for frozen executables.
        reload = False

    # Disable uvicorn's default console logging config; use our configured handlers instead.
    multiprocessing.freeze_support()
    # Default to loopback only (safe-by-default). Override with HOST env var if needed.
    host = (os.getenv("HOST") or "localhost").strip()
    uvicorn.run(app_entry, host=host, port=port, reload=reload, log_config=None)

