from __future__ import annotations

import logging
import logging.config
import os
import sys
from pathlib import Path
from typing import Optional


def _resolve_log_dir() -> Path:
    """
    Choose a writable default log directory for:
    - Windows Service (often no console)
    - PyInstaller frozen executables (can't write inside _MEIPASS)
    """
    explicit = os.getenv("GLOBALTECH_LOG_DIR") or os.getenv("LOG_DIR")
    if explicit:
        return Path(explicit)

    program_data = os.getenv("PROGRAMDATA")
    local_appdata = os.getenv("LOCALAPPDATA")

    # In frozen/service scenarios we prefer a machine-wide directory.
    if getattr(sys, "frozen", False) and program_data:
        return Path(program_data) / "GlobalTech" / "Backend" / "logs"

    # Dev fallback: per-user location if available.
    if local_appdata:
        return Path(local_appdata) / "GlobalTech" / "Backend" / "logs"

    if program_data:
        return Path(program_data) / "GlobalTech" / "Backend" / "logs"

    return Path.cwd() / "logs"


def configure_logging(
    *,
    log_level: Optional[str] = None,
    log_dir: Optional[Path] = None,
    log_filename: str = "backend.log",
) -> Path:
    """
    Centralized logging configuration:
    - File logging by default (service-friendly).
    - Optional console logging via LOG_TO_CONSOLE=1.
    """
    level = (log_level or os.getenv("LOG_LEVEL") or "INFO").upper()
    log_dir = log_dir or _resolve_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / log_filename

    log_to_console = (os.getenv("LOG_TO_CONSOLE") or "0").strip() == "0"

    handlers: dict = {
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": level,
            "formatter": "standard",
            "filename": str(log_file),
            "maxBytes": 10 * 1024 * 1024,  # 10MB
            "backupCount": 10,
            "encoding": "utf-8",
        }
    }

    root_handlers = ["file"]
    if log_to_console:
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "level": level,
            "formatter": "standard",
            "stream": "ext://sys.stderr",
        }
        root_handlers.append("console")

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s %(levelname)s %(name)s [pid=%(process)d] %(message)s"
            }
        },
        "handlers": handlers,
        "root": {"level": level, "handlers": root_handlers},
        "loggers": {
            # Make sure uvicorn logs also go to the same handlers.
            "uvicorn": {"level": level, "handlers": root_handlers, "propagate": False},
            "uvicorn.error": {"level": level, "handlers": root_handlers, "propagate": False},
            "uvicorn.access": {"level": level, "handlers": root_handlers, "propagate": False},
        },
    }

    logging.config.dictConfig(config)
    return log_file


