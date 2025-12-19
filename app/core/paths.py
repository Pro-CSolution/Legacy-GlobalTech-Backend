from __future__ import annotations

import sys
from pathlib import Path
from typing import Union


def get_app_base_dir() -> Path:
    """
    Returns the base directory where embedded resources live.

    - Dev/Source: project root (folder containing `app/`, `config/`, `parameters/`)
    - PyInstaller (frozen): sys._MEIPASS (onefile temp dir or onedir bundle dir)
    """
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(meipass).resolve()
        # Fallback: best-effort to the executable directory.
        return Path(sys.executable).resolve().parent

    # app/core/paths.py -> app/core -> app -> project root
    return Path(__file__).resolve().parents[2]


ResourcePathLike = Union[str, Path]


def resolve_resource(path: ResourcePathLike) -> Path:
    """
    Resolve a resource path relative to the app base dir.
    If `path` is absolute, it is returned as-is.
    """
    p = Path(path)
    if p.is_absolute():
        return p
    return (get_app_base_dir() / p).resolve()


