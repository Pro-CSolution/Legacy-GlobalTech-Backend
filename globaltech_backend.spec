# -*- mode: python ; coding: utf-8 -*-
#
# PyInstaller spec for GlobalTech Backend (Windows Service + embedded resources).
#
# This spec is intended for a one-file build where all resources are bundled and
# extracted to sys._MEIPASS at runtime. The application uses app.core.paths.resolve_resource()
# to locate files under _MEIPASS/config and _MEIPASS/parameters.
#

from PyInstaller.utils.hooks import collect_submodules


block_cipher = None


# Hidden imports:
# - SQLAlchemy loads dialects by string (postgresql+asyncpg)
# - python-socketio/engineio loads the ASGI driver dynamically
hiddenimports = [
    "sqlalchemy.dialects.postgresql.asyncpg",
    "asyncpg",
    "engineio.async_drivers.asgi",
    # pywin32: required by win32serviceutil (used by our service.exe command handler)
    "win32timezone",
    # Uvicorn (keep explicit to reduce surprises in frozen builds)
    "uvicorn.lifespan.on",
    "uvicorn.protocols.http.h11_impl",
    "uvicorn.protocols.websockets.websockets_impl",
]

# Add engineio/socketio submodules defensively (rare hook misses)
hiddenimports += collect_submodules("engineio")
hiddenimports += collect_submodules("socketio")


datas = [
    # Embedded runtime resources (resolved via resolve_resource()).
    # Use explicit globs (more compatible than Tree across PyInstaller versions).
    ("config\\*.yaml", "config"),
    ("config\\*.json", "config"),
    ("parameters\\*.json", "parameters"),
    ("parameters.json", "."),
]


a = Analysis(
    ["service.py"],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="GlobalTechBackend",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
)


