from __future__ import annotations

import logging
import multiprocessing
import os
import sys
from typing import Optional

import uvicorn

from app.core.logging_config import configure_logging


logger = logging.getLogger("service")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


class _UvicornServiceRunner:
    def __init__(self):
        self.server: Optional[uvicorn.Server] = None

    def start(self) -> None:
        # Configure file logging BEFORE importing the ASGI app (service-friendly).
        configure_logging()

        from app.main import app_entry  # import after logging is configured

        # Default to loopback only (safe-by-default). Override with HOST env var if needed.
        host = (os.getenv("HOST") or "127.0.0.1").strip()
        port = int(os.getenv("PORT") or "8000")

        access_log = _env_flag("ACCESS_LOG", default=False)

        # Use pure-python HTTP implementation for maximum PyInstaller stability.
        # (httptools is faster but is a compiled extension).
        config = uvicorn.Config(
            app_entry,
            host=host,
            port=port,
            reload=False,
            log_config=None,
            access_log=access_log,
            loop="asyncio",
            http="h11",
        )
        self.server = uvicorn.Server(config)
        logger.info("Starting Uvicorn server on %s:%s", host, port)
        self.server.run()
        logger.info("Uvicorn server stopped.")

    def stop(self) -> None:
        if self.server is not None:
            logger.info("Stopping Uvicorn server...")
            self.server.should_exit = True


def _run_as_windows_service() -> None:
    """
    Windows Service entrypoint (pywin32).

    Usage (dev/python):
      python service.py install
      python service.py start
      python service.py stop
      python service.py remove
      python service.py debug

    In production, this file is the PyInstaller entry script for the service .exe.
    """
    if sys.platform != "win32":
        raise RuntimeError("Windows service runner is only supported on Windows.")

    try:
        import win32event  # type: ignore
        import win32service  # type: ignore
        import win32serviceutil  # type: ignore
        import servicemanager  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "pywin32 is required to run as a Windows Service. Install pywin32 on Windows."
        ) from e

    # Configure logging even for CLI commands (install/start/stop/status/remove)
    # because this exe is built without a console window.
    try:
        configure_logging()
    except Exception:
        # Avoid preventing service management if logging can't initialize.
        pass

    runner = _UvicornServiceRunner()

    class GlobalTechBackendService(win32serviceutil.ServiceFramework):
        _svc_name_ = "GlobalTechBackend"
        _svc_display_name_ = "GlobalTech Backend"
        _svc_description_ = "GlobalTech Backend (FastAPI + Socket.IO) Windows Service"

        def __init__(self, args):
            super().__init__(args)
            self._stop_event = win32event.CreateEvent(None, 0, 0, None)

        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            try:
                runner.stop()
            finally:
                win32event.SetEvent(self._stop_event)

        def SvcDoRun(self):
            # Log to the Windows Event Log as well.
            try:
                servicemanager.LogInfoMsg("GlobalTechBackendService starting...")
            except Exception:
                pass

            try:
                self.ReportServiceStatus(win32service.SERVICE_RUNNING)
                runner.start()
            except Exception:
                # Ensure we have a record even without a console.
                logger.exception("Service crashed")
                try:
                    servicemanager.LogErrorMsg("GlobalTechBackendService crashed. See backend log file.")
                except Exception:
                    pass
                raise
            finally:
                try:
                    servicemanager.LogInfoMsg("GlobalTechBackendService stopped.")
                except Exception:
                    pass

    # When started by the Windows Service Control Manager, the service EXE is
    # typically launched with *no* command-line arguments. In that case we must
    # enter the service control dispatcher (equivalent to pythonservice.exe).
    #
    # When launched manually with arguments (install/start/stop/status/remove/debug),
    # delegate to pywin32's command handler.
    if len(sys.argv) <= 1:
        logger.info("Launching as SCM service host (no CLI args).")
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(GlobalTechBackendService)
        servicemanager.StartServiceCtrlDispatcher()
        return

    argv = list(sys.argv)
    # Default install/update to Automatic startup unless user explicitly chose otherwise.
    if len(argv) >= 2 and argv[1] in {"install", "update"} and "--startup" not in argv:
        argv = [argv[0], "--startup", "auto"] + argv[1:]
        logger.info("Injecting default service startup type: auto")

    win32serviceutil.HandleCommandLine(GlobalTechBackendService, argv=argv)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    _run_as_windows_service()


