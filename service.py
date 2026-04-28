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
        configure_logging()

        from app.main import app_entry

        host = (os.getenv("HOST") or "127.0.0.1").strip()
        port = int(os.getenv("PORT") or "8000")
        access_log = _env_flag("ACCESS_LOG", default=False)

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


RUNNER = _UvicornServiceRunner()


if sys.platform == "win32":
    import win32event  # type: ignore
    import win32service  # type: ignore
    import win32serviceutil  # type: ignore
    import servicemanager  # type: ignore

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
                RUNNER.stop()
            finally:
                win32event.SetEvent(self._stop_event)

        def SvcDoRun(self):
            try:
                servicemanager.LogInfoMsg("GlobalTechBackendService starting...")
            except Exception:
                pass

            try:
                self.ReportServiceStatus(win32service.SERVICE_RUNNING)
                RUNNER.start()
            except Exception:
                logger.exception("Service crashed")
                try:
                    servicemanager.LogErrorMsg(
                        "GlobalTechBackendService crashed. See backend log file."
                    )
                except Exception:
                    pass
                raise
            finally:
                try:
                    servicemanager.LogInfoMsg("GlobalTechBackendService stopped.")
                except Exception:
                    pass
else:
    GlobalTechBackendService = None  # type: ignore


def _run_as_windows_service() -> None:
    if sys.platform != "win32":
        raise RuntimeError("Windows service runner is only supported on Windows.")

    try:
        configure_logging()
    except Exception:
        pass

    if len(sys.argv) <= 1:
        logger.info("Launching as SCM service host (no CLI args).")
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(GlobalTechBackendService)
        servicemanager.StartServiceCtrlDispatcher()
        return

    argv = list(sys.argv)
    if len(argv) >= 2 and argv[1] in {"install", "update"} and "--startup" not in argv:
        argv = [argv[0], "--startup", "auto"] + argv[1:]
        logger.info("Injecting default service startup type: auto")

    win32serviceutil.HandleCommandLine(GlobalTechBackendService, argv=argv)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    _run_as_windows_service()