import logging
import subprocess
import sys
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request

from app.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


def _is_loopback(host: Optional[str]) -> bool:
    if not host:
        return False
    if host in {"127.0.0.1", "::1"}:
        return True
    # 127.0.0.0/8
    return host.startswith("127.")


def _do_windows_reboot() -> None:
    """
    Ejecuta un reboot del sistema en Windows.
    Debe correr en un contexto con privilegios suficientes (p.ej. Windows Service como LocalSystem).
    """
    if sys.platform != "win32":
        logger.error("Reboot requested on non-Windows platform: %s", sys.platform)
        return

    try:
        # Nota: /f fuerza cierre de apps para un reboot inmediato.
        # Si luego querés un reboot "graceful", lo hacemos configurable.
        subprocess.Popen(
            ["shutdown", "/r", "/t", "0", "/f"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        logger.warning("Windows reboot command executed.")
    except Exception:
        logger.exception("Failed to execute Windows reboot command.")


@router.post("/reboot", status_code=202)
async def reboot_system(
    request: Request,
    background_tasks: BackgroundTasks,
    x_system_token: Optional[str] = Header(default=None, alias="X-System-Token"),
):
    """
    Reinicia el sistema operativo (Windows).

    Seguridad:
    - Por defecto solo acepta requests desde loopback (127.0.0.1 / ::1)
    - Si se habilita settings.SYSTEM_ACTIONS_ALLOW_REMOTE=true, entonces:
      - se requiere X-System-Token y debe matchear settings.SYSTEM_ACTIONS_TOKEN
    """
    client_host = getattr(getattr(request, "client", None), "host", None)
    if not settings.SYSTEM_ACTIONS_ALLOW_REMOTE:
        if not _is_loopback(client_host):
            raise HTTPException(status_code=403, detail="Forbidden.")
    else:
        token = (settings.SYSTEM_ACTIONS_TOKEN or "").strip()
        if not token:
            raise HTTPException(
                status_code=503,
                detail="System actions token is required when remote access is enabled.",
            )
        if not x_system_token or x_system_token != token:
            raise HTTPException(status_code=401, detail="Unauthorized.")

    if sys.platform != "win32":
        raise HTTPException(status_code=501, detail="Reboot is only supported on Windows.")

    logger.warning("Reboot requested from host=%s ua=%s", client_host, request.headers.get("user-agent"))
    background_tasks.add_task(_do_windows_reboot)
    return {"status": "accepted"}


