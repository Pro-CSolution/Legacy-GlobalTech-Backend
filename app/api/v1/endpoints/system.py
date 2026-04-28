import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

import yaml
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from app.core.config import settings
from app.core.paths import resolve_resource
from app.modbus_engine.manager import modbus_manager

router = APIRouter()
logger = logging.getLogger(__name__)

SUPPORTED_US_TIME_ZONES: tuple[dict[str, str], ...] = (
    {
        "id": "Eastern Standard Time",
        "label": "Eastern Time (ET)",
        "ianaName": "America/New_York",
    },
    {
        "id": "Central Standard Time",
        "label": "Central Time (CT)",
        "ianaName": "America/Chicago",
    },
    {
        "id": "Mountain Standard Time",
        "label": "Mountain Time (MT)",
        "ianaName": "America/Denver",
    },
    {
        "id": "Pacific Standard Time",
        "label": "Pacific Time (PT)",
        "ianaName": "America/Los_Angeles",
    },
)
SUPPORTED_US_TIME_ZONES_BY_ID = {zone["id"]: zone for zone in SUPPORTED_US_TIME_ZONES}


def _is_loopback(host: Optional[str]) -> bool:
    if not host:
        return False
    if host in {"127.0.0.1", "::1"}:
        return True
    # 127.0.0.0/8
    return host.startswith("127.")


class DeviceConfigRead(BaseModel):
    id: str
    host: str
    port: int
    name: str


class DeviceConfigUpdateRequest(BaseModel):
    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    name: str = Field(min_length=1)

    @field_validator("host", "name")
    @classmethod
    def _strip_and_validate(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("must not be empty")
        return normalized


class SupportedTimeZoneRead(BaseModel):
    id: str
    label: str
    ianaName: str


class SystemTimeZoneRead(BaseModel):
    currentTimeZoneId: str
    currentTimeZoneLabel: str
    currentTimeZoneIanaName: Optional[str] = None
    currentTimeZoneSupported: bool
    supportedTimeZones: list[SupportedTimeZoneRead]


class SystemTimeZoneUpdateRequest(BaseModel):
    timeZoneId: str = Field(min_length=1)

    @field_validator("timeZoneId")
    @classmethod
    def _validate_time_zone_id(cls, value: str) -> str:
        normalized = value.strip()
        if normalized not in SUPPORTED_US_TIME_ZONES_BY_ID:
            raise ValueError(
                "Unsupported time zone. Allowed values are Eastern, Central, Mountain, and Pacific."
            )
        return normalized


class CommsRefreshRead(BaseModel):
    status: str
    running: bool
    device_count: int
    devices: list[str]


def _authorize_system_action(
    request: Request,
    x_system_token: Optional[str],
) -> None:
    client_host = getattr(getattr(request, "client", None), "host", None)

    if not settings.SYSTEM_ACTIONS_ALLOW_REMOTE:
        if not _is_loopback(client_host):
            raise HTTPException(status_code=403, detail="Forbidden.")
        return

    token = (settings.SYSTEM_ACTIONS_TOKEN or "").strip()
    if not token:
        raise HTTPException(
            status_code=503,
            detail="System actions token is required when remote access is enabled.",
        )
    if not x_system_token or x_system_token != token:
        raise HTTPException(status_code=401, detail="Unauthorized.")


def _devices_config_path() -> Path:
    return resolve_resource("config/devices.yaml")


def _load_devices_document() -> tuple[Path, dict[str, Any]]:
    file_path = _devices_config_path()
    if not file_path.exists():
        raise HTTPException(status_code=500, detail="devices.yaml not found.")

    try:
        with file_path.open("r", encoding="utf-8") as config_file:
            document = yaml.safe_load(config_file) or {}
    except yaml.YAMLError:
        logger.exception("Failed to parse devices.yaml")
        raise HTTPException(status_code=500, detail="devices.yaml is invalid.")

    if not isinstance(document, dict):
        raise HTTPException(status_code=500, detail="devices.yaml root must be an object.")

    devices = document.get("devices")
    if devices is None:
        document["devices"] = []
    elif not isinstance(devices, list):
        raise HTTPException(status_code=500, detail="devices.yaml must contain a devices list.")

    return file_path, document


def _serialize_device_config(raw_device: dict[str, Any]) -> DeviceConfigRead:
    try:
        return DeviceConfigRead(
            id=str(raw_device["id"]),
            host=str(raw_device["host"]).strip(),
            port=int(raw_device.get("port", 502)),
            name=str(raw_device.get("name") or raw_device["id"]).strip(),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Invalid device entry in devices.yaml: {exc}",
        ) from exc


def _write_devices_document(file_path: Path, document: dict[str, Any]) -> None:
    fd, temp_path = tempfile.mkstemp(
        dir=str(file_path.parent),
        prefix=f".{file_path.stem}.",
        suffix=".tmp",
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as temp_file:
            yaml.safe_dump(
                document,
                temp_file,
                sort_keys=False,
                allow_unicode=True,
                default_flow_style=False,
            )
        os.replace(temp_path, file_path)
    except Exception as exc:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        logger.exception("Failed to write devices.yaml")
        raise HTTPException(status_code=500, detail="Failed to update devices.yaml.") from exc


def _run_windows_command(command: list[str], *, error_detail: str) -> str:
    if sys.platform != "win32":
        raise HTTPException(status_code=501, detail="Time zone changes are only supported on Windows.")

    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
    except Exception as exc:
        logger.exception("Failed to execute Windows system command: %s", command)
        raise HTTPException(status_code=500, detail=error_detail) from exc

    if completed.returncode != 0:
        logger.error(
            "Windows system command failed: %s stdout=%s stderr=%s",
            command,
            completed.stdout.strip(),
            completed.stderr.strip(),
        )
        raise HTTPException(status_code=500, detail=error_detail)

    return completed.stdout.strip()


def _get_current_windows_time_zone_id() -> str:
    current_time_zone_id = _run_windows_command(
        ["tzutil", "/g"],
        error_detail="Failed to read the current Windows time zone.",
    )
    if not current_time_zone_id:
        logger.error("tzutil /g returned an empty time zone id.")
        raise HTTPException(status_code=500, detail="Failed to read the current Windows time zone.")
    return current_time_zone_id


def _set_windows_time_zone(time_zone_id: str) -> None:
    _run_windows_command(
        ["tzutil", "/s", time_zone_id],
        error_detail="Failed to update the Windows time zone.",
    )


def _build_system_time_zone_read() -> SystemTimeZoneRead:
    current_time_zone_id = _get_current_windows_time_zone_id()
    selected_zone = SUPPORTED_US_TIME_ZONES_BY_ID.get(current_time_zone_id)

    return SystemTimeZoneRead(
        currentTimeZoneId=current_time_zone_id,
        currentTimeZoneLabel=selected_zone["label"] if selected_zone else current_time_zone_id,
        currentTimeZoneIanaName=selected_zone["ianaName"] if selected_zone else None,
        currentTimeZoneSupported=selected_zone is not None,
        supportedTimeZones=[
            SupportedTimeZoneRead(**zone_data) for zone_data in SUPPORTED_US_TIME_ZONES
        ],
    )


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


@router.get("/devices", response_model=list[DeviceConfigRead])
async def list_devices():
    _, document = _load_devices_document()
    return [_serialize_device_config(device) for device in document.get("devices", [])]


@router.put("/devices/{device_id}", response_model=DeviceConfigRead)
async def update_device_config(
    device_id: str,
    payload: DeviceConfigUpdateRequest,
    request: Request,
    x_system_token: Optional[str] = Header(default=None, alias="X-System-Token"),
):
    _authorize_system_action(request, x_system_token)

    file_path, document = _load_devices_document()
    devices = document.get("devices", [])

    target_index = next(
        (index for index, device in enumerate(devices) if str(device.get("id")) == device_id),
        None,
    )
    if target_index is None:
        raise HTTPException(status_code=404, detail=f"Device {device_id} not found.")

    updated_device = dict(devices[target_index])
    updated_device["host"] = payload.host
    updated_device["port"] = payload.port
    updated_device["name"] = payload.name
    devices[target_index] = updated_device

    _write_devices_document(file_path, document)

    runtime_applied = modbus_manager.apply_device_config(updated_device)
    logger.info(
        "Device config updated for %s -> %s:%s (%s), runtime_applied=%s",
        device_id,
        payload.host,
        payload.port,
        payload.name,
        runtime_applied,
    )

    return _serialize_device_config(updated_device)


@router.get("/time-zone", response_model=SystemTimeZoneRead)
async def get_system_time_zone():
    return _build_system_time_zone_read()


@router.put("/time-zone", response_model=SystemTimeZoneRead)
async def update_system_time_zone(
    payload: SystemTimeZoneUpdateRequest,
    request: Request,
    x_system_token: Optional[str] = Header(default=None, alias="X-System-Token"),
):
    client_host = getattr(getattr(request, "client", None), "host", None)
    _authorize_system_action(request, x_system_token)

    _set_windows_time_zone(payload.timeZoneId)
    logger.warning("Time zone updated from host=%s to %s", client_host, payload.timeZoneId)

    return _build_system_time_zone_read()


@router.post("/refresh-comms", response_model=CommsRefreshRead)
async def refresh_comms(
    request: Request,
    x_system_token: Optional[str] = Header(default=None, alias="X-System-Token"),
):
    """
    Rebuilds the Modbus/WAGO communication clients without rebooting Windows.
    """
    client_host = getattr(getattr(request, "client", None), "host", None)
    _authorize_system_action(request, x_system_token)

    from app.api.v1.endpoints.wago_live import close_live_clients

    await close_live_clients()
    result = await modbus_manager.refresh_comms()
    logger.warning(
        "Comms refresh requested from host=%s devices=%s",
        client_host,
        result.get("devices"),
    )
    return CommsRefreshRead(**result)


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
    _authorize_system_action(request, x_system_token)

    if sys.platform != "win32":
        raise HTTPException(status_code=501, detail="Reboot is only supported on Windows.")

    logger.warning("Reboot requested from host=%s ua=%s", client_host, request.headers.get("user-agent"))
    background_tasks.add_task(_do_windows_reboot)
    return {"status": "accepted"}


