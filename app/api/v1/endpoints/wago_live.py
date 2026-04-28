import asyncio
import logging
from typing import Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.utils import utc_now
from app.modbus_engine.client import ModbusClientWrapper
from app.modbus_engine.manager import modbus_manager

router = APIRouter(prefix="/wago-live", tags=["Wago Live"])
logger = logging.getLogger(__name__)


_live_clients: Dict[str, ModbusClientWrapper] = {}
_live_client_targets: Dict[str, Tuple[str, int]] = {}
_live_clients_lock = asyncio.Lock()


class WagoLiveRegisterValue(BaseModel):
    address: int
    modbus_register: int
    value: Optional[int]


class WagoLiveSnapshotResponse(BaseModel):
    device_id: str
    register_type: str
    connected: bool
    fetched_at: str
    values: List[WagoLiveRegisterValue]


class WagoLiveWriteCoilRequest(BaseModel):
    device_id: str
    address: int
    value: bool | int = 1
    pulse_ms: Optional[int] = None


class WagoLiveWriteCoilResponse(BaseModel):
    device_id: str
    address: int
    modbus_register: int
    connected: bool
    requested_value: int
    active_readback_value: Optional[int]
    readback_value: Optional[int]
    pulse_ms: Optional[int]
    method: Optional[str]
    fetched_at: str


class WagoLiveWriteRegisterRequest(BaseModel):
    device_id: str
    address: int
    value: int | float


class WagoLiveWriteRegisterResponse(BaseModel):
    device_id: str
    address: int
    modbus_register: int
    connected: bool
    requested_value: int
    readback_value: Optional[int]
    fetched_at: str


async def _get_live_client(device_id: str, host: str, port: int) -> ModbusClientWrapper:
    """
    Reuse one dedicated direct-read client per live WAGO device so the
    validation screen does not reconnect on every poll tick.
    """
    async with _live_clients_lock:
        current_target = (host, port)
        existing_client = _live_clients.get(device_id)
        existing_target = _live_client_targets.get(device_id)
        if existing_client is not None and existing_target == current_target:
            return existing_client

        if existing_client is not None:
            existing_client.close()

        client = ModbusClientWrapper(host=host, port=port)
        _live_clients[device_id] = client
        _live_client_targets[device_id] = current_target
        return client


async def close_live_clients() -> None:
    async with _live_clients_lock:
        clients = list(_live_clients.values())
        _live_clients.clear()
        _live_client_targets.clear()

    for client in clients:
        client.close()


def _normalize_snapshot_address(value: int, register_type: str) -> int:
    """
    Accepts either raw zero-based offsets or the spreadsheet-style Modbus
    register numbers used in the HMI screens.
    """
    if value < 0:
        return value

    if register_type == "coil":
        return value - 30511 if value >= 30511 else value

    if register_type == "discrete":
        if value >= 100001:
            return value - 100001
        return value - 30001 if value >= 30001 else value

    if register_type == "input":
        if value >= 443000:
            return value - 443000
        return value - 300001 if value >= 300001 else value

    if register_type == "holding":
        return value - 400001 if value >= 400001 else value

    return value


def _get_unit_id(device_conf: Dict[str, object]) -> int:
    try:
        return int(device_conf.get("unit_id", 1))
    except (TypeError, ValueError):
        return 1


def _get_coil_write_mode(device_conf: Dict[str, object]) -> str:
    mode = str(device_conf.get("coil_write_mode", "single")).strip().lower()
    if mode in {"multiple", "multi", "block"}:
        return "multiple"
    if mode == "auto":
        return "auto"
    return "single"


async def _read_single_coil(
    client: ModbusClientWrapper, address: int, *, unit_id: int
) -> Optional[int]:
    values = await client.read_coils(address, 1, unit=unit_id)
    if not values:
        return None
    return int(values[0])


async def _read_single_holding_register(
    client: ModbusClientWrapper, address: int, *, unit_id: int
) -> Optional[int]:
    values = await client.read_holding_registers(address, 1, unit=unit_id)
    if not values:
        return None
    return int(values[0])


async def _write_single_coil(
    client: ModbusClientWrapper,
    address: int,
    value: bool,
    *,
    unit_id: int,
    coil_write_mode: str,
) -> Optional[str]:
    attempts = [
        ("multiple", lambda: client.write_coils(address, [value], unit=unit_id)),
        ("single", lambda: client.write_coil(address, value, unit=unit_id)),
    ]
    if coil_write_mode == "single":
        attempts.reverse()

    for method_name, writer in attempts:
        if not await writer():
            continue

        readback = await _read_single_coil(client, address, unit_id=unit_id)
        if readback is None or readback == int(value):
            return method_name

        logger.warning(
            "WAGO live coil write reported success but readback mismatched for device=%s address=%s via %s: readback=%s expected=%s",
            getattr(client, "host", "unknown"),
            address,
            method_name,
            readback,
            int(value),
        )

    return None


@router.get("/snapshot", response_model=WagoLiveSnapshotResponse)
async def get_wago_live_snapshot(
    device_id: str = Query("wago", description="WAGO device ID"),
    register_type: str = Query(
        "input",
        description="Register type to read: input, holding, discrete, or coil",
    ),
    address: List[int] = Query(
        ...,
        description="Raw offsets or spreadsheet Modbus register numbers to read",
    ),
):
    """
    Reads raw Modbus registers/bits directly from the configured WAGO device.
    This bypasses the shared parameter/state pipeline so the live WAGO screen can
    validate PLC values against the spreadsheet-mapped Modbus addresses only.
    """
    if device_id not in modbus_manager.devices:
        raise HTTPException(status_code=404, detail=f"Unknown device: {device_id}")

    device_conf = modbus_manager.devices.get(device_id) or {}
    host = device_conf.get("host")
    port = int(device_conf.get("port", 502))
    unit_id = _get_unit_id(device_conf)
    if not host:
        raise HTTPException(status_code=503, detail=f"Device host unavailable for device: {device_id}")

    normalized_register_type = register_type.strip().lower()
    if normalized_register_type not in {"holding", "input", "discrete", "coil"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported register_type: {register_type}",
        )

    requested_addresses = sorted(
        {
            _normalize_snapshot_address(int(value), normalized_register_type)
            for value in address
        }
    )
    if not requested_addresses:
        raise HTTPException(status_code=400, detail="At least one address is required")

    values_by_address = {addr: None for addr in requested_addresses}
    blocks = modbus_manager.optimize_blocks(requested_addresses)
    blocks_ok = 0

    # Use one dedicated direct-read client per live view device so the shared
    # poller remains isolated, but repeated UI snapshots do not reconnect every time.
    client = await _get_live_client(device_id=device_id, host=host, port=port)
    for start, count in blocks:
        if normalized_register_type == "input":
            registers = await client.read_input_registers(start, count, unit=unit_id)
        elif normalized_register_type == "discrete":
            registers = await client.read_discrete_inputs(start, count, unit=unit_id)
        elif normalized_register_type == "coil":
            registers = await client.read_coils(start, count, unit=unit_id)
        else:
            registers = await client.read_holding_registers(start, count, unit=unit_id)
        if registers is None:
            continue

        blocks_ok += 1
        for index, raw_value in enumerate(registers):
            current_address = start + index
            if current_address in values_by_address:
                values_by_address[current_address] = raw_value

    fetched_at = utc_now().replace(microsecond=0).isoformat()
    connected = blocks_ok > 0
    register_base = {
        "coil": 30511,
        "discrete": 30001,
        "input": 300001,
        "holding": 400001,
    }[normalized_register_type]

    return WagoLiveSnapshotResponse(
        device_id=device_id,
        register_type=normalized_register_type,
        connected=connected,
        fetched_at=fetched_at,
        values=[
            WagoLiveRegisterValue(
                address=current_address,
                modbus_register=register_base + current_address,
                value=values_by_address[current_address],
            )
            for current_address in requested_addresses
        ],
    )


@router.post("/write-coil", response_model=WagoLiveWriteCoilResponse)
async def write_wago_live_coil(cmd: WagoLiveWriteCoilRequest):
    if cmd.device_id not in modbus_manager.devices:
        raise HTTPException(status_code=404, detail=f"Unknown device: {cmd.device_id}")

    if cmd.address < 0:
        raise HTTPException(status_code=400, detail="Address must be zero or greater")

    device_conf = modbus_manager.devices.get(cmd.device_id) or {}
    host = device_conf.get("host")
    port = int(device_conf.get("port", 502))
    unit_id = _get_unit_id(device_conf)
    coil_write_mode = _get_coil_write_mode(device_conf)
    if not host:
        raise HTTPException(
            status_code=503,
            detail=f"Device host unavailable for device: {cmd.device_id}",
        )

    client = await _get_live_client(device_id=cmd.device_id, host=host, port=port)
    normalized_address = _normalize_snapshot_address(int(cmd.address), "coil")
    requested_value = 1 if bool(cmd.value) else 0
    pulse_ms = cmd.pulse_ms
    if pulse_ms is not None:
        pulse_ms = max(50, min(int(pulse_ms), 10000))

    method = await _write_single_coil(
        client,
        normalized_address,
        bool(requested_value),
        unit_id=unit_id,
        coil_write_mode=coil_write_mode,
    )
    if method is None:
        raise HTTPException(
            status_code=502,
            detail=f"Modbus write rejected for {cmd.device_id} coil {normalized_address}",
        )

    active_readback_value = await _read_single_coil(client, normalized_address, unit_id=unit_id)
    final_readback_value = active_readback_value

    if pulse_ms and requested_value:
        await asyncio.sleep(pulse_ms / 1000.0)
        reset_method = await _write_single_coil(
            client,
            normalized_address,
            False,
            unit_id=unit_id,
            coil_write_mode=coil_write_mode,
        )
        if reset_method is None:
            raise HTTPException(
                status_code=502,
                detail=f"Modbus write rejected while clearing {cmd.device_id} coil {normalized_address}",
            )
        final_readback_value = await _read_single_coil(client, normalized_address, unit_id=unit_id)
        method = f"{method}->{reset_method}"

    fetched_at = utc_now().replace(microsecond=0).isoformat()
    logger.info(
        "WAGO LIVE WRITE - device=%s host=%s unit=%s coil=%s register=%s request=%s pulse_ms=%s method=%s active=%s final=%s",
        cmd.device_id,
        host,
        unit_id,
        normalized_address,
        30511 + normalized_address,
        requested_value,
        pulse_ms if requested_value else None,
        method,
        active_readback_value,
        final_readback_value,
    )
    return WagoLiveWriteCoilResponse(
        device_id=cmd.device_id,
        address=normalized_address,
        modbus_register=30511 + normalized_address,
        connected=active_readback_value is not None or final_readback_value is not None,
        requested_value=requested_value,
        active_readback_value=active_readback_value,
        readback_value=final_readback_value,
        pulse_ms=pulse_ms if requested_value else None,
        method=method,
        fetched_at=fetched_at,
    )


@router.post("/write-register", response_model=WagoLiveWriteRegisterResponse)
async def write_wago_live_register(cmd: WagoLiveWriteRegisterRequest):
    if cmd.device_id not in modbus_manager.devices:
        raise HTTPException(status_code=404, detail=f"Unknown device: {cmd.device_id}")

    if cmd.address < 0:
        raise HTTPException(status_code=400, detail="Address must be zero or greater")

    device_conf = modbus_manager.devices.get(cmd.device_id) or {}
    host = device_conf.get("host")
    port = int(device_conf.get("port", 502))
    unit_id = _get_unit_id(device_conf)
    if not host:
        raise HTTPException(
            status_code=503,
            detail=f"Device host unavailable for device: {cmd.device_id}",
        )

    client = await _get_live_client(device_id=cmd.device_id, host=host, port=port)
    normalized_address = _normalize_snapshot_address(int(cmd.address), "holding")
    requested_value = int(round(float(cmd.value)))

    success = await client.write_register(normalized_address, requested_value, unit=unit_id)
    if not success:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Modbus write rejected for {cmd.device_id} holding register "
                f"{normalized_address}"
            ),
        )

    readback_value = await _read_single_holding_register(
        client, normalized_address, unit_id=unit_id
    )
    fetched_at = utc_now().replace(microsecond=0).isoformat()
    logger.info(
        "WAGO LIVE REGISTER WRITE - device=%s host=%s unit=%s register=%s map=%s request=%s readback=%s",
        cmd.device_id,
        host,
        unit_id,
        normalized_address,
        400001 + normalized_address,
        requested_value,
        readback_value,
    )
    return WagoLiveWriteRegisterResponse(
        device_id=cmd.device_id,
        address=normalized_address,
        modbus_register=400001 + normalized_address,
        connected=readback_value is not None,
        requested_value=requested_value,
        readback_value=readback_value,
        fetched_at=fetched_at,
    )
