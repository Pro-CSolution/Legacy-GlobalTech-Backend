from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import aiohttp
import socketio
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import parameter_registry
from app.core.paths import resolve_resource
from app.core.utils import calculate_modbus_address
from app.modbus_engine.client import ModbusClientWrapper

DEFAULT_PARAMS = [
    "P4.04",
    "P4.05",
    "P4.06",
    "P4.09",
    "P33.00",
    "P33.01",
    "P33.02",
    "P34.16",
    "P32.60",
    "P32.61",
]


def load_device_config(device_id: str) -> dict[str, Any]:
    devices_path = resolve_resource("config/devices.yaml")
    with devices_path.open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}

    for device in config.get("devices", []):
        if device.get("id") == device_id:
            return device

    raise ValueError(f"Device {device_id} not found in {devices_path}")


def ensure_registry_loaded(device_id: str, device_conf: dict[str, Any]) -> None:
    parameter_file = device_conf.get("parameter_file") or "parameters/default.parameters.json"
    parameter_registry.register_device(device_id, parameter_file)


def signed_16(value: int) -> int:
    return value - 65536 if value >= 32768 else value


def describe_source_code(raw_value: int) -> str:
    signed = signed_16(raw_value)
    invert = signed < 0
    magnitude = abs(signed)
    major = magnitude // 1000
    minor = magnitude % 1000
    prefix = "INV " if invert else ""

    if magnitude == 0:
        return "Fixed 0 (cleared)"
    if magnitude in (1, 1000):
        return "Fixed 1 (set)"
    if major == 1 and 1 <= minor <= 6:
        return f"{prefix}Digital Input {minor}"
    if major == 1 and minor == 14:
        return f"{prefix}Interlock"
    if major == 2 and 0 <= minor <= 111:
        return f"{prefix}Status Flag {minor}"
    if major == 3 and 0 <= minor <= 15:
        return f"{prefix}RS485 Control Word 0 bit {minor}"
    if major == 3 and 100 <= minor <= 115:
        return f"{prefix}RS485 Control Word 1 bit {minor - 100}"
    if major == 4 and 0 <= minor <= 15:
        return f"{prefix}RS232 Control Word 0 bit {minor}"
    if major == 4 and 100 <= minor <= 115:
        return f"{prefix}RS232 Control Word 1 bit {minor - 100}"
    if major == 5 and 300 <= minor <= 315:
        return f"{prefix}Ethernet Ch1 Control Word 1 bit {minor - 300}"
    if major == 5 and 316 <= minor <= 331:
        return f"{prefix}Ethernet Ch1 Control Word 2 bit {minor - 316}"
    if major == 5 and 332 <= minor <= 347:
        return f"{prefix}Ethernet Ch2 Control Word 1 bit {minor - 332}"
    if major == 5 and 348 <= minor <= 363:
        return f"{prefix}Ethernet Ch2 Control Word 2 bit {minor - 348}"

    return f"{prefix}Unknown source code {signed / 1000:.3f} (raw={raw_value})"


def format_bits(value: int) -> str:
    return format(value & 0xFFFF, "016b")


def list_set_bits(value: int, prefix: str, offset: int = 0) -> str:
    labels = [f"{prefix}{bit + offset}" for bit in range(16) if value & (1 << bit)]
    return ", ".join(labels) if labels else "(none)"


def bitfield_summary(meta_id: str, value: int) -> str | None:
    if meta_id == "P11.21":
        return list_set_bits(value, "DI", 1)
    if meta_id == "P11.22":
        return list_set_bits(value, "DO", 1)
    if meta_id == "P11.23":
        return list_set_bits(value, "CF", 0)
    if meta_id == "P11.24":
        return list_set_bits(value, "CF", 16)
    if meta_id == "P11.25":
        return list_set_bits(value, "CF", 32)
    if meta_id == "P11.26":
        return list_set_bits(value, "CF", 48)
    if meta_id in {"P32.60", "P32.61", "P86.27", "P86.28", "P88.27", "P88.28"}:
        return list_set_bits(value, "bit", 0)
    return None


def parameter_meta(param_id: str, device_id: str) -> dict[str, Any]:
    meta = parameter_registry.get_parameter(param_id, device_id=device_id) or {}
    address = parameter_registry.get_address(param_id, device_id=device_id)
    register_type = parameter_registry.get_register_type(param_id, device_id=device_id)

    if address is None:
        address = calculate_modbus_address(param_id)
    if not register_type:
        register_type = "holding"

    return {
        "id": param_id,
        "name": meta.get("name") or param_id,
        "address": address,
        "register_type": register_type,
        "scale_factor": meta.get("scale_factor"),
    }


async def read_register(client: ModbusClientWrapper, register_type: str, address: int) -> int | None:
    if register_type == "holding":
        values = await client.read_holding_registers(address, 1, unit=1)
    elif register_type == "input":
        values = await client.read_input_registers(address, 1, unit=1)
    else:
        raise ValueError(f"Unsupported register type for this script: {register_type}")

    if not values:
        return None
    return int(values[0])


async def inspect_parameter(client: ModbusClientWrapper, meta: dict[str, Any]) -> None:
    value = await read_register(client, meta["register_type"], meta["address"])
    print("-" * 88)
    print(f"{meta['id']} | {meta['name']}")
    print(f"  register_type : {meta['register_type']}")
    print(f"  address       : {meta['address']}")

    if value is None:
        print("  read          : FAILED")
        return

    signed = signed_16(value)
    print(f"  raw           : {value}")
    print(f"  signed        : {signed}")
    print(f"  hex           : 0x{value:04X}")
    print(f"  bits          : {format_bits(value)}")

    if meta["id"] in {"P32.60", "P32.61"}:
        print("  decode        : Control word bitfield")
    else:
        print(f"  /1000         : {signed / 1000:.3f}")
        print(f"  decode        : {describe_source_code(value)}")


def print_decoded_value(meta: dict[str, Any], raw_value: int, scaled_value: Any, source: str) -> None:
    signed = signed_16(raw_value)
    summary = bitfield_summary(meta["id"], raw_value)
    print("-" * 88)
    print(f"{meta['id']} | {meta['name']}")
    print(f"  source        : {source}")
    print(f"  register_type : {meta['register_type']}")
    print(f"  address       : {meta['address']}")
    print(f"  scaled        : {scaled_value}")
    print(f"  raw           : {raw_value}")
    print(f"  signed        : {signed}")
    print(f"  hex           : 0x{raw_value:04X}")
    print(f"  bits          : {format_bits(raw_value)}")
    if summary is not None:
        print(f"  set_bits      : {summary}")

    if meta["id"] in {"P32.60", "P32.61"}:
        print("  decode        : Control word bitfield")
    else:
        print(f"  /1000         : {signed / 1000:.3f}")
        print(f"  decode        : {describe_source_code(raw_value)}")


async def read_via_backend_socket(
    backend_url: str,
    socket_path: str,
    device_id: str,
    param_ids: list[str],
    timeout_s: float,
) -> dict[str, Any]:
    http_session = aiohttp.ClientSession()
    client = socketio.AsyncClient(
        reconnection=False,
        logger=False,
        engineio_logger=False,
        http_session=http_session,
    )
    collected: dict[str, Any] = {}
    expected = set(param_ids)
    event = asyncio.Event()

    @client.on("device_update")
    async def _on_device_update(payload: dict[str, Any]) -> None:
        if payload.get("device_id") != device_id:
            return
        data = payload.get("data") or {}
        for pid, value in data.items():
            if pid in expected:
                collected[pid] = value
        if expected.issubset(collected.keys()):
            event.set()

    try:
        await client.connect(backend_url, socketio_path=socket_path, transports=["websocket"])
        await client.emit("subscribe_device", device_id)
        if param_ids:
            await client.emit(
                "subscribe_parameter",
                {"device_id": device_id, "parameter_ids": param_ids},
            )
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_s)
        except asyncio.TimeoutError:
            pass
    except Exception as exc:
        raise RuntimeError(
            f"Could not connect to backend socket at {backend_url}{socket_path}: {exc}"
        ) from exc
    finally:
        try:
            if client.connected:
                await client.disconnect()
        finally:
            await client.shutdown()
            await http_session.close()

    return collected


async def main() -> int:
    parser = argparse.ArgumentParser(description="Read drive control source parameters in read-only mode.")
    parser.add_argument("--device", default="drive_avid", help="Device id from config/devices.yaml")
    parser.add_argument(
        "--mode",
        choices=["auto", "direct", "backend"],
        default="auto",
        help="Read directly from the device, via the running backend, or auto-fallback.",
    )
    parser.add_argument("--backend-url", default="http://localhost:8000", help="Backend base URL")
    parser.add_argument("--socket-path", default="/ws/socket.io", help="Socket.IO path")
    parser.add_argument("--timeout", type=float, default=8.0, help="Seconds to wait for socket data")
    parser.add_argument(
        "--params",
        nargs="*",
        default=DEFAULT_PARAMS,
        help="Parameter ids to inspect. Defaults to drive start/stop related parameters.",
    )
    args = parser.parse_args()

    device_conf = load_device_config(args.device)
    ensure_registry_loaded(args.device, device_conf)

    metas = [parameter_meta(param_id, args.device) for param_id in args.params]

    direct_succeeded = False
    if args.mode in {"auto", "direct"}:
        print(f"Connecting directly to {args.device} at {device_conf['host']}:{device_conf.get('port', 502)}")
        print("Read-only inspection; no writes will be performed.")

        client = ModbusClientWrapper(host=device_conf["host"], port=device_conf.get("port", 502))
        try:
            for meta in metas:
                value = await read_register(client, meta["register_type"], meta["address"])
                if value is not None:
                    direct_succeeded = True
                    print_decoded_value(meta, value, value, "direct-modbus")
                else:
                    print("-" * 88)
                    print(f"{meta['id']} | {meta['name']}")
                    print("  source        : direct-modbus")
                    print(f"  register_type : {meta['register_type']}")
                    print(f"  address       : {meta['address']}")
                    print("  read          : FAILED")
        finally:
            client.close()

    if args.mode == "direct" or (args.mode == "auto" and direct_succeeded):
        return 0

    registry_backed = [meta for meta in metas if parameter_registry.get_parameter(meta["id"], args.device)]
    skipped = [meta["id"] for meta in metas if meta not in registry_backed]

    print(f"Falling back to backend socket at {args.backend_url}{args.socket_path}")
    if skipped:
        print(f"Skipping non-registry params in backend mode: {', '.join(skipped)}")

    try:
        values = await read_via_backend_socket(
            args.backend_url,
            args.socket_path,
            args.device,
            [meta["id"] for meta in registry_backed],
            args.timeout,
        )
    except Exception as exc:
        print(f"Backend socket read failed: {exc}")
        return 1

    for meta in registry_backed:
        scaled_value = values.get(meta["id"])
        print("-" * 88)
        print(f"{meta['id']} | {meta['name']}")
        print("  source        : backend-socket")
        print(f"  register_type : {meta['register_type']}")
        print(f"  address       : {meta['address']}")
        if scaled_value is None:
            print("  read          : FAILED")
            continue

        scale_factor = meta["scale_factor"] or 1
        raw_value = int(round(float(scaled_value) * scale_factor))
        print(f"  scaled        : {scaled_value}")
        print(f"  raw_guess     : {raw_value}")
        print(f"  signed_guess  : {signed_16(raw_value)}")
        print(f"  hex_guess     : 0x{raw_value:04X}")
        print(f"  bits_guess    : {format_bits(raw_value)}")
        if meta["id"] in {"P32.60", "P32.61"}:
            print("  decode        : Control word bitfield")
        else:
            print(f"  /1000 guess   : {signed_16(raw_value) / 1000:.3f}")
            print(f"  decode        : {describe_source_code(raw_value)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
