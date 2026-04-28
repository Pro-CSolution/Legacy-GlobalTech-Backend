from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import parameter_registry
from app.core.paths import resolve_resource
from app.modbus_engine.client import ModbusClientWrapper

DRIVE_DEFAULT_DEVICE_ID = "drive_avid"
WAGO_DEFAULT_DEVICE_ID = "wago"

DRIVE_PARAMS = [
    "P1.00",   # Speed reference
    "P1.01",   # Speed feedback
    "P1.02",   # Motor current
    "P1.03",   # Frequency feedback
    "P4.04",   # CF1: Start
    "P4.05",   # CF0: Normal Stop
    "P4.06",   # CF2: Rapid Stop
    "P4.09",   # CF116: Keypad/Remote
    "P5.01",   # Speed ref 1 source
    "P5.02",
    "P5.03",
    "P5.04",
    "P5.05",   # Backup speed ref
    "P5.11",   # Direction control
    "P5.12",   # CF3 direction
    "P11.23",  # Control flags 0-15
    "P11.24",  # Control flags 16-31
    "P33.00",
    "P33.01",
    "P33.02",
    "P33.03",  # CF3 source
    "P33.04",
    "P33.05",
    "P33.06",
    "P33.07",
    "P34.16",
    "P42.00",  # Pointer 1 source
    "P42.01",  # Pointer 1 scale
    "P86.25",  # Ethernet reference
    "P86.27",  # Ethernet control word 1
    "P86.29",  # Ethernet reference fallback
]

WAGO_PARAMS = [
    "RB_Local_Control",
    "RB_Remote_Control",
    "RB_Estop_Status",
    "Supply_480VAC_On",
    "RB_Drive_OK",
    "RB_Drive_Running",
    "RB_Drive_Start",
]

ETHERNET_EXPECTED: dict[str, float] = {
    "P33.00": 5300,
    "P33.01": 5301,
    "P33.02": 5302,
    "P33.04": 1,
    "P33.05": 0,
    "P33.06": 0,
    "P33.07": 0,
    "P34.16": 5303,
    "P42.00": 86.25,
    "P42.01": 100,
    "P5.01": 21,
    "P5.05": 1,
}


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


def is_signed_register_parameter(device_id: str, param_id: str) -> bool:
    param = parameter_registry.get_parameter(param_id, device_id=device_id) or {}
    register_type = parameter_registry.get_register_type(param_id, device_id=device_id)
    if register_type not in {"holding", "input"}:
        return False

    range_numeric = param.get("range_numeric")
    if isinstance(range_numeric, dict):
        min_value = range_numeric.get("min")
        try:
            if min_value is not None and float(min_value) < 0:
                return True
        except (TypeError, ValueError):
            pass

    default_value = param.get("default")
    if isinstance(default_value, str) and default_value.strip().startswith("-"):
        return True

    try:
        if int(param.get("menu")) in {33, 34}:
            return True
    except (TypeError, ValueError):
        pass

    control_flag_text = " ".join(
        str(param.get(field) or "")
        for field in ("name", "description", "range_text")
    ).lower()
    return "control flag" in control_flag_text


def decode_signed_register_value(raw_value: int) -> int:
    return raw_value - 0x10000 if raw_value >= 0x8000 else raw_value


def format_bits(value: int) -> str:
    return format(value & 0xFFFF, "016b")


def parameter_meta(param_id: str, device_id: str) -> dict[str, Any]:
    meta = parameter_registry.get_parameter(param_id, device_id=device_id) or {}
    address = parameter_registry.get_address(param_id, device_id=device_id)
    register_type = parameter_registry.get_register_type(param_id, device_id=device_id)
    scale_factor = parameter_registry.get_scale_factor(param_id, device_id=device_id)

    if address is None:
        raise ValueError(f"{device_id}.{param_id} is not mapped to an address")

    return {
        "id": param_id,
        "name": meta.get("name") or param_id,
        "address": int(address),
        "register_type": register_type,
        "scale_factor": scale_factor or 1,
    }


async def read_raw_value(client: ModbusClientWrapper, register_type: str, address: int) -> int | None:
    if register_type == "holding":
        values = await client.read_holding_registers(address, 1, unit=1)
    elif register_type == "input":
        values = await client.read_input_registers(address, 1, unit=1)
    elif register_type == "coil":
        values = await client.read_coils(address, 1, unit=1)
    elif register_type == "discrete":
        values = await client.read_discrete_inputs(address, 1, unit=1)
    else:
        raise ValueError(f"Unsupported register type: {register_type}")

    if not values:
        return None
    return int(values[0])


def scale_value(device_id: str, meta: dict[str, Any], raw_value: int) -> Any:
    if meta["register_type"] in {"coil", "discrete"}:
        return int(raw_value)

    decoded = decode_signed_register_value(raw_value) if is_signed_register_parameter(device_id, meta["id"]) else raw_value
    return decoded / (meta["scale_factor"] or 1)


async def capture_device(device_id: str, params: list[str]) -> list[dict[str, Any]]:
    device_conf = load_device_config(device_id)
    ensure_registry_loaded(device_id, device_conf)

    client = ModbusClientWrapper(host=device_conf["host"], port=device_conf.get("port", 502))
    rows: list[dict[str, Any]] = []

    try:
        for param_id in params:
            meta = parameter_meta(param_id, device_id)
            raw_value = await read_raw_value(client, meta["register_type"], meta["address"])
            scaled_value = None if raw_value is None else scale_value(device_id, meta, raw_value)
            rows.append(
                {
                    **meta,
                    "raw": raw_value,
                    "scaled": scaled_value,
                }
            )
    finally:
        client.close()

    return rows


def build_value_map(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {row["id"]: row["scaled"] for row in rows if row.get("scaled") is not None}


def ethernet_mode_summary(values: dict[str, Any]) -> tuple[bool, list[str]]:
    mismatches: list[str] = []
    for param_id, expected in ETHERNET_EXPECTED.items():
        actual = values.get(param_id)
        if actual is None:
            mismatches.append(f"{param_id}=missing (expected {expected})")
            continue
        if abs(float(actual) - float(expected)) > 0.01:
            mismatches.append(f"{param_id}={actual} (expected {expected})")
    return len(mismatches) == 0, mismatches


def print_section(title: str) -> None:
    print()
    print("=" * 96)
    print(title)
    print("=" * 96)


def print_rows(device_id: str, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        raw = row["raw"]
        scaled = row["scaled"]
        if raw is None:
            print(
                f"{device_id:12} {row['id']:15} {row['register_type']:8} addr={row['address']:5} "
                f"READ FAILED"
            )
            continue

        line = (
            f"{device_id:12} {row['id']:15} {row['register_type']:8} addr={row['address']:5} "
            f"scaled={str(scaled):>10} raw={raw:>6}"
        )
        if row["register_type"] in {"holding", "input"}:
            line += f" hex=0x{raw:04X}"
        print(line)

        if row["id"] in {"P11.23", "P11.24", "P86.27"} and row["register_type"] == "holding":
            print(f"{'':12} {'':15} {'':8}            bits={format_bits(int(raw))}")


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture a read-only snapshot of the HMI Ethernet speed path and run permissives."
    )
    parser.add_argument("--label", default="", help="Short label for this snapshot")
    parser.add_argument("--drive-device", default=DRIVE_DEFAULT_DEVICE_ID, help="Drive device id")
    parser.add_argument("--wago-device", default=WAGO_DEFAULT_DEVICE_ID, help="Wago device id")
    args = parser.parse_args()

    timestamp = datetime.now().isoformat(timespec="seconds")
    snapshot_label = args.label.strip() or "snapshot"

    print(f"[{timestamp}] {snapshot_label}")
    print("Read-only capture. No writes are performed.")

    drive_rows = await capture_device(args.drive_device, DRIVE_PARAMS)
    wago_rows = await capture_device(args.wago_device, WAGO_PARAMS)

    drive_values = build_value_map(drive_rows)
    wago_values = build_value_map(wago_rows)
    ethernet_ok, ethernet_mismatches = ethernet_mode_summary(drive_values)

    print_section("Drive Snapshot")
    print_rows(args.drive_device, drive_rows)

    print_section("Wago Snapshot")
    print_rows(args.wago_device, wago_rows)

    print_section("Quick Summary")
    print(f"ethernet_mode_complete: {ethernet_ok}")
    if ethernet_mismatches:
        print("ethernet_mode_mismatches:")
        for mismatch in ethernet_mismatches:
            print(f"  - {mismatch}")
    print(f"P86.25_ethernet_reference: {drive_values.get('P86.25')}")
    print(f"P1.00_speed_reference:     {drive_values.get('P1.00')}")
    print(f"P1.03_frequency_feedback:  {drive_values.get('P1.03')}")
    print(f"P1.02_motor_current:       {drive_values.get('P1.02')}")
    print(f"P86.27_control_word:       {drive_values.get('P86.27')}")
    print(f"P5.11_direction_control:   {drive_values.get('P5.11')}")
    print(f"P5.12_cf3_direction:       {drive_values.get('P5.12')}")
    print(f"P33.03_cf3_source:         {drive_values.get('P33.03')}")
    print(f"RB_Remote_Control:         {wago_values.get('RB_Remote_Control')}")
    print(f"RB_Local_Control:          {wago_values.get('RB_Local_Control')}")
    print(f"RB_Drive_OK:               {wago_values.get('RB_Drive_OK')}")
    print(f"RB_Drive_Running:          {wago_values.get('RB_Drive_Running')}")
    print(f"RB_Drive_Start:            {wago_values.get('RB_Drive_Start')}")
    print(f"Supply_480VAC_On:          {wago_values.get('Supply_480VAC_On')}")
    print(f"RB_Estop_Status:           {wago_values.get('RB_Estop_Status')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
