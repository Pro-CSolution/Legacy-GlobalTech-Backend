import asyncio
import yaml
import logging
from bisect import bisect_left
from typing import Any, Dict, List, Set, Tuple
from app.core.config import settings, parameter_registry
from app.core.paths import resolve_resource
from app.core.utils import utc_now
from app.modbus_engine.client import ModbusClientWrapper
from app.modbus_engine.mock_client import MockModbusClient
from app.modbus_engine.state import state_manager

logger = logging.getLogger(__name__)

LEGACY_ETHERNET_SPEED_DEVICE_ID = "drive_avid"
LEGACY_FIXED_REFERENCE_PARAM_ID = "P21.01"
ETHERNET_REFERENCE_PARAM_ID = "P86.25"
ETHERNET_REFERENCE_SOURCE_PARAM_ID = "P5.01"
BACKUP_SPEED_REFERENCE_PARAM_ID = "P5.05"
CLAMP_ZERO_REFERENCE_PARAM_ID = "P5.21"
ETHERNET_POINTER_SOURCE_PARAM_ID = "P42.00"
ETHERNET_POINTER_SCALE_PARAM_ID = "P42.01"
ETHERNET_REF1_SELECTOR_PARAM_ID = "P33.04"
ETHERNET_REF2_SELECTOR_PARAM_ID = "P33.05"
ETHERNET_REF3_SELECTOR_PARAM_ID = "P33.06"
ETHERNET_REF4_SELECTOR_PARAM_ID = "P33.07"
LEGACY_ETHERNET_REFERENCE_SOURCE_VALUE = 4
ETHERNET_REFERENCE_SOURCE_VALUE = 21
KEYPAD_SPEED_REFERENCE_SOURCE_VALUE = 1
ETHERNET_POINTER_SOURCE_VALUE = 86.25
ETHERNET_POINTER_SCALE_VALUE = 100
CONTROL_FLAG_SET_VALUE = 1
CONTROL_FLAG_CLEAR_VALUE = 0
TRIP_RESET_SOURCE_PARAM_ID = "P10.34"
TRIP_RESET_FIXED_SET_SOURCE_RAW = 1000
TRIP_RESET_CONTROL_WORD_MAPPINGS: Tuple[Tuple[int, int, str, int], ...] = (
    (3000, 3015, "P32.60", 3000),
    (3100, 3115, "P32.61", 3100),
    (5300, 5315, "P86.27", 5300),
    (5316, 5331, "P86.28", 5316),
    (5332, 5347, "P86.77", 5332),
    (5348, 5363, "P86.78", 5348),
)
ETHERNET_MODE_CONTROL_PARAMS = {
    "P33.00": 5300,
    "P33.01": 5301,
    "P33.02": 5302,
    ETHERNET_REF1_SELECTOR_PARAM_ID: CONTROL_FLAG_SET_VALUE,
    ETHERNET_REF2_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
    ETHERNET_REF3_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
    ETHERNET_REF4_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
    "P34.16": 5303,
}
TRIP_STATUS_PARAM_IDS: Tuple[str, ...] = (
    "P10.00",
    "P10.01",
    "P10.02",
    "P10.10",
    "P10.11",
    "P10.12",
    "P10.13",
    "P10.14",
    "P10.20",
    TRIP_RESET_SOURCE_PARAM_ID,
)


class _RestartPollingLoop(RuntimeError):
    """Raised when the active polling loop is wedged and must be replaced."""


class ModbusManager:
    def __init__(self):
        self.devices: Dict[str, Dict] = {} # Config data
        self.clients: Dict[str, ModbusClientWrapper] = {}
        # device_id -> {register_type: [addresses]}
        self.critical_addresses: Dict[str, Dict[str, List[int]]] = {}
        self.running = False
        # Connection tracking (published via state_manager as non-parameter fields)
        self._disconnect_after_failures: int = 1
        self._consecutive_failures: Dict[str, int] = {}
        self._connected: Dict[str, bool] = {}
        self._last_ok_ts: Dict[str, str | None] = {}
        self._loop_task: asyncio.Task | None = None

    def _start_loop_task(self) -> asyncio.Task | None:
        if not self.running:
            return None
        if self._loop_task is None or self._loop_task.done():
            logger.info("Starting Modbus Engine Loop...")
            self._loop_task = asyncio.create_task(self._loop())
        return self._loop_task

    async def _restart_loop_task(self, reason: str) -> None:
        loop_task = self._loop_task
        self._loop_task = None

        if loop_task is not None and not loop_task.done():
            logger.warning("Restarting Modbus polling task (%s).", reason)
            loop_task.cancel()
            try:
                await asyncio.wait_for(loop_task, timeout=2.0)
            except asyncio.TimeoutError:
                logger.error(
                    "Timed out while cancelling the previous Modbus polling task during %s; "
                    "starting a replacement loop.",
                    reason,
                )
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning(
                    "Previous Modbus polling task exited with an error during %s: %s",
                    reason,
                    exc,
                )

        self._start_loop_task()

    async def _drain_device_tasks(
        self,
        task_by_device: Dict[str, asyncio.Task],
        *,
        timeout_s: float = 2.0,
        context: str = "Modbus device tasks",
    ) -> None:
        if not task_by_device:
            return

        try:
            await asyncio.wait_for(
                asyncio.gather(*task_by_device.values(), return_exceptions=True),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError as exc:
            stuck_devices = [did for did, task in task_by_device.items() if not task.done()]
            raise _RestartPollingLoop(
                f"{context} did not drain after cancellation; stuck devices: {stuck_devices}"
            ) from exc

    def _build_client(self, device_conf: Dict):
        if settings.USE_MOCK_DATA:
            logger.warning(f"Using MOCK client for device {device_conf['id']}")
            return MockModbusClient(
                host=device_conf["host"],
                port=device_conf.get("port", 502),
                device_id=device_conf["id"],
            )

        return ModbusClientWrapper(
            host=device_conf["host"],
            port=device_conf.get("port", 502),
        )

    def _get_device_unit_id(self, device_id: str) -> int:
        device_conf = self.devices.get(device_id) or {}
        try:
            return int(device_conf.get("unit_id", 1))
        except (TypeError, ValueError):
            return 1

    def _get_coil_write_mode(self, device_id: str) -> str:
        device_conf = self.devices.get(device_id) or {}
        coil_write_mode = str(device_conf.get("coil_write_mode", "single")).strip().lower()
        if coil_write_mode in {"multiple", "multi", "block"}:
            return "multiple"
        if coil_write_mode == "auto":
            return "auto"
        return "single"

    def _publish_connection_state(self, device_id: str):
        """
        Publishes connection status for a device into the same realtime stream
        used for parameters (Socket.IO device_update).

        Keys are prefixed with '__' to avoid collisions with real parameter IDs.
        """
        connected = bool(self._connected.get(device_id, False))
        last_ts = self._last_ok_ts.get(device_id)
        state_manager.update(
            device_id,
            {
                "__connected": connected,
                "__lastOkTs": last_ts,
            },
        )

    def _mark_cycle_result(self, device_id: str, ok: bool):
        """
        Updates internal counters and publishes changes:
        - ok=True resets failure counter, marks connected, updates lastOkTs (throttled to 1Hz)
        - ok=False increments failures; marks disconnected after threshold
        """
        prev_connected = self._connected.get(device_id, False)
        prev_last = self._last_ok_ts.get(device_id)
        prev_failures = self._consecutive_failures.get(device_id, 0)

        if ok:
            self._consecutive_failures[device_id] = 0
            self._connected[device_id] = True

            # Throttle timestamp updates to 1Hz (avoids emitting every poll tick)
            ts = utc_now().replace(microsecond=0).isoformat()
            if ts != prev_last:
                self._last_ok_ts[device_id] = ts
        else:
            self._consecutive_failures[device_id] = prev_failures + 1
            if self._consecutive_failures[device_id] >= self._disconnect_after_failures:
                self._connected[device_id] = False

        current_connected = self._connected.get(device_id, False)
        current_last = self._last_ok_ts.get(device_id)

        if current_connected != prev_connected or current_last != prev_last:
            self._publish_connection_state(device_id)

    def load_config(self):
        try:
            file_path = resolve_resource("config/devices.yaml")
            with file_path.open("r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                
            if not config:
                logger.warning("Empty devices.yaml")
                return

            for device_conf in config.get("devices", []):
                device_id = device_conf["id"]
                self.devices[device_id] = device_conf

                # Registrar parámetros por dispositivo
                param_file = device_conf.get("parameter_file") or "parameters/default.parameters.json"
                parameter_registry.register_device(device_id, param_file)
                
                # Init Client
                self.clients[device_id] = self._build_client(device_conf)
                
                # Resolve critical parameters
                crit_ids = device_conf.get("critical_parameters", [])
                addresses_by_type: Dict[str, Set[int]] = {}
                for pid in crit_ids:
                    addr = parameter_registry.get_address(pid, device_id=device_id)
                    reg_type = parameter_registry.get_register_type(pid, device_id=device_id)
                    if addr is not None:
                        addresses_by_type.setdefault(reg_type, set()).add(addr)
                    else:
                        logger.warning(f"Critical parameter {pid} not found in registry for device {device_id}")

                # Normalizar a listas ordenadas
                self.critical_addresses[device_id] = {
                    rtype: sorted(list(addrs)) for rtype, addrs in addresses_by_type.items()
                }
                
            logger.info(f"Loaded {len(self.devices)} devices configuration.")
        except Exception as e:
            logger.error(f"Error loading devices.yaml: {e}")

    def apply_device_config(self, device_conf: Dict) -> bool:
        device_id = device_conf.get("id")
        if not device_id or device_id not in self.devices:
            logger.warning("Cannot apply runtime config for unknown device %s", device_id)
            return False

        current_conf = self.devices[device_id]
        next_conf = {**current_conf, **device_conf}

        host_changed = current_conf.get("host") != next_conf.get("host")
        port_changed = current_conf.get("port", 502) != next_conf.get("port", 502)
        needs_new_client = host_changed or port_changed or device_id not in self.clients

        self.devices[device_id] = next_conf

        if not needs_new_client:
            logger.info("Updated runtime metadata for device %s", device_id)
            return True

        old_client = self.clients.get(device_id)
        self.clients[device_id] = self._build_client(next_conf)

        if old_client is not None:
            try:
                old_client.close()
            except Exception as exc:
                logger.warning("Error closing client for %s: %s", device_id, exc)

        self._consecutive_failures[device_id] = 0
        self._connected[device_id] = False
        self._last_ok_ts[device_id] = None
        self._publish_connection_state(device_id)

        logger.info(
            "Applied runtime connection update for %s -> %s:%s",
            device_id,
            next_conf.get("host"),
            next_conf.get("port", 502),
        )
        return True

    def optimize_blocks(self, addresses: List[int]) -> List[Tuple[int, int]]:
        """
        Agrupa direcciones individuales en bloques de lectura Modbus para eficiencia.
        Ejemplo: [10, 11, 12, 20] -> [(10, 3), (20, 1)]
        """
        if not addresses:
            return []
        
        sorted_addr = sorted(list(set(addresses)))
        blocks = []
        
        current_start = sorted_addr[0]
        current_count = 1
        last_addr = sorted_addr[0]
        
        # Gap máximo permitido para agrupar en una sola petición
        MAX_GAP = 20 
        # Tamaño máximo de bloque Modbus (usualmente 125 registros)
        MAX_BLOCK_SIZE = 100 
        
        for addr in sorted_addr[1:]:
            gap = addr - last_addr
            new_count_if_merged = addr - current_start + 1
            
            if gap <= MAX_GAP and new_count_if_merged <= MAX_BLOCK_SIZE:
                current_count = new_count_if_merged
            else:
                blocks.append((current_start, current_count))
                current_start = addr
                current_count = 1
            last_addr = addr
            
        blocks.append((current_start, current_count))
        return blocks

    def _is_signed_register_parameter(self, device_id: str, param_id: str) -> bool:
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

    @staticmethod
    def _decode_signed_register_value(raw_value: int) -> int:
        return raw_value - 0x10000 if raw_value >= 0x8000 else raw_value

    @staticmethod
    def _encode_signed_register_value(raw_value: int) -> int:
        return raw_value & 0xFFFF

    @staticmethod
    def _scale_raw_parameter_value(device_id: str, param_id: str, raw_value: int) -> float:
        scale_factor = parameter_registry.get_scale_factor(param_id, device_id=device_id)
        if scale_factor == 0:
            scale_factor = 1
        return raw_value / scale_factor

    @staticmethod
    def _resolve_control_word_mapping(source_raw: int) -> Tuple[str, int] | None:
        for start, end, param_id, base in TRIP_RESET_CONTROL_WORD_MAPPINGS:
            if start <= source_raw <= end:
                return param_id, source_raw - base
        return None

    async def start(self):
        if not self.devices:
            self.load_config()
        # Publish initial OFF state so the frontend can show OFF immediately on first subscribe
        for did in self.devices.keys():
            self._consecutive_failures.setdefault(did, 0)
            self._connected.setdefault(did, False)
            self._last_ok_ts.setdefault(did, None)
            self._publish_connection_state(did)
        self.running = True
        self._start_loop_task()

    async def stop(self):
        self.running = False
        loop_task = self._loop_task
        self._loop_task = None
        if loop_task is not None and not loop_task.done():
            loop_task.cancel()
            try:
                await loop_task
            except asyncio.CancelledError:
                pass
        # Close connections
        for client in self.clients.values():
            client.close()
        logger.info("Modbus Engine Stopped")

    async def refresh_comms(self) -> Dict[str, Any]:
        """
        Rebuilds all Modbus TCP clients and restarts the polling task if needed.
        This is intended for long device disconnects where the TCP/client state can
        stay stale after the PLC or WAGO hardware is plugged back in.
        """
        if not self.devices:
            self.load_config()

        refreshed_devices: List[str] = []

        for device_id, device_conf in list(self.devices.items()):
            old_client = self.clients.get(device_id)
            self.clients[device_id] = self._build_client(device_conf)

            if old_client is not None:
                try:
                    old_client.close()
                except Exception as exc:
                    logger.warning("Error closing client during comms refresh for %s: %s", device_id, exc)

            self._consecutive_failures[device_id] = 0
            self._connected[device_id] = False
            self._last_ok_ts[device_id] = None
            self._publish_connection_state(device_id)
            refreshed_devices.append(device_id)

        if self.running:
            await self._restart_loop_task("comms refresh")

        logger.warning("Comms refresh rebuilt %d Modbus client(s).", len(refreshed_devices))
        return {
            "status": "accepted",
            "running": self.running,
            "device_count": len(refreshed_devices),
            "devices": refreshed_devices,
        }

    async def _read_device(self, device_id: str):
        client = self.clients.get(device_id)
        if not client:
            self._mark_cycle_result(device_id, ok=False)
            return
        device_unit = self._get_device_unit_id(device_id)

        blocks_attempted = 0
        blocks_ok = 0
        blocks_none = 0

        critical_blocks_attempted = 0
        critical_blocks_ok = 0
        critical_blocks_none = 0
        try:
            # 1. Combine Critical + OnDemand
            critical_by_type = self.critical_addresses.get(device_id, {})
            critical_lists: Dict[str, List[int]] = {
                rtype: (addrs if isinstance(addrs, list) else sorted(list(addrs)))
                for rtype, addrs in (critical_by_type or {}).items()
            }

            def _is_critical_block(rtype: str, start: int, count: int) -> bool:
                crit = critical_lists.get(rtype) or []
                if not crit:
                    return False
                end_exclusive = start + count
                idx = bisect_left(crit, start)
                return idx < len(crit) and crit[idx] < end_exclusive

            addresses_by_type: Dict[str, Set[int]] = {
                rtype: set(addrs) for rtype, addrs in critical_by_type.items()
            }

            # Add OnDemand addresses (por parámetro)
            active_params = state_manager.get_active_parameters(device_id)
            for pid in active_params:
                addr = parameter_registry.get_address(pid, device_id=device_id)
                reg_type = parameter_registry.get_register_type(pid, device_id=device_id)
                if addr is not None:
                    addresses_by_type.setdefault(reg_type, set()).add(addr)

            if not any(addresses_by_type.values()):
                self._mark_cycle_result(device_id, ok=False)
                return

            # 2. Optimize por tipo y leer
            device_data = {}

            for reg_type, addr_set in addresses_by_type.items():
                if not addr_set:
                    continue

                blocks = self.optimize_blocks(list(addr_set))

                for start, count in blocks:
                    blocks_attempted += 1
                    is_critical = _is_critical_block(reg_type, start, count)
                    if is_critical:
                        critical_blocks_attempted += 1
                    registers = None
                    # Note: 'unit' parameter es equivalente a 'device_id' en pymodbus moderno
                    if reg_type == "holding":
                        registers = await client.read_holding_registers(
                            start, count, unit=device_unit
                        )
                    elif reg_type == "input":
                        registers = await client.read_input_registers(
                            start, count, unit=device_unit
                        )
                    elif reg_type == "coil":
                        registers = await client.read_coils(start, count, unit=device_unit)
                    elif reg_type == "discrete":
                        registers = await client.read_discrete_inputs(
                            start, count, unit=device_unit
                        )

                    if registers is None:
                        blocks_none += 1
                        if is_critical:
                            critical_blocks_none += 1
                        continue

                    blocks_ok += 1
                    if is_critical:
                        critical_blocks_ok += 1
                    if registers:
                        for i, val in enumerate(registers):
                            addr = start + i
                            pid = parameter_registry.get_id_by_address(addr, reg_type, device_id=device_id)
                            if pid:
                                # Apply scaling: divide by scale_factor to get the actual value
                                scale_factor = parameter_registry.get_scale_factor(pid, device_id=device_id)
                                if scale_factor == 0:
                                    scale_factor = 1  # Avoid division by zero

                                raw_value = val
                                if self._is_signed_register_parameter(device_id, pid):
                                    raw_value = self._decode_signed_register_value(raw_value)

                                scaled_value = raw_value / scale_factor
                                device_data[pid] = scaled_value

            # 3. Update State
            if device_data:
                # logger.info(f"READ {device_id}: Sucesfull")
                state_manager.update(device_id, device_data)

            # Update connection state after the cycle (even if device_data is empty)
            if critical_blocks_attempted > 0:
                cycle_ok = critical_blocks_ok == critical_blocks_attempted
            else:
                cycle_ok = blocks_ok > 0

            self._mark_cycle_result(device_id, ok=cycle_ok)
                
        except Exception as e:
            logger.error(f"Error reading device {device_id}: {e}")
            self._mark_cycle_result(device_id, ok=False)

    async def _write_parameter_direct(self, client, device_id: str, param_id: str, value: any, *, publish_state: bool = True):
        address = parameter_registry.get_address(param_id, device_id=device_id)
        register_type = parameter_registry.get_register_type(param_id, device_id=device_id)
        device_unit = self._get_device_unit_id(device_id)

        if address is None:
            raise ValueError(f"Parameter {param_id} not mapped to an address")

        # Apply scaling: multiply by scale_factor to get the raw register value
        scale_factor = parameter_registry.get_scale_factor(param_id, device_id=device_id)
        if scale_factor == 0:
            scale_factor = 1

        scaled_value = value * scale_factor

        # Convert scaled value to int (assuming raw Modbus register)
        # Use round to avoid precision issues (e.g. 49.9999 -> 50)
        int_value = int(round(scaled_value))

        if self._is_signed_register_parameter(device_id, param_id) and int_value < 0:
            int_value = self._encode_signed_register_value(int_value)

        if register_type in ["input", "discrete"]:
            raise ValueError(f"Parameter {param_id} ({register_type}) is read-only")
        if register_type == "coil":
            coil_value = bool(int_value)
            write_attempts = [
                ("multiple", lambda: client.write_coils(address, [coil_value], unit=device_unit)),
                ("single", lambda: client.write_coil(address, coil_value, unit=device_unit)),
            ]
            if self._get_coil_write_mode(device_id) == "single":
                write_attempts.reverse()

            success = False
            verified_method: str | None = None
            for method_name, writer in write_attempts:
                success = await writer()
                if not success:
                    continue

                readback: int | None = None
                try:
                    read_values = await client.read_coils(address, 1, unit=device_unit)
                    if read_values:
                        readback = int(read_values[0])
                except Exception:
                    logger.debug(
                        "Coil write readback failed for %s.%s via %s",
                        device_id,
                        param_id,
                        method_name,
                        exc_info=True,
                    )

                if readback is None or readback == int(coil_value):
                    verified_method = method_name
                    logger.info(
                        "WRITE %s - %s (%s) = %s via %s coil write (readback=%s)",
                        device_id,
                        param_id,
                        address,
                        int(coil_value),
                        method_name,
                        readback,
                    )
                    break

                logger.warning(
                    "WRITE %s - %s (%s) via %s reported success but readback=%s; trying next method",
                    device_id,
                    param_id,
                    address,
                    method_name,
                    readback,
                )
                success = False

            if verified_method is None and success:
                verified_method = method_name
        else:
            # holding (por defecto)
            success = await client.write_register(address, int_value, unit=device_unit)

        if not success:
            raise ConnectionError(
                f"Modbus write rejected for {device_id}.{param_id} at address {address}"
            )

        logger.info(f"WRITE {device_id} - {param_id} ({address}) = {value} (scaled to {int_value})")

        if publish_state:
            # Optimistic update of state with the original value (not scaled)
            state_manager.update(device_id, {param_id: value})

        return True

    async def _write_parameter_raw(self, client, device_id: str, param_id: str, raw_value: int, *, publish_state: bool = True):
        address = parameter_registry.get_address(param_id, device_id=device_id)
        register_type = parameter_registry.get_register_type(param_id, device_id=device_id)
        device_unit = self._get_device_unit_id(device_id)

        if address is None:
            raise ValueError(f"Parameter {param_id} not mapped to an address")
        if register_type != "holding":
            raise ValueError(f"Parameter {param_id} ({register_type}) does not support raw register writes")

        int_value = int(raw_value)
        if self._is_signed_register_parameter(device_id, param_id) and int_value < 0:
            int_value = self._encode_signed_register_value(int_value)

        success = await client.write_register(address, int_value, unit=device_unit)
        if not success:
            raise ConnectionError(
                f"Modbus raw write rejected for {device_id}.{param_id} at address {address}"
            )

        logger.info(f"RAW WRITE {device_id} - {param_id} ({address}) = {int_value}")

        if publish_state:
            state_manager.update(
                device_id,
                {param_id: self._scale_raw_parameter_value(device_id, param_id, int_value)},
            )

        return True

    async def _read_single_register_value(self, client, device_id: str, param_id: str):
        address = parameter_registry.get_address(param_id, device_id=device_id)
        register_type = parameter_registry.get_register_type(param_id, device_id=device_id)
        device_unit = self._get_device_unit_id(device_id)

        if address is None or register_type != "holding":
            return None

        registers = await client.read_holding_registers(address, 1, unit=device_unit)
        if not registers:
            return None

        raw_value = registers[0]
        if self._is_signed_register_parameter(device_id, param_id):
            return self._decode_signed_register_value(raw_value)

        return raw_value

    async def _read_trip_status_snapshot(self, client, device_id: str) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        for param_id in TRIP_STATUS_PARAM_IDS:
            try:
                snapshot[param_id] = await self._read_single_register_value(client, device_id, param_id)
            except Exception as exc:
                snapshot[param_id] = f"ERR:{exc}"
        return snapshot

    async def pulse_trip_reset(self, device_id: str, pulse_ms: int = 500) -> Dict[str, Any]:
        client = self.clients.get(device_id)
        if not client:
            raise ValueError(f"Device {device_id} not found")

        pulse_ms = max(50, min(int(pulse_ms), 5000))
        pulse_s = pulse_ms / 1000.0

        before_snapshot = await self._read_trip_status_snapshot(client, device_id)
        logger.info("TRIP RESET PRE %s %s", device_id, before_snapshot)

        source_raw = await self._read_single_register_value(
            client, device_id, TRIP_RESET_SOURCE_PARAM_ID
        )
        if source_raw is None:
            raise ConnectionError(
                f"Unable to read {TRIP_RESET_SOURCE_PARAM_ID} before trip reset"
            )

        mapping = self._resolve_control_word_mapping(int(source_raw))
        if mapping is not None:
            control_param_id, bit = mapping
            current_word = await self._read_single_register_value(
                client, device_id, control_param_id
            )
            if current_word is None:
                raise ConnectionError(
                    f"Unable to read {control_param_id} before trip reset"
                )

            mask = 1 << bit
            cleared_word = int(current_word) & ~mask
            set_word = cleared_word | mask

            if int(current_word) != cleared_word:
                logger.warning(
                    "Trip reset bit %s already high in %s for %s; forcing low before pulse",
                    bit,
                    control_param_id,
                    device_id,
                )
                await self._write_parameter_raw(
                    client, device_id, control_param_id, cleared_word
                )
                await asyncio.sleep(min(0.1, pulse_s / 2))

            try:
                await self._write_parameter_raw(client, device_id, control_param_id, set_word)
                await asyncio.sleep(pulse_s)
            finally:
                await self._write_parameter_raw(
                    client, device_id, control_param_id, cleared_word
                )

            after_snapshot = await self._read_trip_status_snapshot(client, device_id)
            logger.info("TRIP RESET POST %s %s", device_id, after_snapshot)
            return {
                "strategy": "control_word",
                "source_raw": int(source_raw),
                "control_param_id": control_param_id,
                "bit": bit,
                "pulse_ms": pulse_ms,
            }

        try:
            await self._write_parameter_raw(
                client,
                device_id,
                TRIP_RESET_SOURCE_PARAM_ID,
                TRIP_RESET_FIXED_SET_SOURCE_RAW,
            )
            await asyncio.sleep(pulse_s)
        finally:
            await self._write_parameter_raw(
                client, device_id, TRIP_RESET_SOURCE_PARAM_ID, int(source_raw)
            )

        after_snapshot = await self._read_trip_status_snapshot(client, device_id)
        logger.info("TRIP RESET POST %s %s", device_id, after_snapshot)
        return {
            "strategy": "source_override",
            "source_raw": int(source_raw),
            "pulse_ms": pulse_ms,
        }

    async def _is_legacy_ethernet_speed_mode(self, client, device_id: str) -> bool:
        if device_id != LEGACY_ETHERNET_SPEED_DEVICE_ID:
            return False

        for param_id, expected in ETHERNET_MODE_CONTROL_PARAMS.items():
            current = await self._read_single_register_value(client, device_id, param_id)
            if current != expected:
                return False

        return True

    async def _ensure_ethernet_reference_path(self, client, device_id: str):
        await self._write_parameter_direct(
            client,
            device_id,
            ETHERNET_REF1_SELECTOR_PARAM_ID,
            CONTROL_FLAG_SET_VALUE,
            publish_state=False,
        )
        await self._write_parameter_direct(
            client,
            device_id,
            ETHERNET_REF2_SELECTOR_PARAM_ID,
            CONTROL_FLAG_CLEAR_VALUE,
            publish_state=False,
        )
        await self._write_parameter_direct(
            client,
            device_id,
            ETHERNET_REF3_SELECTOR_PARAM_ID,
            CONTROL_FLAG_CLEAR_VALUE,
            publish_state=False,
        )
        await self._write_parameter_direct(
            client,
            device_id,
            ETHERNET_REF4_SELECTOR_PARAM_ID,
            CONTROL_FLAG_CLEAR_VALUE,
            publish_state=False,
        )

        await self._write_parameter_direct(
            client,
            device_id,
            ETHERNET_POINTER_SOURCE_PARAM_ID,
            ETHERNET_POINTER_SOURCE_VALUE,
            publish_state=False,
        )
        await self._write_parameter_direct(
            client,
            device_id,
            ETHERNET_POINTER_SCALE_PARAM_ID,
            ETHERNET_POINTER_SCALE_VALUE,
            publish_state=False,
        )

        await self._write_parameter_direct(
            client,
            device_id,
            CLAMP_ZERO_REFERENCE_PARAM_ID,
            CONTROL_FLAG_CLEAR_VALUE,
            publish_state=False,
        )

        backup_source = await self._read_single_register_value(
            client, device_id, BACKUP_SPEED_REFERENCE_PARAM_ID
        )
        if backup_source == 0:
            logger.info(
                "Ethernet reference source selected for %s with no backup; setting %s=%s first",
                device_id,
                BACKUP_SPEED_REFERENCE_PARAM_ID,
                KEYPAD_SPEED_REFERENCE_SOURCE_VALUE,
            )
            await self._write_parameter_direct(
                client,
                device_id,
                BACKUP_SPEED_REFERENCE_PARAM_ID,
                KEYPAD_SPEED_REFERENCE_SOURCE_VALUE,
                publish_state=False,
            )

        await self._write_parameter_direct(
            client,
            device_id,
            ETHERNET_REFERENCE_SOURCE_PARAM_ID,
            ETHERNET_REFERENCE_SOURCE_VALUE,
            publish_state=False,
        )

    async def write_parameter(self, device_id: str, param_id: str, value: any):
        client = self.clients.get(device_id)
        if not client:
            raise ValueError(f"Device {device_id} not found")

        try:
            if (
                device_id == LEGACY_ETHERNET_SPEED_DEVICE_ID
                and param_id == ETHERNET_REFERENCE_SOURCE_PARAM_ID
                and int(round(float(value))) in {
                    LEGACY_ETHERNET_REFERENCE_SOURCE_VALUE,
                    ETHERNET_REFERENCE_SOURCE_VALUE,
                }
                and await self._is_legacy_ethernet_speed_mode(client, device_id)
            ):
                logger.info(
                    "Ethernet mode source selection detected for %s; enforcing Pointer 1 -> %s",
                    device_id,
                    ETHERNET_REFERENCE_PARAM_ID,
                )
                await self._ensure_ethernet_reference_path(client, device_id)
                state_manager.update(
                    device_id,
                    {
                        ETHERNET_REF1_SELECTOR_PARAM_ID: CONTROL_FLAG_SET_VALUE,
                        ETHERNET_REF2_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
                        ETHERNET_REF3_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
                        ETHERNET_REF4_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
                        ETHERNET_POINTER_SOURCE_PARAM_ID: ETHERNET_POINTER_SOURCE_VALUE,
                        ETHERNET_POINTER_SCALE_PARAM_ID: ETHERNET_POINTER_SCALE_VALUE,
                        CLAMP_ZERO_REFERENCE_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
                        ETHERNET_REFERENCE_SOURCE_PARAM_ID: ETHERNET_REFERENCE_SOURCE_VALUE,
                    },
                )
                return True

            if (
                param_id == LEGACY_FIXED_REFERENCE_PARAM_ID
                and await self._is_legacy_ethernet_speed_mode(client, device_id)
            ):
                logger.info(
                    "Legacy speed command detected in Ethernet mode for %s; routing %s -> %s",
                    device_id,
                    LEGACY_FIXED_REFERENCE_PARAM_ID,
                    ETHERNET_REFERENCE_PARAM_ID,
                )
                await self._ensure_ethernet_reference_path(client, device_id)
                await self._write_parameter_direct(client, device_id, ETHERNET_REFERENCE_PARAM_ID, value)
                state_manager.update(
                    device_id,
                    {
                        LEGACY_FIXED_REFERENCE_PARAM_ID: value,
                        ETHERNET_REF1_SELECTOR_PARAM_ID: CONTROL_FLAG_SET_VALUE,
                        ETHERNET_REF2_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
                        ETHERNET_REF3_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
                        ETHERNET_REF4_SELECTOR_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
                        ETHERNET_POINTER_SOURCE_PARAM_ID: ETHERNET_POINTER_SOURCE_VALUE,
                        ETHERNET_POINTER_SCALE_PARAM_ID: ETHERNET_POINTER_SCALE_VALUE,
                        CLAMP_ZERO_REFERENCE_PARAM_ID: CONTROL_FLAG_CLEAR_VALUE,
                        ETHERNET_REFERENCE_SOURCE_PARAM_ID: ETHERNET_REFERENCE_SOURCE_VALUE,
                    },
                )
                return True

            return await self._write_parameter_direct(client, device_id, param_id, value)
        except Exception as e:
            logger.error(f"Error writing to device {device_id}: {e}")
            raise e

    async def _loop(self):
        logger.info("Modbus Loop active")
        cycle_count = 0
        # Removed Heartbeat logging
        while self.running:
            current_task = asyncio.current_task()
            if self._loop_task is not None and self._loop_task is not current_task:
                logger.warning("Stopping stale Modbus polling loop instance.")
                return

            task_by_device: Dict[str, asyncio.Task] = {}
            try:
                cycle_count += 1
                start_time = asyncio.get_event_loop().time()

                # Create tasks for all devices
                task_by_device = {did: asyncio.create_task(self._read_device(did)) for did in self.devices}
                if task_by_device:
                    # Timeout global de ciclo para evitar que un dispositivo bloquee a todos.
                    # Usamos asyncio.wait para NO cancelar automáticamente las tareas (wait_for(gather) sí lo hace).
                    timeout_s = settings.MODBUS_POLL_INTERVAL + 22.0
                    done, pending = await asyncio.wait(task_by_device.values(), timeout=timeout_s)

                    if pending:
                        pending_devices = [did for did, t in task_by_device.items() if t in pending]
                        # Cancel pending tasks + mark FAIL
                        for did in pending_devices:
                            try:
                                task_by_device[did].cancel()
                            except Exception:
                                pass
                            self._mark_cycle_result(did, ok=False)

                        # Drain cancellations/exceptions to avoid overlap across cycles
                        await self._drain_device_tasks(
                            task_by_device,
                            context="Timed-out Modbus cycle tasks",
                        )
                    else:
                        # No pending tasks, but still mark FAIL if any task ended cancelled/exceptionally
                        for did, t in task_by_device.items():
                            if t.cancelled():
                                self._mark_cycle_result(did, ok=False)
                                continue
                            exc = t.exception()
                            if exc is not None:
                                self._mark_cycle_result(did, ok=False)
                
                # Calculate sleep to maintain interval
                elapsed = asyncio.get_event_loop().time() - start_time
                sleep_time = max(0.1, settings.MODBUS_POLL_INTERVAL - elapsed)
                await asyncio.sleep(sleep_time)
            except _RestartPollingLoop as exc:
                logger.error("%s", exc)
                if self.running and self._loop_task is current_task:
                    self._loop_task = asyncio.create_task(self._loop())
                return
            except Exception as e:
                logger.error(f"Error in Modbus Loop: {e}")
                # Ensure any tasks started in this cycle are drained
                try:
                    await self._drain_device_tasks(
                        task_by_device,
                        context="Modbus cycle cleanup tasks",
                    )
                except _RestartPollingLoop as exc:
                    logger.error("%s", exc)
                    if self.running and self._loop_task is current_task:
                        self._loop_task = asyncio.create_task(self._loop())
                    return
                except Exception:
                    pass
                await asyncio.sleep(1)

modbus_manager = ModbusManager()
