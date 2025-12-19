import asyncio
import yaml
import logging
from bisect import bisect_left
from typing import List, Dict, Tuple, Set
from app.core.config import settings, parameter_registry
from app.core.paths import resolve_resource
from app.core.utils import utc_now
from app.modbus_engine.client import ModbusClientWrapper
from app.modbus_engine.mock_client import MockModbusClient
from app.modbus_engine.state import state_manager

logger = logging.getLogger(__name__)

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
                if settings.USE_MOCK_DATA:
                    logger.warning(f"Using MOCK client for device {device_id}")
                    self.clients[device_id] = MockModbusClient(
                        host=device_conf["host"],
                        port=device_conf.get("port", 502)
                    )
                else:
                    self.clients[device_id] = ModbusClientWrapper(
                        host=device_conf["host"],
                        port=device_conf.get("port", 502)
                    )
                
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

    async def start(self):
        self.load_config()
        # Publish initial OFF state so the frontend can show OFF immediately on first subscribe
        for did in self.devices.keys():
            self._consecutive_failures.setdefault(did, 0)
            self._connected.setdefault(did, False)
            self._last_ok_ts.setdefault(did, None)
            self._publish_connection_state(did)
        self.running = True
        logger.info("Starting Modbus Engine Loop...")
        asyncio.create_task(self._loop())

    async def stop(self):
        self.running = False
        # Close connections
        for client in self.clients.values():
            client.close()
        logger.info("Modbus Engine Stopped")

    async def _read_device(self, device_id: str):
        client = self.clients.get(device_id)
        if not client:
            self._mark_cycle_result(device_id, ok=False)
            return

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
                        registers = await client.read_holding_registers(start, count, unit=1)
                    elif reg_type == "input":
                        registers = await client.read_input_registers(start, count, unit=1)
                    elif reg_type == "coil":
                        registers = await client.read_coils(start, count, unit=1)
                    elif reg_type == "discrete":
                        registers = await client.read_discrete_inputs(start, count, unit=1)

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
                                
                                scaled_value = val / scale_factor
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

    async def write_parameter(self, device_id: str, param_id: str, value: any):
        client = self.clients.get(device_id)
        if not client:
            raise ValueError(f"Device {device_id} not found")

        address = parameter_registry.get_address(param_id, device_id=device_id)
        register_type = parameter_registry.get_register_type(param_id, device_id=device_id)

        if address is None:
             raise ValueError(f"Parameter {param_id} not mapped to an address")
        
        try:
            # Apply scaling: multiply by scale_factor to get the raw register value
            scale_factor = parameter_registry.get_scale_factor(param_id, device_id=device_id)
            if scale_factor == 0:
                scale_factor = 1

            scaled_value = value * scale_factor

            # Convert scaled value to int (assuming raw Modbus register)
            # Use round to avoid precision issues (e.g. 49.9999 -> 50)
            int_value = int(round(scaled_value))

            if register_type in ["input", "discrete"]:
                raise ValueError(f"Parameter {param_id} ({register_type}) is read-only")
            elif register_type == "coil":
                success = await client.write_coil(address, bool(int_value), unit=1)
            else:
                # holding (por defecto)
                success = await client.write_register(address, int_value, unit=1)
            
            # In some libraries write_register returns response obj or exception, 
            # assuming wrapper handles it or returns something truthy on success.
            
            logger.info(f"WRITE {device_id} - {param_id} ({address}) = {value} (scaled to {int_value})")

            # Optimistic update of state with the original value (not scaled)
            state_manager.update(device_id, {param_id: value})
            
            return True
        except Exception as e:
            logger.error(f"Error writing to device {device_id}: {e}")
            raise e

    async def _loop(self):
        logger.info("Modbus Loop active")
        cycle_count = 0
        # Removed Heartbeat logging
        while self.running:
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
                        await asyncio.gather(*task_by_device.values(), return_exceptions=True)
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
                
            except Exception as e:
                logger.error(f"Error in Modbus Loop: {e}")
                # Ensure any tasks started in this cycle are drained
                try:
                    if task_by_device:
                        await asyncio.gather(*task_by_device.values(), return_exceptions=True)
                except Exception:
                    pass
                await asyncio.sleep(1)

modbus_manager = ModbusManager()
