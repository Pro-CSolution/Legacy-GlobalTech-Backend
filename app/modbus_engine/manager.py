import asyncio
import yaml
import logging
from typing import List, Dict, Tuple, Set
from app.core.config import settings, parameter_registry
from app.modbus_engine.client import ModbusClientWrapper
from app.modbus_engine.mock_client import MockModbusClient
from app.modbus_engine.state import state_manager

logger = logging.getLogger(__name__)

class ModbusManager:
    def __init__(self):
        self.devices: Dict[str, Dict] = {} # Config data
        self.clients: Dict[str, ModbusClientWrapper] = {}
        self.critical_addresses: Dict[str, List[int]] = {} # device_id -> [addr1, addr2]
        self.running = False

    def load_config(self):
        try:
            file_path = "config/devices.yaml"
            with open(file_path, "r") as f:
                config = yaml.safe_load(f)
                
            if not config:
                logger.warning("Empty devices.yaml")
                return

            for device_conf in config.get("devices", []):
                device_id = device_conf["id"]
                self.devices[device_id] = device_conf
                
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
                addresses = []
                for pid in crit_ids:
                    addr = parameter_registry.get_address(pid)
                    if addr is not None:
                        addresses.append(addr)
                    else:
                        logger.warning(f"Critical parameter {pid} not found in registry for device {device_id}")
                self.critical_addresses[device_id] = sorted(list(set(addresses)))
                
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
            return

        try:
            # 1. Combine Critical + OnDemand
            addresses = set(self.critical_addresses.get(device_id, []))
            
            # Add OnDemand addresses
            active_params = state_manager.get_active_parameters(device_id)
            for pid in active_params:
                addr = parameter_registry.get_address(pid)
                if addr is not None:
                    addresses.add(addr)
            
            if not addresses:
                return

            # 2. Optimize
            blocks = self.optimize_blocks(list(addresses))
            
            # 3. Execute Reads
            device_data = {}
            
            for start, count in blocks:
                # Note: 'unit' parameter is deprecated in newer pymodbus, replaced by 'slave' or 'device_id'
                # We use 'slave' which is compatible with the wrapper
                registers = await client.read_holding_registers(start, count, unit=1)
                if registers:
                    for i, val in enumerate(registers):
                        addr = start + i
                        pid = parameter_registry.get_id_by_address(addr)
                        if pid:
                            # TODO: Implementar scaling/factores según parameters.json si es necesario
                            # Por ahora guardamos el valor raw
                            device_data[pid] = val
            
            # 4. Update State
            if device_data:
                # Logging explícito de los valores leídos para depuración (MANTENIDO)
                logger.info(f"READ {device_id}: {device_data}")
                state_manager.update(device_id, device_data)
                
        except Exception as e:
            logger.error(f"Error reading device {device_id}: {e}")

    async def _loop(self):
        logger.info("Modbus Loop active")
        # Removed Heartbeat logging
        while self.running:
            try:
                start_time = asyncio.get_event_loop().time()
                
                # Create tasks for all devices
                tasks = [self._read_device(did) for did in self.devices]
                if tasks:
                    # Timeout global de ciclo para evitar que un dispositivo bloquee a todos
                    # Si el poll interval es 1.0s, damos un margen de seguridad
                    await asyncio.wait_for(asyncio.gather(*tasks), timeout=settings.MODBUS_POLL_INTERVAL + 22.0)
                
                # Calculate sleep to maintain interval
                elapsed = asyncio.get_event_loop().time() - start_time
                sleep_time = max(0.1, settings.MODBUS_POLL_INTERVAL - elapsed)
                await asyncio.sleep(sleep_time)
                
            except asyncio.TimeoutError:
                logger.warning("Modbus cycle timed out - device(s) too slow")
            except Exception as e:
                logger.error(f"Error in Modbus Loop: {e}")
                await asyncio.sleep(1)

modbus_manager = ModbusManager()
