from pymodbus.client import AsyncModbusTcpClient
import asyncio
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)

class ModbusClientWrapper:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        # Timeout más generoso y reintentos internos
        self.client = AsyncModbusTcpClient(host, port=port, timeout=3, retries=3)
        self.lock = asyncio.Lock()

    async def connect(self):
        if not self.client.connected:
            await self.client.connect()

    def close(self):
        """Cierra la conexión de manera segura"""
        try:
            self.client.close()
        except Exception as e:
            logger.warning(f"Error closing client {self.host}: {e}")

    async def read_holding_registers(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """Lee registros de forma thread-safe"""
        async with self.lock:
            # Auto-reconnect logic
            if not self.client.connected:
                await self.client.connect()
                if not self.client.connected:
                    # Log menos ruidoso si está caído
                    logger.debug(f"Connection failed to {self.host}:{self.port}")
                    return None
            
            try:
                # Nota: PyModbus moderno usa 'slave' o 'device_id' dependiendo de la versión.
                # Si falla con device_id, prueba con slave=unit
                result = await self.client.read_holding_registers(start, count=count, device_id=unit)
                
                if result.isError():
                    logger.error(f"Modbus error reading {self.host} @ {start}: {result}")
                    return None
                return result.registers
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Exception reading modbus {self.host}: {e}")
                self.client.close() 
                return None

    async def write_register(self, address: int, value: int, unit: int = 1) -> bool:
        """Escribe un registro de forma thread-safe"""
        async with self.lock:
            if not self.client.connected:
                await self.client.connect()
                if not self.client.connected:
                    return False
            
            try:
                result = await self.client.write_register(address, value, device_id=unit)
                if result.isError():
                    logger.error(f"Modbus error writing {self.host} @ {address}: {result}")
                    return False
                return True
            except Exception as e:
                logger.error(f"Exception writing modbus {self.host}: {e}")
                self.client.close()
                return False
