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

    async def read_input_registers(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """Lee input registers (función 4)"""
        async with self.lock:
            if not self.client.connected:
                await self.client.connect()
                if not self.client.connected:
                    logger.debug(f"Connection failed to {self.host}:{self.port}")
                    return None

            try:
                result = await self.client.read_input_registers(start, count=count, device_id=unit)
                if result.isError():
                    logger.error(f"Modbus error reading INPUT {self.host} @ {start}: {result}")
                    return None
                return result.registers
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Exception reading input registers {self.host}: {e}")
                self.client.close()
                return None

    async def read_coils(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """Lee coils (función 1). Retorna 0/1."""
        async with self.lock:
            if not self.client.connected:
                await self.client.connect()
                if not self.client.connected:
                    logger.debug(f"Connection failed to {self.host}:{self.port}")
                    return None

            try:
                result = await self.client.read_coils(start, count=count, device_id=unit)
                if result.isError():
                    logger.error(f"Modbus error reading COILS {self.host} @ {start}: {result}")
                    return None
                # result.bits puede incluir más bits; limitamos al count solicitado
                return [int(bit) for bit in result.bits[:count]]
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Exception reading coils {self.host}: {e}")
                self.client.close()
                return None

    async def read_discrete_inputs(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """Lee discrete inputs (función 2). Retorna 0/1."""
        async with self.lock:
            if not self.client.connected:
                await self.client.connect()
                if not self.client.connected:
                    logger.debug(f"Connection failed to {self.host}:{self.port}")
                    return None

            try:
                result = await self.client.read_discrete_inputs(start, count=count, device_id=unit)
                if result.isError():
                    logger.error(f"Modbus error reading DISCRETE {self.host} @ {start}: {result}")
                    return None
                return [int(bit) for bit in result.bits[:count]]
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Exception reading discrete inputs {self.host}: {e}")
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

    async def write_coil(self, address: int, value: int | bool, unit: int = 1) -> bool:
        """Escribe un coil (función 5)"""
        async with self.lock:
            if not self.client.connected:
                await self.client.connect()
                if not self.client.connected:
                    return False

            try:
                result = await self.client.write_coil(address, bool(value), device_id=unit)
                if result.isError():
                    logger.error(f"Modbus error writing COIL {self.host} @ {address}: {result}")
                    return False
                return True
            except Exception as e:
                logger.error(f"Exception writing coil {self.host}: {e}")
                self.client.close()
                return False
