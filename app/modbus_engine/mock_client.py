import asyncio
import math
import random
import logging
from typing import List, Optional
from app.core.config import parameter_registry

logger = logging.getLogger(__name__)

class MockModbusClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.connected = False
        self._counter = 0
        logger.info(f"INITIALIZED MOCK CLIENT for {host}:{port}")

    async def connect(self):
        self.connected = True
        logger.info(f"MOCK CONNECTED to {self.host}")

    def close(self):
        self.connected = False
        logger.info(f"MOCK DISCONNECTED from {self.host}")

    async def read_holding_registers(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """
        Genera datos simulados basados en parameters.json
        """
        if not self.connected:
            await self.connect()

        # Simular latencia de red
        await asyncio.sleep(0.05)

        results = []
        self._counter += 0.1  # Incremento para simular ondas

        for i in range(count):
            address = start + i
            # Buscar qué parámetro es esta dirección
            pid = parameter_registry.get_id_by_address(address)
            
            value = 0
            if pid:
                param_def = parameter_registry.get_parameter(pid)
                if param_def:
                    # Extraer rangos para generar datos realistas
                    ranges = param_def.get('range_numeric', {})
                    min_val = ranges.get('min', 0)
                    max_val = ranges.get('max', 100)
                    
                    # Generar un valor que oscila entre min y max
                    # Usamos sin() para que se vea bonito en las gráficas
                    span = max_val - min_val
                    # Evitar division por cero si min == max
                    if span == 0:
                        span = 100 
                        
                    mid = min_val + (span / 2)
                    amplitude = span / 2.2  # Un poco menos del borde
                    
                    # Wave + Random noise
                    wave = math.sin(self._counter + (address * 0.5)) * amplitude
                    noise = random.uniform(-amplitude * 0.05, amplitude * 0.05)
                    
                    simulated_val = mid + wave + noise
                    
                    # Convertir a entero (como lo haría Modbus)
                    value = int(simulated_val)
            else:
                # Si no conocemos el parámetro, devolvemos ruido aleatorio
                value = random.randint(0, 100)
            
            # Asegurar que sea un int válido para Modbus (16-bit signed o unsigned según config, aquí asumimos standard)
            results.append(value)

        return results

    async def write_register(self, address: int, value: int, unit: int = 1) -> bool:
        logger.info(f"MOCK WRITE @ {address} VAL: {value}")
        # Simular éxito
        await asyncio.sleep(0.1)
        return True

