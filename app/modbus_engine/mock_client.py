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
        # Slot settings ensure every parameter id gets its own band of values
        self._slot_size = 800          # Distance between band centers
        self._slot_count = 60          # Number of available bands
        self._slot_start = -20000      # Lowest band center
        logger.info(f"INITIALIZED MOCK CLIENT for {host}:{port}")

    def _pid_anchor(self, pid: str) -> int:
        """
        Generates a deterministic band center based on the parameter id so
        values stay clearly separated in trends.
        """
        digits = "".join(ch for ch in pid if ch.isdigit())
        numeric_id = int(digits) if digits else sum(ord(ch) for ch in pid)
        base = self._slot_start + (numeric_id % self._slot_count) * self._slot_size
        # Keep within signed 16-bit range to emulate Modbus registers
        return max(-30000, min(30000, base))

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
                anchor = self._pid_anchor(pid)
                amplitude = self._slot_size * 0.35  # Fluctuación controlada por banda
                if param_def:
                    # Extraer rangos para generar datos realistas
                    ranges = param_def.get('range_numeric') or {}
                    if not isinstance(ranges, dict):
                        ranges = {}
                    min_val = ranges.get('min', 0)
                    max_val = ranges.get('max', 100)
                    span = max_val - min_val
                    if span == 0:
                        span = 100
                    # Ajustar la amplitud con el rango disponible, pero limitarla a la banda
                    amplitude = min(amplitude, span / 2.2)
                    amplitude = max(amplitude, self._slot_size * 0.1)

                # Generar un valor que oscila alrededor del ancla del parámetro
                wave = math.sin(self._counter + (address * 0.5)) * amplitude
                noise = random.uniform(-amplitude * 0.1, amplitude * 0.1)
                simulated_val = anchor + wave + noise
                value = int(simulated_val)
            else:
                # Si no conocemos el parámetro, devolvemos ruido aleatorio
                value = random.randint(0, 100)

            # Asegurar que sea un int válido para Modbus (16-bit signed o unsigned según config, aquí asumimos standard)
            results.append(value)

        return results

    async def read_input_registers(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        # Reutiliza la misma generación de holding registers
        return await self.read_holding_registers(start, count, unit)

    async def read_coils(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        if not self.connected:
            await self.connect()
        await asyncio.sleep(0.02)
        # Alternar valores para simular cambios de estado
        return [((start + i + int(self._counter * 10)) % 2) for i in range(count)]

    async def read_discrete_inputs(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        # Similar a coils pero con patrón diferente
        if not self.connected:
            await self.connect()
        await asyncio.sleep(0.02)
        return [((start + i + int(self._counter * 7)) % 2) for i in range(count)]

    async def write_register(self, address: int, value: int, unit: int = 1) -> bool:
        logger.info(f"MOCK WRITE @ {address} VAL: {value}")
        # Simular éxito
        await asyncio.sleep(0.1)
        return True

    async def write_coil(self, address: int, value: int | bool, unit: int = 1) -> bool:
        logger.info(f"MOCK WRITE COIL @ {address} VAL: {value}")
        await asyncio.sleep(0.05)
        return True

