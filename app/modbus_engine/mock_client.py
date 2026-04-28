import asyncio
import logging
import math
import random
import re
from typing import Dict, List, Optional

from app.core.config import parameter_registry

logger = logging.getLogger(__name__)


class MockModbusClient:
    def __init__(self, host: str, port: int, device_id: str = "default"):
        self.host = host
        self.port = port
        self.device_id = device_id
        self.connected = False
        self._counter = 0.0
        self._register_overrides: Dict[int, int] = {}
        self._coil_overrides: Dict[int, int] = {}
        self._device_seed = sum(ord(ch) for ch in device_id)
        self._phase_offset = (self._device_seed % 29) / 7.0
        self._device_bias = ((self._device_seed % 11) - 5) / 50.0
        logger.info(
            "INITIALIZED MOCK CLIENT for %s:%s (%s)",
            host,
            port,
            device_id,
        )

    async def connect(self):
        self.connected = True
        logger.info("MOCK CONNECTED to %s (%s)", self.host, self.device_id)

    def close(self):
        self.connected = False
        logger.info("MOCK DISCONNECTED from %s (%s)", self.host, self.device_id)

    @staticmethod
    def _coerce_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _extract_numeric_range(self, param_def: Optional[dict]) -> tuple[Optional[float], Optional[float]]:
        if not param_def:
            return (None, None)

        range_numeric = param_def.get("range_numeric")
        if isinstance(range_numeric, dict):
            min_value = self._coerce_float(range_numeric.get("min"))
            max_value = self._coerce_float(range_numeric.get("max"))
            if min_value is not None and max_value is not None and min_value <= max_value:
                return (min_value, max_value)

        range_text = str(param_def.get("range_text") or "").strip()
        if not range_text:
            return (None, None)

        matches = [
            self._coerce_float(match)
            for match in re.findall(r"-?\d+(?:\.\d+)?", range_text)
        ]
        numbers = [value for value in matches if value is not None]
        if len(numbers) >= 2:
            return (min(numbers), max(numbers))

        return (None, None)

    def _parameter_text(self, pid: str, param_def: Optional[dict]) -> str:
        if not param_def:
            return pid.lower()
        return " ".join(
            str(param_def.get(field) or "")
            for field in ("alias", "name", "description", "range_text")
        ).lower()

    def _is_signed_parameter(self, param_def: Optional[dict]) -> bool:
        if not param_def:
            return False

        min_value, _ = self._extract_numeric_range(param_def)
        if min_value is not None and min_value < 0:
            return True

        default_value = param_def.get("default")
        if isinstance(default_value, str) and default_value.strip().startswith("-"):
            return True

        numeric_default = self._coerce_float(default_value)
        return numeric_default is not None and numeric_default < 0

    def _encode_register_value(self, value: float, param_def: Optional[dict]) -> int:
        raw_value = int(round(value))
        if self._is_signed_parameter(param_def):
            return raw_value & 0xFFFF
        return max(0, min(0xFFFF, raw_value))

    def _simulate_alarm_code(
        self,
        address: int,
        min_value: Optional[float],
        max_value: Optional[float],
        *,
        always_active: bool = False,
    ) -> int:
        minimum = int(min_value if min_value is not None else 1)
        maximum = int(max_value if max_value is not None else 99)

        if maximum <= minimum:
            maximum = minimum + 1

        if minimum <= 0:
            minimum = 100 if maximum >= 100 else 1

        cycle = int((self._counter + self._phase_offset) * 5)
        if not always_active and ((cycle + address + self._device_seed) % 18) >= 4:
            return 0

        span = maximum - minimum
        return minimum + ((address + cycle + self._device_seed) % (span + 1))

    def _simulate_elapsed_time(self, text: str) -> int:
        elapsed_seconds = int((self._counter + self._phase_offset) * 18) + (self._device_seed % 240)
        if "hours since trip" in text:
            return elapsed_seconds // 3600
        if "seconds since trip" in text:
            return elapsed_seconds
        return elapsed_seconds

    def _fallback_engineering_value(self, pid: str, address: int) -> float:
        baseline = 25 + ((sum(ord(ch) for ch in pid) + self._device_seed) % 120)
        amplitude = 10 + ((address + self._device_seed) % 25)
        phase = self._counter + self._phase_offset + (address * 0.12)
        return baseline + math.sin(phase) * amplitude

    def _simulate_register_value(self, address: int, register_type: str) -> int:
        pid = parameter_registry.get_id_by_address(
            address,
            register_type,
            device_id=self.device_id,
        )
        if not pid:
            baseline = ((address * 37) + self._device_seed) % 5000
            wave = math.sin(self._counter + self._phase_offset + (address * 0.08)) * 120
            return max(0, int(baseline + wave))

        param_def = parameter_registry.get_parameter(pid, device_id=self.device_id)
        scale_factor = parameter_registry.get_scale_factor(pid, device_id=self.device_id) or 1
        min_value, max_value = self._extract_numeric_range(param_def)
        text = self._parameter_text(pid, param_def)

        if "trip history" in text:
            return self._simulate_alarm_code(address, min_value, max_value, always_active=True)

        if "warning no." in text or "warning " in text:
            return self._simulate_alarm_code(address, min_value, max_value)

        if "trip no." in text:
            return self._simulate_alarm_code(address, min_value, max_value)

        if "hours since trip" in text or "seconds since trip" in text:
            return self._simulate_elapsed_time(text)

        if min_value is not None and max_value is not None:
            span = max(max_value - min_value, 1.0)
            midpoint = min_value + (span / 2.0)
            amplitude = max(span * 0.22, 0.5)
            amplitude = min(amplitude, span / 2.1)
            bias = span * self._device_bias
            phase = self._counter + self._phase_offset + (address * 0.07)
            noise = random.uniform(-amplitude * 0.04, amplitude * 0.04)
            engineering_value = midpoint + bias + math.sin(phase) * amplitude + noise
            engineering_value = max(min_value, min(max_value, engineering_value))
        else:
            engineering_value = self._fallback_engineering_value(pid, address)

        raw_value = engineering_value * scale_factor
        return self._encode_register_value(raw_value, param_def)

    async def _read_registers(
        self,
        start: int,
        count: int,
        *,
        register_type: str,
        delay_seconds: float,
    ) -> Optional[List[int]]:
        if not self.connected:
            await self.connect()

        await asyncio.sleep(delay_seconds)
        self._counter += 0.1

        results: List[int] = []
        for offset in range(count):
            address = start + offset
            if address in self._register_overrides:
                results.append(self._register_overrides[address])
                continue

            results.append(self._simulate_register_value(address, register_type))

        return results

    async def read_holding_registers(
        self,
        start: int,
        count: int,
        unit: int = 1,
    ) -> Optional[List[int]]:
        return await self._read_registers(
            start,
            count,
            register_type="holding",
            delay_seconds=0.05,
        )

    async def read_input_registers(
        self,
        start: int,
        count: int,
        unit: int = 1,
    ) -> Optional[List[int]]:
        return await self._read_registers(
            start,
            count,
            register_type="input",
            delay_seconds=0.05,
        )

    async def read_coils(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        if not self.connected:
            await self.connect()

        await asyncio.sleep(0.02)
        self._counter += 0.05

        values: List[int] = []
        for offset in range(count):
            address = start + offset
            if address in self._coil_overrides:
                values.append(self._coil_overrides[address])
                continue

            state = ((address + int(self._counter * 10) + self._device_seed) % 2)
            values.append(state)

        return values

    async def read_discrete_inputs(
        self,
        start: int,
        count: int,
        unit: int = 1,
    ) -> Optional[List[int]]:
        if not self.connected:
            await self.connect()

        await asyncio.sleep(0.02)
        self._counter += 0.05
        return [
            ((start + offset + int(self._counter * 7) + self._device_seed) % 2)
            for offset in range(count)
        ]

    async def write_register(self, address: int, value: int, unit: int = 1) -> bool:
        logger.info(
            "MOCK WRITE REGISTER %s @ %s = %s",
            self.device_id,
            address,
            value,
        )
        self._register_overrides[address] = int(value)
        await asyncio.sleep(0.08)
        return True

    async def write_coils(self, address: int, values: List[int | bool], unit: int = 1) -> bool:
        normalized_values = [1 if bool(value) else 0 for value in values]
        logger.info(
            "MOCK WRITE COILS %s @ %s = %s",
            self.device_id,
            address,
            normalized_values,
        )
        for offset, value in enumerate(normalized_values):
            self._coil_overrides[address + offset] = value
        await asyncio.sleep(0.04)
        return True

    async def write_coil(self, address: int, value: int | bool, unit: int = 1) -> bool:
        logger.info(
            "MOCK WRITE COIL %s @ %s = %s",
            self.device_id,
            address,
            value,
        )
        self._coil_overrides[address] = 1 if bool(value) else 0
        await asyncio.sleep(0.04)
        return True
