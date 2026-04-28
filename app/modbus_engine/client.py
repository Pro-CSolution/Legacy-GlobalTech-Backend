import asyncio
import inspect
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from typing import Optional, List

from pymodbus.client import AsyncModbusTcpClient

logger = logging.getLogger(__name__)


class Backoff:
    """
    Simple exponential backoff with jitter.
    Used to avoid reconnect storms when a device/port is flaky.
    """

    def __init__(self, base: float = 0.5, cap: float = 10.0):
        self.base = float(base)
        self.cap = float(cap)
        self.fail = 0
        self.next_time = 0.0

    def ok(self) -> None:
        self.fail = 0
        self.next_time = 0.0

    def bad(self) -> None:
        self.fail += 1
        delay = min(self.cap, self.base * (2 ** (self.fail - 1)))
        delay = delay * (0.8 + random.random() * 0.4)  # jitter 0.8–1.2
        self.next_time = time.monotonic() + delay

    def can_try(self) -> bool:
        return time.monotonic() >= self.next_time


class _WritePriorityLock:
    """
    Single-operation mutex with writer preference.
    If a write is waiting, new reads will wait so writes get priority.
    """

    def __init__(self) -> None:
        self._cond = asyncio.Condition()
        self._locked = False
        self._writers_waiting = 0

    @asynccontextmanager
    async def read(self):
        async with self._cond:
            while self._locked or self._writers_waiting > 0:
                await self._cond.wait()
            self._locked = True
        try:
            yield
        finally:
            async with self._cond:
                self._locked = False
                self._cond.notify_all()

    @asynccontextmanager
    async def write(self):
        async with self._cond:
            self._writers_waiting += 1
            try:
                while self._locked:
                    await self._cond.wait()
                self._locked = True
            finally:
                self._writers_waiting -= 1
        try:
            yield
        finally:
            async with self._cond:
                self._locked = False
                self._cond.notify_all()


class ModbusClientWrapper:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        # Prefer predictable retries: we control reconnection/backoff ourselves.
        self._timeout_s = float(os.getenv("MODBUS_TIMEOUT_S", "3"))
        self.client = self._build_client()

        # Writes should win over reads if they are waiting.
        self._op_lock = _WritePriorityLock()

        # Backoff/circuit breaker for reconnection attempts.
        backoff_base = float(os.getenv("MODBUS_BACKOFF_BASE_S", "0.5"))
        backoff_cap = float(os.getenv("MODBUS_BACKOFF_CAP_S", "10"))
        self._backoff = Backoff(base=backoff_base, cap=backoff_cap)

        # Small gap between requests to avoid overwhelming fragile drives.
        self._min_gap_s = float(os.getenv("MODBUS_MIN_GAP_S", "0.02"))  # 20ms default (fast)
        self._last_req_ts = 0.0

        # Connection control: avoid many coroutines blocking on a slow TCP connect.
        self._connect_task: Optional[asyncio.Task] = None
        self._connect_timeout_s = float(os.getenv("MODBUS_CONNECT_TIMEOUT_S", "1.0"))

        # Compatibility for pymodbus API changes: device_id vs slave.
        self._unit_kw = self._detect_unit_kw()

    def _build_client(self) -> AsyncModbusTcpClient:
        return AsyncModbusTcpClient(
            self.host,
            port=self.port,
            timeout=self._timeout_s,
            retries=0,
        )

    def _reset_client(self) -> None:
        old_client = self.client
        self._connect_task = None
        try:
            old_client.close()
        except Exception:
            pass
        self.client = self._build_client()
        self._unit_kw = self._detect_unit_kw()

    def _detect_unit_kw(self) -> str:
        try:
            sig = inspect.signature(self.client.read_holding_registers)
            return "device_id" if "device_id" in sig.parameters else "slave"
        except Exception:
            # Safe fallback for modern pymodbus.
            return "device_id"

    async def _gap(self) -> None:
        now = asyncio.get_running_loop().time()
        dt = now - self._last_req_ts
        if dt < self._min_gap_s:
            await asyncio.sleep(self._min_gap_s - dt)
        self._last_req_ts = asyncio.get_running_loop().time()

    async def _do_connect(self) -> bool:
        try:
            await asyncio.wait_for(self.client.connect(), timeout=self._connect_timeout_s)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("TCP connect exception to %s:%s: %s", self.host, self.port, repr(e))
            logger.debug("Connect exception details:", exc_info=True)
            self._reset_client()
            return False

        if not self.client.connected:
            # Typical case: SYN timeout / no response
            logger.warning("TCP connect failed to %s:%s (no response)", self.host, self.port)
            self._reset_client()
            return False

        logger.info("TCP connected to %s:%s", self.host, self.port)
        return True

    async def _ensure_connected(self) -> bool:
        if self.client.connected:
            return True

        if not self._backoff.can_try():
            return False

        # If a connect is already in flight, do not block other operations.
        if self._connect_task is not None and not self._connect_task.done():
            return False

        self._connect_task = asyncio.create_task(self._do_connect())
        ok = await self._connect_task
        if ok:
            self._backoff.ok()
            return True

        self._backoff.bad()
        return False

    async def _call(self, method_name: str, *args, is_write: bool, **kwargs):
        lock_ctx = self._op_lock.write() if is_write else self._op_lock.read()
        async with lock_ctx:
            ok = await self._ensure_connected()
            if not ok:
                return None

            await self._gap()
            fn = getattr(self.client, method_name)

            try:
                result = await fn(*args, **kwargs)
                if result is None or result.isError():
                    logger.error("Modbus error from %s:%s -> %s", self.host, self.port, result)
                    self._backoff.bad()
                    self._reset_client()
                    return None

                self._backoff.ok()
                return result

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Modbus exception %s:%s -> %s", self.host, self.port, str(e))
                logger.debug("Modbus exception details:", exc_info=True)
                self._backoff.bad()
                self._reset_client()
                return None

    def close(self):
        connect_task = self._connect_task
        self._connect_task = None
        if connect_task is not None and not connect_task.done():
            connect_task.cancel()
        """Cierra la conexión de manera segura"""
        try:
            self.client.close()
        except Exception as e:
            logger.warning("Error closing client %s:%s: %s", self.host, self.port, str(e))

    async def read_holding_registers(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """Lee registros de forma thread-safe"""
        kw = {self._unit_kw: unit}
        result = await self._call(
            "read_holding_registers",
            start,
            is_write=False,
            count=count,
            **kw,
        )
        return None if result is None else result.registers

    async def read_input_registers(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """Lee input registers (función 4)"""
        kw = {self._unit_kw: unit}
        result = await self._call(
            "read_input_registers",
            start,
            is_write=False,
            count=count,
            **kw,
        )
        return None if result is None else result.registers

    async def read_coils(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """Lee coils (función 1). Retorna 0/1."""
        kw = {self._unit_kw: unit}
        result = await self._call(
            "read_coils",
            start,
            is_write=False,
            count=count,
            **kw,
        )
        return None if result is None else [int(bit) for bit in result.bits[:count]]

    async def read_discrete_inputs(self, start: int, count: int, unit: int = 1) -> Optional[List[int]]:
        """Lee discrete inputs (función 2). Retorna 0/1."""
        kw = {self._unit_kw: unit}
        result = await self._call(
            "read_discrete_inputs",
            start,
            is_write=False,
            count=count,
            **kw,
        )
        return None if result is None else [int(bit) for bit in result.bits[:count]]

    async def write_register(self, address: int, value: int, unit: int = 1) -> bool:
        """Escribe un registro de forma thread-safe"""
        kw = {self._unit_kw: unit}
        result = await self._call(
            "write_register",
            address,
            value,
            is_write=True,
            **kw,
        )
        return result is not None

    async def write_coils(self, address: int, values: List[int | bool], unit: int = 1) -> bool:
        """Escribe uno o mÃ¡s coils (funciÃ³n 15)."""
        kw = {self._unit_kw: unit}
        normalized_values = [bool(value) for value in values]
        result = await self._call(
            "write_coils",
            address,
            normalized_values,
            is_write=True,
            **kw,
        )
        return result is not None

    async def write_coil(self, address: int, value: int | bool, unit: int = 1) -> bool:
        """Escribe un coil (función 5)"""
        kw = {self._unit_kw: unit}
        result = await self._call(
            "write_coil",
            address,
            bool(value),
            is_write=True,
            **kw,
        )
        return result is not None
