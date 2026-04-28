import asyncio
import logging
from datetime import timedelta

from sqlmodel import text

from app.core.config import settings
from app.core.utils import utc_now
from app.db.models import TrendData
from app.db.session import async_session_factory, is_db_ready
from app.modbus_engine.state import state_manager

logger = logging.getLogger(__name__)


class DataLogger:
    def __init__(self):
        self.running = False
        # Intervalo de guardado en DB alineado al polling (configurable)
        self.log_interval = settings.DATA_LOG_INTERVAL or settings.MODBUS_POLL_INTERVAL
        self.retention_days = max(1, int(settings.TREND_DATA_RETENTION_DAYS or 30))
        self.retention_cleanup_interval_seconds = max(
            3600,
            int(settings.TREND_RETENTION_CLEANUP_INTERVAL_HOURS or 12) * 3600,
        )
        self._last_retention_cleanup_monotonic = 0.0

    async def start(self):
        self.running = True
        logger.info("Starting Data Logger...")
        asyncio.create_task(self._loop())

    async def stop(self):
        self.running = False
        logger.info("Data Logger Stopped")

    def _get_loggable_parameter_ids(self, device_id: str, data: dict) -> set[str]:
        trend_ids = state_manager.get_trend_parameters(device_id)
        return {
            pid
            for pid in trend_ids
            if isinstance(pid, str) and pid in data and not pid.startswith("__")
        }

    def _should_run_retention_cleanup(self) -> bool:
        loop = asyncio.get_running_loop()
        now_monotonic = loop.time()
        return (
            now_monotonic - self._last_retention_cleanup_monotonic
            >= self.retention_cleanup_interval_seconds
        )

    async def _run_retention_cleanup(self, session) -> None:
        cutoff = utc_now() - timedelta(days=self.retention_days)
        result = await session.execute(
            text("DELETE FROM trend_data WHERE time < :cutoff"),
            {"cutoff": cutoff},
        )
        await session.commit()
        self._last_retention_cleanup_monotonic = asyncio.get_running_loop().time()

        deleted_rows = getattr(result, "rowcount", None)
        if isinstance(deleted_rows, int) and deleted_rows > 0:
            logger.info(
                "Trend retention cleanup deleted %s rows older than %s days",
                deleted_rows,
                self.retention_days,
            )

    async def _loop(self):
        while self.running:
            try:
                await asyncio.sleep(self.log_interval)

                if not is_db_ready():
                    continue

                async with async_session_factory() as session:
                    entries = []
                    timestamp = utc_now()
                    current_data = state_manager.get_snapshot()

                    for device_id, data in current_data.items():
                        loggable_ids = self._get_loggable_parameter_ids(device_id, data)
                        for pid in sorted(loggable_ids):
                            val = data.get(pid)
                            try:
                                val_float = float(val)
                            except (ValueError, TypeError):
                                continue

                            entries.append(
                                TrendData(
                                    time=timestamp,
                                    device_id=device_id,
                                    parameter_id=pid,
                                    value=val_float,
                                )
                            )

                    if entries:
                        session.add_all(entries)
                        await session.commit()
                        logger.debug("Logged %s data points to DB", len(entries))

                    if self._should_run_retention_cleanup():
                        await self._run_retention_cleanup(session)

            except Exception as e:
                logger.error("Error in DataLogger: %s", e)
                await asyncio.sleep(1)


data_logger = DataLogger()
