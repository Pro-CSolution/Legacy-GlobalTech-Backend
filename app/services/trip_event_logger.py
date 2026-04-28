import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlmodel import select

from app.core.config import settings
from app.core.utils import utc_now
from app.db.models import StoredTripEvent
from app.db.session import async_session_factory, is_db_ready
from app.modbus_engine.state import state_manager

logger = logging.getLogger(__name__)

TRIP_HISTORY_PARAM_ID = "P10.20"
SECONDS_SINCE_TRIP_PARAM_ID = "P10.30"
HOURS_SINCE_TRIP_PARAM_ID = "P10.31"
TRIP_MONITORED_PARAM_IDS = {
    TRIP_HISTORY_PARAM_ID,
    SECONDS_SINCE_TRIP_PARAM_ID,
    HOURS_SINCE_TRIP_PARAM_ID,
}


def _decode_fault_code(value: Any) -> int:
    if value is None:
        return 0

    try:
        numeric = int(float(value))
    except (TypeError, ValueError):
        return 0

    if numeric < 0:
        numeric = numeric & 0xFFFF
    if numeric > 999:
        numeric = round(numeric / 100)
    return numeric if numeric > 0 else 0


def _parse_trip_age_seconds(snapshot: dict[str, Any]) -> int | None:
    try:
        hours = int(float(snapshot.get(HOURS_SINCE_TRIP_PARAM_ID)))
        seconds = int(float(snapshot.get(SECONDS_SINCE_TRIP_PARAM_ID)))
    except (TypeError, ValueError):
        return None

    if hours < 0 or seconds < 0:
        return None

    return hours * 3600 + seconds


class TripEventLogger:
    def __init__(self):
        self.running = False
        self.retention_days = max(1, int(settings.TREND_DATA_RETENTION_DAYS or 30))
        self.retention_cleanup_interval_seconds = max(
            3600,
            int(settings.TREND_RETENTION_CLEANUP_INTERVAL_HOURS or 12) * 3600,
        )
        self._last_retention_cleanup_monotonic = 0.0
        self._last_seen_by_device: dict[str, tuple[int, int]] = {}
        self._last_persisted_by_device: dict[str, tuple[int, datetime]] = {}
        self._pending_events: list[tuple[datetime, str, int]] = []
        self._listener = self._handle_state_update

    async def start(self):
        if self.running:
            return

        self.running = True
        state_manager.add_listener(self._listener)
        logger.info("Trip Event Logger initialized.")

    async def stop(self):
        await self._flush_pending_events()
        self.running = False
        logger.info("Trip Event Logger stopped.")

    def _extract_trip_state(self, snapshot: dict[str, Any]) -> tuple[int, int] | None:
        age_seconds = _parse_trip_age_seconds(snapshot)
        if age_seconds is None:
            return None

        code = _decode_fault_code(snapshot.get(TRIP_HISTORY_PARAM_ID))
        return (code, age_seconds)

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
            text("DELETE FROM trip_events WHERE time < :cutoff"),
            {"cutoff": cutoff},
        )
        await session.commit()
        self._last_retention_cleanup_monotonic = asyncio.get_running_loop().time()

        deleted_rows = getattr(result, "rowcount", None)
        if isinstance(deleted_rows, int) and deleted_rows > 0:
            logger.info(
                "Trip-event retention cleanup deleted %s rows older than %s days",
                deleted_rows,
                self.retention_days,
            )

    async def _flush_pending_events(self) -> None:
        if not self._pending_events or not is_db_ready():
            return

        pending = list(self._pending_events)
        self._pending_events.clear()

        async with async_session_factory() as session:
            for event_time, device_id, code in pending:
                if self._is_recent_duplicate(device_id, code, event_time):
                    continue
                session.add(StoredTripEvent(time=event_time, device_id=device_id, code=code))
                self._last_persisted_by_device[device_id] = (code, event_time)

            await session.commit()

            if self._should_run_retention_cleanup():
                await self._run_retention_cleanup(session)

    def _is_recent_duplicate(self, device_id: str, code: int, event_time: datetime) -> bool:
        previous = self._last_persisted_by_device.get(device_id)
        if previous is None:
            return False

        previous_code, previous_time = previous
        return previous_code == code and abs((event_time - previous_time).total_seconds()) <= 5

    async def _persist_event(self, device_id: str, code: int, age_seconds: int) -> None:
        if code <= 0:
            return

        event_time = utc_now() - timedelta(seconds=max(0, age_seconds))
        if self._is_recent_duplicate(device_id, code, event_time):
            return

        if not is_db_ready():
            self._pending_events.append((event_time, device_id, code))
            return

        await self._flush_pending_events()

        async with async_session_factory() as session:
            session.add(StoredTripEvent(time=event_time, device_id=device_id, code=code))
            await session.commit()

            if self._should_run_retention_cleanup():
                await self._run_retention_cleanup(session)

        self._last_persisted_by_device[device_id] = (code, event_time)

    async def _backfill_current_trip_if_missing(
        self,
        device_id: str,
        code: int,
        age_seconds: int,
    ) -> None:
        if code <= 0 or not is_db_ready():
            return

        event_time = utc_now() - timedelta(seconds=max(0, age_seconds))
        if self._is_recent_duplicate(device_id, code, event_time):
            return

        await self._flush_pending_events()

        async with async_session_factory() as session:
            statement = (
                select(StoredTripEvent)
                .where(StoredTripEvent.device_id == device_id)
                .order_by(StoredTripEvent.time.desc())
                .limit(1)
            )
            latest_result = await session.exec(statement)
            latest = latest_result.first()
            if latest is not None:
                if latest.code == code and abs((event_time - latest.time).total_seconds()) <= 120:
                    self._last_persisted_by_device[device_id] = (latest.code, latest.time)
                    return

            session.add(StoredTripEvent(time=event_time, device_id=device_id, code=code))
            await session.commit()
            self._last_persisted_by_device[device_id] = (code, event_time)

            if self._should_run_retention_cleanup():
                await self._run_retention_cleanup(session)

    async def _handle_state_update(self, device_id: str, values: dict[str, Any]) -> None:
        if not self.running or not any(param_id in values for param_id in TRIP_MONITORED_PARAM_IDS):
            return

        snapshot = state_manager.get_snapshot(device_id)
        next_state = self._extract_trip_state(snapshot)
        if next_state is None:
            return

        previous_state = self._last_seen_by_device.get(device_id)
        self._last_seen_by_device[device_id] = next_state

        next_code, next_age_seconds = next_state
        if previous_state is None:
            await self._backfill_current_trip_if_missing(device_id, next_code, next_age_seconds)
            return

        _, previous_age_seconds = previous_state
        if next_code <= 0 or next_age_seconds >= previous_age_seconds:
            await self._flush_pending_events()
            return

        await self._persist_event(device_id, next_code, next_age_seconds)


trip_event_logger = TripEventLogger()
