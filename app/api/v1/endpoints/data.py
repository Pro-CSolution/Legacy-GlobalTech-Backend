from datetime import datetime, timedelta
from typing import List

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.core.config import settings
from app.core.utils import utc_now
from app.db.models import StoredTripEvent
from app.db.session import get_session
from app.db.models import TrendData
from app.modbus_engine.state import state_manager

router = APIRouter()

TRIP_HISTORY_PARAM_ID = "P10.20"
SECONDS_SINCE_TRIP_PARAM_ID = "P10.30"
HOURS_SINCE_TRIP_PARAM_ID = "P10.31"


class TripEvent(BaseModel):
    time: datetime
    code: int


def _decode_fault_code(value: float | int | None) -> int:
    if value is None:
        return 0

    numeric = int(value)
    if numeric < 0:
        numeric = numeric & 0xFFFF
    if numeric > 999:
        numeric = round(numeric / 100)
    return numeric if numeric > 0 else 0


def _parse_trip_age_seconds(snapshot: dict[str, object]) -> int | None:
    try:
        hours = int(float(snapshot.get(HOURS_SINCE_TRIP_PARAM_ID)))
        seconds = int(float(snapshot.get(SECONDS_SINCE_TRIP_PARAM_ID)))
    except (TypeError, ValueError):
        return None

    if hours < 0 or seconds < 0:
        return None

    return hours * 3600 + seconds


def _build_current_trip_event(device_id: str) -> TripEvent | None:
    snapshot = state_manager.get_snapshot(device_id)
    if not snapshot:
        return None

    code = _decode_fault_code(snapshot.get(TRIP_HISTORY_PARAM_ID))
    age_seconds = _parse_trip_age_seconds(snapshot)
    if code <= 0 or age_seconds is None:
        return None

    return TripEvent(time=utc_now() - timedelta(seconds=age_seconds), code=code)


def _append_unique_trip_event(
    bucket: list[TripEvent],
    candidate: TripEvent,
    *,
    max_time_delta_seconds: int = 10,
) -> None:
    for existing in bucket:
        if existing.code != candidate.code:
            continue
        if abs((existing.time - candidate.time).total_seconds()) <= max_time_delta_seconds:
            return

    bucket.append(candidate)


async def _load_trip_events_from_trend_data(
    session: AsyncSession,
    *,
    device_id: str,
    start_time: datetime,
    end_time: datetime,
    limit: int,
) -> list[TripEvent]:
    statement = text(
        """
        WITH trip_age AS (
            SELECT
                trip_seconds.time AS time,
                CAST(trip_hours.value AS INTEGER) * 3600 + CAST(trip_seconds.value AS INTEGER) AS age_seconds,
                LAG(
                    CAST(trip_hours.value AS INTEGER) * 3600 + CAST(trip_seconds.value AS INTEGER)
                ) OVER (ORDER BY trip_seconds.time) AS prev_age_seconds
            FROM trend_data AS trip_seconds
            JOIN trend_data AS trip_hours
                ON trip_hours.device_id = trip_seconds.device_id
                AND trip_hours.time = trip_seconds.time
                AND trip_hours.parameter_id = :hours_param_id
            WHERE trip_seconds.device_id = :device_id
                AND trip_seconds.parameter_id = :seconds_param_id
                AND trip_seconds.time >= :start_time
                AND trip_seconds.time <= :end_time
        ),
        trip_events AS (
            SELECT
                trip_age.time AS time,
                trip_code.value AS code,
                trip_age.age_seconds AS age_seconds,
                trip_age.prev_age_seconds AS prev_age_seconds
            FROM trip_age
            LEFT JOIN trend_data AS trip_code
                ON trip_code.device_id = :device_id
                AND trip_code.time = trip_age.time
                AND trip_code.parameter_id = :trip_history_param_id
            WHERE trip_age.prev_age_seconds IS NOT NULL
                AND trip_age.age_seconds < trip_age.prev_age_seconds
        )
        SELECT time, code
        FROM trip_events
        WHERE code IS NOT NULL
        ORDER BY time DESC
        LIMIT :limit
        """
    )

    result = await session.execute(
        statement,
        {
            "device_id": device_id,
            "start_time": start_time,
            "end_time": end_time,
            "limit": limit,
            "trip_history_param_id": TRIP_HISTORY_PARAM_ID,
            "seconds_param_id": SECONDS_SINCE_TRIP_PARAM_ID,
            "hours_param_id": HOURS_SINCE_TRIP_PARAM_ID,
        },
    )

    rows = result.mappings().all()
    return [
        TripEvent(time=row["time"], code=_decode_fault_code(row["code"]))
        for row in rows
        if _decode_fault_code(row["code"]) > 0
    ]


@router.get("/history", response_model=List[TrendData])
async def get_history(
    device_id: str,
    parameter_id: str,
    start_time: datetime = Query(default_factory=lambda: utc_now() - timedelta(hours=1)),
    end_time: datetime = Query(default_factory=utc_now),
    limit: int = 100,
    session: AsyncSession = Depends(get_session)
):
    # Note: TimescaleDB queries are standard SQL/SQLAlchemy queries
    statement = select(TrendData).where(
        TrendData.device_id == device_id,
        TrendData.parameter_id == parameter_id,
        TrendData.time >= start_time,
        TrendData.time <= end_time
    ).order_by(TrendData.time.desc()).limit(limit)
    
    result = await session.exec(statement)
    return result.all()


@router.get("/trip-events", response_model=List[TripEvent])
async def get_trip_events(
    device_id: str,
    start_time: datetime = Query(
        default_factory=lambda: utc_now()
        - timedelta(days=max(1, int(settings.TREND_DATA_RETENTION_DAYS or 30)))
    ),
    end_time: datetime = Query(default_factory=utc_now),
    limit: int = 10,
    session: AsyncSession = Depends(get_session),
):
    safe_limit = max(1, min(limit, 25))
    stored_statement = (
        select(StoredTripEvent)
        .where(
            StoredTripEvent.device_id == device_id,
            StoredTripEvent.time >= start_time,
            StoredTripEvent.time <= end_time,
        )
        .order_by(StoredTripEvent.time.desc())
        .limit(safe_limit)
    )

    stored_result = await session.exec(stored_statement)
    merged_events = [TripEvent(time=row.time, code=row.code) for row in stored_result.all()]

    current_trip_event = _build_current_trip_event(device_id)
    if current_trip_event is not None:
        _append_unique_trip_event(merged_events, current_trip_event)

    if len(merged_events) < safe_limit:
        trend_events = await _load_trip_events_from_trend_data(
            session,
            device_id=device_id,
            start_time=start_time,
            end_time=end_time,
            limit=safe_limit,
        )
        for event in trend_events:
            _append_unique_trip_event(merged_events, event)

    merged_events.sort(key=lambda event: event.time, reverse=True)
    return merged_events[:safe_limit]

