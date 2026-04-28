from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoints.data import (
    TripEvent,
    _append_unique_trip_event,
    _build_current_trip_event,
    _load_trip_events_from_trend_data,
)
from app.api.v1.endpoints.drive import (
    get_drive_menus,
    get_drive_parameters,
    get_fault_codes,
)
from app.api.v1.endpoints.manual_trend import get_series_history, list_series
from app.api.v1.endpoints.trend import get_trend_history
from app.api.v1.endpoints.wago_live import (
    WagoLiveSnapshotResponse,
    get_wago_live_snapshot,
)
from app.core.config import settings
from app.core.utils import utc_now
from app.db.models import StoredTripEvent
from app.db.session import get_session
from app.db.session import get_db_status, is_db_ready
from app.modbus_engine.manager import modbus_manager
from app.modbus_engine.state import state_manager
from app.services.shared_trend_config import (
    SharedTrendConfigResponse,
    read_shared_trend_config,
)

router = APIRouter(prefix="/monitor", tags=["Monitor"])


class MonitorDeviceSummary(BaseModel):
    device_id: str
    name: str
    host: Optional[str] = None
    connected: bool
    last_ok_ts: Optional[str] = None
    parameter_count: int


class MonitorSummaryResponse(BaseModel):
    status: str
    db_ready: bool
    modbus_running: bool
    fetched_at: str
    devices: List[MonitorDeviceSummary]


@router.get("/health")
async def get_monitor_health() -> Dict[str, Any]:
    return {
        "status": "ok" if bool(getattr(modbus_manager, "running", False)) else "degraded",
        "db": get_db_status(),
        "modbus_running": bool(getattr(modbus_manager, "running", False)),
        "fetched_at": utc_now().replace(microsecond=0).isoformat(),
    }


@router.get("/summary", response_model=MonitorSummaryResponse)
async def get_monitor_summary() -> MonitorSummaryResponse:
    devices: List[MonitorDeviceSummary] = []

    for device_id, device_conf in (getattr(modbus_manager, "devices", {}) or {}).items():
        snapshot = state_manager.get_snapshot(device_id) or {}
        devices.append(
            MonitorDeviceSummary(
                device_id=device_id,
                name=str(device_conf.get("name") or device_id),
                host=str(device_conf.get("host")) if device_conf.get("host") else None,
                connected=bool(snapshot.get("__connected")),
                last_ok_ts=(
                    str(snapshot.get("__lastOkTs"))
                    if isinstance(snapshot.get("__lastOkTs"), str)
                    else None
                ),
                parameter_count=len(snapshot),
            )
        )

    devices.sort(key=lambda item: item.device_id)

    status = "ok"
    if not bool(getattr(modbus_manager, "running", False)) or not is_db_ready():
        status = "degraded"

    return MonitorSummaryResponse(
        status=status,
        db_ready=is_db_ready(),
        modbus_running=bool(getattr(modbus_manager, "running", False)),
        fetched_at=utc_now().replace(microsecond=0).isoformat(),
        devices=devices,
    )


@router.get("/trend-config", response_model=SharedTrendConfigResponse)
async def get_monitor_trend_config(
    session: AsyncSession = Depends(get_session),
) -> SharedTrendConfigResponse:
    return await read_shared_trend_config(session)


@router.get("/trend/history")
async def get_monitor_trend_history(
    device_id: str,
    parameter_ids: List[str] = Query(..., min_length=1),
    window_minutes: int = 5,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit_per_param: int = 2000,
    session: AsyncSession = Depends(get_session),
):
    return await get_trend_history(
        device_id=device_id,
        parameter_ids=parameter_ids,
        window_minutes=window_minutes,
        start_time=start_time,
        end_time=end_time,
        limit_per_param=limit_per_param,
        session=session,
    )


@router.get("/drive/menus")
async def get_monitor_drive_menus(
    device_id: str = Query("drive_avid", description="Drive device ID"),
):
    return await get_drive_menus(device_id=device_id)


@router.get("/drive/parameters")
async def get_monitor_drive_parameters(
    device_id: str = Query("drive_avid", description="Drive device ID"),
    menu: Optional[int] = Query(None, description="Menu number to filter by"),
    search: Optional[str] = Query(None, description="Buscar por id o nombre"),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=100),
):
    return await get_drive_parameters(
        device_id=device_id,
        menu=menu,
        search=search,
        page=page,
        page_size=page_size,
    )


@router.get("/drive/fault-codes")
async def get_monitor_fault_codes():
    return await get_fault_codes()


@router.get("/trip-events")
async def get_monitor_trip_events(
    device_id: str = Query(..., description="Drive device ID"),
    start_time: Optional[datetime] = Query(None, description="ISO start time"),
    end_time: Optional[datetime] = Query(None, description="ISO end time"),
    limit: int = Query(10, ge=1, le=25),
    session: AsyncSession = Depends(get_session),
):
    safe_limit = max(1, min(limit, 25))
    safe_start_time = start_time or (
        utc_now() - timedelta(days=max(1, int(settings.TREND_DATA_RETENTION_DAYS or 30)))
    )
    safe_end_time = end_time or utc_now()

    stored_statement = (
        select(StoredTripEvent)
        .where(
            StoredTripEvent.device_id == device_id,
            StoredTripEvent.time >= safe_start_time,
            StoredTripEvent.time <= safe_end_time,
        )
        .order_by(StoredTripEvent.time.desc())
        .limit(safe_limit)
    )

    stored_result = await session.execute(stored_statement)
    merged_events = [TripEvent(time=row.time, code=row.code) for row in stored_result.scalars().all()]

    current_trip_event = _build_current_trip_event(device_id)
    if current_trip_event is not None:
        _append_unique_trip_event(merged_events, current_trip_event)

    if len(merged_events) < safe_limit:
        trend_events = await _load_trip_events_from_trend_data(
            session,
            device_id=device_id,
            start_time=safe_start_time,
            end_time=safe_end_time,
            limit=safe_limit,
        )
        for event in trend_events:
            _append_unique_trip_event(merged_events, event)

    merged_events.sort(key=lambda event: event.time, reverse=True)
    return merged_events[:safe_limit]


@router.get("/manual-trend/series")
async def get_monitor_manual_trend_series(
    session: AsyncSession = Depends(get_session),
):
    return await list_series(session=session)


@router.get("/manual-trend/series/{series_id}/history")
async def get_monitor_manual_trend_history(
    series_id: int,
    start_time: Optional[datetime] = Query(None, description="ISO start time"),
    end_time: Optional[datetime] = Query(None, description="ISO end time"),
    window_minutes: Optional[int] = Query(60, ge=1, le=24 * 60),
    limit: int = Query(2000, ge=1, le=5000),
    session: AsyncSession = Depends(get_session),
):
    return await get_series_history(
        series_id=series_id,
        start_time=start_time,
        end_time=end_time,
        window_minutes=window_minutes,
        limit=limit,
        session=session,
    )


@router.get("/wago-live/snapshot", response_model=WagoLiveSnapshotResponse)
async def get_monitor_wago_live_snapshot(
    device_id: str = Query("wago", description="WAGO device ID"),
    register_type: str = Query(
        "input",
        description="Register type to read: input, holding, discrete, or coil",
    ),
    address: List[int] = Query(
        ...,
        description="Raw offsets or spreadsheet Modbus register numbers to read",
    ),
):
    return await get_wago_live_snapshot(
        device_id=device_id,
        register_type=register_type,
        address=address,
    )
