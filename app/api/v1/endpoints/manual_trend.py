from datetime import datetime, timedelta
from typing import List, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.core.utils import utc_now
from app.db.models import ManualTrendSeries, ManualTrendPoint
from app.db.session import get_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/manual-trend", tags=["ManualTrend"])


class ManualSeriesCreate(BaseModel):
    name: str
    unit: Optional[str] = None
    color: Optional[str] = None


class ManualSeriesUpdate(BaseModel):
    name: Optional[str] = None
    unit: Optional[str] = None
    color: Optional[str] = None


class ManualPointCreate(BaseModel):
    value: float
    time: Optional[datetime] = None
    note: Optional[str] = None
    created_by: Optional[str] = None


class ManualPointUpdate(BaseModel):
    value: Optional[float] = None
    time: Optional[datetime] = None
    note: Optional[str] = None
    created_by: Optional[str] = None


# ----- Series -----
@router.get("/series", response_model=List[ManualTrendSeries])
async def list_series(session: AsyncSession = Depends(get_session)):
    logger.info("[manual_trend] list_series")
    result = await session.execute(select(ManualTrendSeries))
    series = result.scalars().all()
    logger.debug("[manual_trend] list_series count=%s", len(series))
    return series


@router.post("/series", response_model=ManualTrendSeries)
async def create_series(payload: ManualSeriesCreate, session: AsyncSession = Depends(get_session)):
    logger.info("[manual_trend] create_series name=%s unit=%s color=%s", payload.name, payload.unit, payload.color)
    series = ManualTrendSeries(name=payload.name, unit=payload.unit, color=payload.color or "#3b82f6")
    session.add(series)
    await session.commit()
    await session.refresh(series)
    logger.debug("[manual_trend] created series id=%s", series.id)
    return series


@router.patch("/series/{series_id}", response_model=ManualTrendSeries)
async def update_series(
    series_id: int,
    payload: ManualSeriesUpdate,
    session: AsyncSession = Depends(get_session),
):
    logger.info("[manual_trend] update_series id=%s name=%s unit=%s color=%s", series_id, payload.name, payload.unit, payload.color)
    series = await session.get(ManualTrendSeries, series_id)
    if not series:
        raise HTTPException(status_code=404, detail="Serie manual no encontrada")

    if payload.name is not None:
        series.name = payload.name
    if payload.unit is not None:
        series.unit = payload.unit
    if payload.color is not None:
        series.color = payload.color

    await session.commit()
    await session.refresh(series)
    logger.debug("[manual_trend] updated series id=%s", series.id)
    return series


@router.delete("/series/{series_id}")
async def delete_series(series_id: int, session: AsyncSession = Depends(get_session)):
    logger.info("[manual_trend] delete_series id=%s", series_id)
    series = await session.get(ManualTrendSeries, series_id)
    if not series:
        raise HTTPException(status_code=404, detail="Serie manual no encontrada")
    await session.delete(series)
    await session.commit()
    logger.debug("[manual_trend] deleted series id=%s", series_id)
    return {"status": "deleted", "series_id": series_id}


# ----- Points -----
@router.get("/series/{series_id}/history")
async def get_series_history(
    series_id: int,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    window_minutes: Optional[int] = 60,
    limit: int = 2000,
    session: AsyncSession = Depends(get_session),
):
    """
    Obtiene puntos manuales para una serie.
    - Si no se proveen start/end y window_minutes es None, devuelve todos.
    - Si solo window_minutes viene, usa ventana hacia atrás desde ahora.
    - Devuelve orden ascendente por tiempo.
    """
    logger.info(
        "[manual_trend] history series_id=%s start=%s end=%s window_minutes=%s limit=%s",
        series_id,
        start_time,
        end_time,
        window_minutes,
        limit,
    )
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit debe ser mayor a 0")

    exists = await session.get(ManualTrendSeries, series_id)
    if not exists:
        raise HTTPException(status_code=404, detail="Serie manual no encontrada")

    conditions = [ManualTrendPoint.series_id == series_id]

    effective_end = end_time or utc_now()
    effective_start = start_time

    if start_time is None and end_time is None and window_minutes:
        effective_start = effective_end - timedelta(minutes=window_minutes)

    if effective_start:
        conditions.append(ManualTrendPoint.time >= effective_start)
    if end_time:
        conditions.append(ManualTrendPoint.time <= effective_end)

    stmt = select(ManualTrendPoint).where(*conditions).order_by(ManualTrendPoint.time.desc()).limit(limit)
    result = await session.execute(stmt)
    rows = result.scalars().all()
    rows.reverse()  # ascendente
    logger.debug(
        "[manual_trend] history rows=%s start=%s end=%s window=%s",
        len(rows),
        effective_start,
        effective_end if end_time else None,
        window_minutes,
    )
    return {"series_id": series_id, "points": rows}


@router.post("/series/{series_id}/points", response_model=ManualTrendPoint)
async def create_point(
    series_id: int,
    payload: ManualPointCreate,
    session: AsyncSession = Depends(get_session),
):
    logger.info(
        "[manual_trend] create_point series_id=%s value=%s time=%s note=%s created_by=%s",
        series_id,
        payload.value,
        payload.time,
        payload.note,
        payload.created_by,
    )
    series = await session.get(ManualTrendSeries, series_id)
    if not series:
        raise HTTPException(status_code=404, detail="Serie manual no encontrada")

    point = ManualTrendPoint(
        series_id=series_id,
        value=float(payload.value),
        time=payload.time or utc_now(),
        note=payload.note,
        created_by=payload.created_by,
    )
    session.add(point)
    await session.commit()
    await session.refresh(point)
    logger.debug("[manual_trend] created point id=%s", point.id)
    return point


@router.patch("/points/{point_id}", response_model=ManualTrendPoint)
async def update_point(
    point_id: int,
    payload: ManualPointUpdate,
    session: AsyncSession = Depends(get_session),
):
    logger.info(
        "[manual_trend] update_point id=%s value=%s time=%s note=%s created_by=%s",
        point_id,
        payload.value,
        payload.time,
        payload.note,
        payload.created_by,
    )
    point = await session.get(ManualTrendPoint, point_id)
    if not point:
        raise HTTPException(status_code=404, detail="Punto manual no encontrado")

    if payload.value is not None:
        point.value = float(payload.value)
    if payload.time is not None:
        point.time = payload.time
    if payload.note is not None:
        point.note = payload.note
    if payload.created_by is not None:
        point.created_by = payload.created_by

    await session.commit()
    await session.refresh(point)
    logger.debug("[manual_trend] updated point id=%s", point.id)
    return point


@router.delete("/points/{point_id}")
async def delete_point(point_id: int, session: AsyncSession = Depends(get_session)):
    logger.info("[manual_trend] delete_point id=%s", point_id)
    point = await session.get(ManualTrendPoint, point_id)
    if not point:
        raise HTTPException(status_code=404, detail="Punto manual no encontrado")
    await session.delete(point)
    await session.commit()
    logger.debug("[manual_trend] deleted point id=%s", point_id)
    return {"status": "deleted", "point_id": point_id}
