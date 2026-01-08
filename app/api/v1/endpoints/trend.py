from datetime import datetime, timedelta
from typing import Dict, List
import logging
import time

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.core.utils import utc_now
from app.db.models import TrendData
from app.db.session import get_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trend", tags=["Trend"])


@router.get("/history")
async def get_trend_history(
    device_id: str,
    parameter_ids: List[str] = Query(..., min_length=1),
    window_minutes: int = 5,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit_per_param: int = 2000,
    session: AsyncSession = Depends(get_session),
):
    """
    Retorna series históricas para múltiples parámetros críticos de un dispositivo.
    - Si no se proveen start/end, se usa una ventana (window_minutes) hacia atrás desde ahora.
    - Limita la cantidad de puntos por parámetro con limit_per_param.
    """
    t_total_0 = time.perf_counter()
    if limit_per_param <= 0:
        raise HTTPException(
            status_code=400, detail="limit_per_param debe ser mayor a 0"
        )

    # Resolver ventana temporal
    effective_end = end_time or utc_now()
    effective_start = start_time or (effective_end - timedelta(minutes=window_minutes))

    if effective_start >= effective_end:
        raise HTTPException(
            status_code=400, detail="start_time debe ser anterior a end_time"
        )

    # Query base: traemos los más recientes primero para respetar limit_per_param
    max_rows = limit_per_param * max(len(parameter_ids), 1)
    stmt = (
        select(TrendData)
        .where(
            TrendData.device_id == device_id,
            TrendData.parameter_id.in_(parameter_ids),
            TrendData.time >= effective_start,
            TrendData.time <= effective_end,
        )
        .order_by(TrendData.time.desc())
        .limit(max_rows)
    )

    t_query_0 = time.perf_counter()
    result = await session.execute(stmt)
    t_query_1 = time.perf_counter()
    rows = result.scalars().all()
    t_fetch_1 = time.perf_counter()

    # Agrupar y aplicar límite por parámetro en memoria
    series: Dict[str, List[Dict[str, object]]] = {pid: [] for pid in parameter_ids}
    t_group_0 = time.perf_counter()
    for row in rows:
        bucket = series.get(row.parameter_id)
        if bucket is None:
            continue
        if len(bucket) >= limit_per_param:
            continue
        bucket.append({"time": row.time, "value": row.value})
    t_group_1 = time.perf_counter()

    # Invertir cada serie para devolverlas en orden ascendente (tiempo creciente)
    t_rev_0 = time.perf_counter()
    for pid in series:
        series[pid].reverse()
    t_rev_1 = time.perf_counter()

    t_total_1 = time.perf_counter()

    meta = {
        "query_ms": round((t_query_1 - t_query_0) * 1000, 2),
        "fetch_ms": round((t_fetch_1 - t_query_1) * 1000, 2),
        "group_ms": round((t_group_1 - t_group_0) * 1000, 2),
        "reverse_ms": round((t_rev_1 - t_rev_0) * 1000, 2),
        "total_ms": round((t_total_1 - t_total_0) * 1000, 2),
        "rows": len(rows),
        "params": len(parameter_ids),
        "max_rows": max_rows,
        "limit_per_param": limit_per_param,
        "window_minutes": window_minutes,
    }

    logger.info(
        "[trend] history device=%s params=%s window=%s limit_per_param=%s rows=%s query_ms=%.2f total_ms=%.2f",
        device_id,
        len(parameter_ids),
        window_minutes,
        limit_per_param,
        len(rows),
        meta["query_ms"],
        meta["total_ms"],
    )

    return {"device_id": device_id, "series": series, "meta": meta}
