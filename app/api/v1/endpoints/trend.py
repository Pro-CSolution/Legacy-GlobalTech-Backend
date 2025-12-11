from datetime import datetime, timedelta
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.core.utils import utc_now
from app.db.models import TrendData
from app.db.session import get_session

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

    result = await session.execute(stmt)
    rows = result.scalars().all()

    # Agrupar y aplicar límite por parámetro en memoria
    series: Dict[str, List[Dict[str, object]]] = {pid: [] for pid in parameter_ids}
    for row in rows:
        bucket = series.get(row.parameter_id)
        if bucket is None:
            continue
        if len(bucket) >= limit_per_param:
            continue
        bucket.append({"time": row.time, "value": row.value})

    # Invertir cada serie para devolverlas en orden ascendente (tiempo creciente)
    for pid in series:
        series[pid].reverse()

    return {"device_id": device_id, "series": series}
