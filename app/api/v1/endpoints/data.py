from fastapi import APIRouter, Depends, Query
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from datetime import datetime, timedelta

from app.db.session import get_session
from app.db.models import TrendData

router = APIRouter()

@router.get("/history", response_model=List[TrendData])
async def get_history(
    device_id: str,
    parameter_id: str,
    start_time: datetime = Query(default_factory=lambda: datetime.utcnow() - timedelta(hours=1)),
    end_time: datetime = Query(default_factory=datetime.utcnow),
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

