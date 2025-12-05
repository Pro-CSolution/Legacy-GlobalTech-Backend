from datetime import datetime
from sqlalchemy import Column, DateTime
from sqlmodel import SQLModel, Field

from app.core.utils import utc_now
class TrendData(SQLModel, table=True):
    __tablename__ = "trend_data"

    time: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), primary_key=True, nullable=False),
    )
    device_id: str = Field(primary_key=True, nullable=False)
    parameter_id: str = Field(primary_key=True, nullable=False)
    value: float = Field(nullable=False)
