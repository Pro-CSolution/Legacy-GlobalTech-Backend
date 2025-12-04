from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field

class TrendData(SQLModel, table=True):
    __tablename__ = "trend_data"
    
    time: datetime = Field(default_factory=datetime.utcnow, primary_key=True, nullable=False)
    device_id: str = Field(primary_key=True, nullable=False)
    parameter_id: str = Field(primary_key=True, nullable=False)
    value: float = Field(nullable=False)
