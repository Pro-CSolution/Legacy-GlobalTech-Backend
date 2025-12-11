from datetime import datetime
from typing import List, Optional
from sqlalchemy import Column, String, Integer, ForeignKey, Float, DateTime
from sqlmodel import SQLModel, Field, Relationship
from app.core.utils import utc_now

# Existing Models
class TrendData(SQLModel, table=True):
    __tablename__ = "trend_data"

    time: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), primary_key=True, nullable=False),
    )
    device_id: str = Field(primary_key=True, nullable=False)
    parameter_id: str = Field(primary_key=True, nullable=False)
    value: float = Field(nullable=False)

# New Profile Models
class Profile(SQLModel, table=True):
    __tablename__ = "profiles"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    color: str  # Hex color code
    icon: str   # Icon name (e.g., from Lucide)
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    last_used: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
    )

    parameters: List["ProfileParameter"] = Relationship(
        back_populates="profile", sa_relationship_kwargs={"cascade": "all, delete"}
    )


class ProfileParameter(SQLModel, table=True):
    __tablename__ = "profile_parameters"

    id: Optional[int] = Field(default=None, primary_key=True)
    profile_id: int = Field(foreign_key="profiles.id")
    device_id: str
    parameter_id: str
    value: str  # Stored as string to handle various types, parsed on usage

    profile: Optional[Profile] = Relationship(back_populates="parameters")
