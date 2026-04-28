from datetime import datetime
from typing import List, Optional
from sqlalchemy import Column, String, Integer, ForeignKey, Float, DateTime, Text
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


class StoredTripEvent(SQLModel, table=True):
    __tablename__ = "trip_events"

    id: Optional[int] = Field(default=None, primary_key=True)
    time: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    device_id: str = Field(nullable=False, index=True)
    code: int = Field(sa_column=Column(Integer, nullable=False))


# Manual Trend Models
class ManualTrendSeries(SQLModel, table=True):
    __tablename__ = "manual_trend_series"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    unit: Optional[str] = Field(default=None)
    color: str = Field(default="#3b82f6")  # Hex color (paleta fija en frontend)
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), nullable=False, onupdate=utc_now),
    )

    points: List["ManualTrendPoint"] = Relationship(
        back_populates="series", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )


class ManualTrendPoint(SQLModel, table=True):
    __tablename__ = "manual_trend_points"

    id: Optional[int] = Field(default=None, primary_key=True)
    series_id: int = Field(foreign_key="manual_trend_series.id", nullable=False)
    time: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    value: float = Field(nullable=False)
    note: Optional[str] = Field(default=None)
    created_by: Optional[str] = Field(default=None)
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    series: Optional[ManualTrendSeries] = Relationship(back_populates="points")


class SharedTrendConfig(SQLModel, table=True):
    __tablename__ = "shared_trend_config"

    key: str = Field(default="default", primary_key=True)
    selected_var_ids_json: str = Field(
        default="[]", sa_column=Column(Text, nullable=False)
    )
    selected_manual_ids_json: str = Field(
        default="[]", sa_column=Column(Text, nullable=False)
    )
    time_range: int = Field(default=60, nullable=False)
    range_mode: str = Field(default="relative", sa_column=Column(String, nullable=False))
    custom_range_json: Optional[str] = Field(
        default=None, sa_column=Column(Text, nullable=True)
    )
    y_axis_scale_json: str = Field(
        default='{"mode":"auto","min":"","max":""}',
        sa_column=Column(Text, nullable=False),
    )
    series_color_overrides_json: str = Field(
        default="{}", sa_column=Column(Text, nullable=False)
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=True), nullable=False, onupdate=utc_now),
    )


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
