import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import SharedTrendConfig

DEFAULT_SHARED_TREND_CONFIG_KEY = "default"
DEFAULT_Y_AXIS_SCALE = {"mode": "auto", "min": "", "max": ""}
LOCAL_TREND_STORAGE_DIR = Path.home() / "AppData" / "Roaming" / "globaltech-hmi" / "Local Storage" / "leveldb"


class SharedTrendCustomRange(BaseModel):
    start: str
    end: str


class SharedTrendYAxisScale(BaseModel):
    mode: str = "auto"
    min: str = ""
    max: str = ""


class SharedTrendConfigUpdate(BaseModel):
    selectedVarIds: List[str] = Field(default_factory=list)
    selectedManualIds: List[int] = Field(default_factory=list)
    timeRange: int = 60
    rangeMode: str = "relative"
    customRange: Optional[SharedTrendCustomRange] = None
    yAxisScale: SharedTrendYAxisScale = Field(default_factory=SharedTrendYAxisScale)
    seriesColorOverrides: Dict[str, str] = Field(default_factory=dict)


class SharedTrendConfigResponse(SharedTrendConfigUpdate):
    updatedAt: Optional[str] = None


def _default_response() -> SharedTrendConfigResponse:
    return SharedTrendConfigResponse()


def _decode_json(value: Optional[str], fallback):
    if not value:
        return fallback

    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


def _normalize_string_list(values: List[object]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []

    for value in values:
        if not isinstance(value, str):
            continue
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        normalized.append(item)

    return normalized


def _normalize_int_list(values: List[object]) -> List[int]:
    seen: set[int] = set()
    normalized: List[int] = []

    for value in values:
        try:
            item = int(value)
        except (TypeError, ValueError):
            continue
        if item in seen:
            continue
        seen.add(item)
        normalized.append(item)

    return normalized


def _normalize_custom_range(value: Optional[SharedTrendCustomRange]) -> Optional[Dict[str, str]]:
    if value is None:
        return None

    start = value.start.strip()
    end = value.end.strip()
    if not start or not end:
        return None

    return {"start": start, "end": end}


def _normalize_y_axis_scale(value: SharedTrendYAxisScale) -> Dict[str, str]:
    mode = "manual" if value.mode == "manual" else "auto"
    min_value = value.min if isinstance(value.min, str) else str(value.min or "")
    max_value = value.max if isinstance(value.max, str) else str(value.max or "")
    return {"mode": mode, "min": min_value, "max": max_value}


def _normalize_series_color_overrides(values: Dict[object, object]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        if not key.strip() or not value.strip():
            continue
        normalized[key] = value
    return normalized


def _normalize_payload(payload: SharedTrendConfigUpdate) -> SharedTrendConfigUpdate:
    data = payload.model_dump()
    return SharedTrendConfigUpdate(
        selectedVarIds=_normalize_string_list(list(data.get("selectedVarIds") or [])),
        selectedManualIds=_normalize_int_list(list(data.get("selectedManualIds") or [])),
        timeRange=max(1, int(data.get("timeRange") or 60)),
        rangeMode="absolute" if data.get("rangeMode") == "absolute" else "relative",
        customRange=_normalize_custom_range(payload.customRange),
        yAxisScale=_normalize_y_axis_scale(payload.yAxisScale),
        seriesColorOverrides=_normalize_series_color_overrides(
            dict(data.get("seriesColorOverrides") or {})
        ),
    )


def _build_response(row: Optional[SharedTrendConfig]) -> SharedTrendConfigResponse:
    if row is None:
        return _default_response()

    raw_custom_range = _decode_json(row.custom_range_json, None)
    raw_y_axis_scale = _decode_json(row.y_axis_scale_json, DEFAULT_Y_AXIS_SCALE)
    raw_series_colors = _decode_json(row.series_color_overrides_json, {})

    return SharedTrendConfigResponse(
        selectedVarIds=_normalize_string_list(
            list(_decode_json(row.selected_var_ids_json, []))
        ),
        selectedManualIds=_normalize_int_list(
            list(_decode_json(row.selected_manual_ids_json, []))
        ),
        timeRange=max(1, int(row.time_range or 60)),
        rangeMode="absolute" if row.range_mode == "absolute" else "relative",
        customRange=_normalize_custom_range(
            SharedTrendCustomRange(**raw_custom_range)
        )
        if isinstance(raw_custom_range, dict)
        else None,
        yAxisScale=_normalize_y_axis_scale(SharedTrendYAxisScale(**raw_y_axis_scale))
        if isinstance(raw_y_axis_scale, dict)
        else DEFAULT_Y_AXIS_SCALE,
        seriesColorOverrides=_normalize_series_color_overrides(
            raw_series_colors if isinstance(raw_series_colors, dict) else {}
        ),
        updatedAt=row.updated_at.isoformat() if row.updated_at else None,
    )


def _iter_local_trend_storage_files() -> List[Path]:
    configured_root = os.getenv("GLOBALTECH_HMI_STORAGE_DIR")
    storage_root = Path(configured_root) if configured_root else LOCAL_TREND_STORAGE_DIR
    if not storage_root.exists():
        return []

    candidates = [
        path
        for path in storage_root.iterdir()
        if path.is_file() and path.suffix.lower() in {".log", ".ldb"}
    ]
    return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)


def _extract_json_object(text: str, start_index: int) -> Optional[str]:
    depth = 0
    in_string = False
    escape = False

    for index in range(start_index, len(text)):
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue

        if char == "{":
            depth += 1
            continue

        if char == "}":
            depth -= 1
            if depth == 0:
                return text[start_index : index + 1]

    return None


def _parse_local_trend_config_candidate(raw: str, updated_at: Optional[str]) -> Optional[SharedTrendConfigResponse]:
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None

    if not isinstance(parsed, dict):
        return None

    response = SharedTrendConfigResponse(
        selectedVarIds=_normalize_string_list(list(parsed.get("selectedVarIds") or [])),
        selectedManualIds=_normalize_int_list(list(parsed.get("selectedManualIds") or [])),
        timeRange=max(1, int(parsed.get("timeRange") or 60)),
        rangeMode="absolute" if parsed.get("rangeMode") == "absolute" else "relative",
        customRange=_normalize_custom_range(
            SharedTrendCustomRange(**parsed["customRange"])
        )
        if isinstance(parsed.get("customRange"), dict)
        else None,
        yAxisScale=_normalize_y_axis_scale(
            SharedTrendYAxisScale(**parsed["yAxisScale"])
        )
        if isinstance(parsed.get("yAxisScale"), dict)
        else DEFAULT_Y_AXIS_SCALE,
        seriesColorOverrides=_normalize_series_color_overrides(
            parsed.get("seriesColorOverrides") if isinstance(parsed.get("seriesColorOverrides"), dict) else {}
        ),
        updatedAt=updated_at,
    )

    if not response.selectedVarIds and not response.selectedManualIds:
        return None

    return response


def _read_local_trend_config_fallback() -> Optional[SharedTrendConfigResponse]:
    for path in _iter_local_trend_storage_files():
        try:
            content = path.read_bytes().decode("latin-1", errors="ignore")
        except OSError:
            continue

        marker_indexes: List[int] = []
        search_offset = 0
        while True:
            next_index = content.find('{"selectedVarIds"', search_offset)
            if next_index == -1:
                break
            marker_indexes.append(next_index)
            search_offset = next_index + 1

        updated_at = datetime.fromtimestamp(path.stat().st_mtime).isoformat()
        for marker_index in reversed(marker_indexes):
            json_blob = _extract_json_object(content, marker_index)
            if not json_blob:
                continue

            parsed = _parse_local_trend_config_candidate(json_blob, updated_at)
            if parsed is not None:
                return parsed

    return None


async def read_shared_trend_config(
    session: AsyncSession,
) -> SharedTrendConfigResponse:
    row = await session.get(SharedTrendConfig, DEFAULT_SHARED_TREND_CONFIG_KEY)
    response = _build_response(row)
    if response.selectedVarIds or response.selectedManualIds:
        return response

    fallback = _read_local_trend_config_fallback()
    return fallback or response


async def write_shared_trend_config(
    session: AsyncSession,
    payload: SharedTrendConfigUpdate,
) -> SharedTrendConfigResponse:
    normalized = _normalize_payload(payload)
    row = await session.get(SharedTrendConfig, DEFAULT_SHARED_TREND_CONFIG_KEY)

    if row is None:
        row = SharedTrendConfig(key=DEFAULT_SHARED_TREND_CONFIG_KEY)
        session.add(row)

    row.selected_var_ids_json = json.dumps(normalized.selectedVarIds)
    row.selected_manual_ids_json = json.dumps(normalized.selectedManualIds)
    row.time_range = normalized.timeRange
    row.range_mode = normalized.rangeMode
    row.custom_range_json = (
        json.dumps(normalized.customRange) if normalized.customRange is not None else None
    )
    row.y_axis_scale_json = json.dumps(normalized.yAxisScale)
    row.series_color_overrides_json = json.dumps(normalized.seriesColorOverrides)

    await session.commit()
    await session.refresh(row)
    return _build_response(row)
