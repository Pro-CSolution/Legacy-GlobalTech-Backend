import base64
import html as html_utils
import io
import math
import re
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Literal, Optional, Tuple, Union, Annotated

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field, field_validator
from sqlmodel import select

from app.core.config import settings
from app.db.models import ManualTrendPoint, TrendData
from app.db.session import async_session_factory, is_db_ready

router = APIRouter(prefix="/reports", tags=["Reports"])

EMAIL_REGEX = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
ALLOWED_EXCEL_SAMPLE_INTERVAL_VALUES = (0.25, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 15.0, 20.0, 30.0, 45.0)
ALLOWED_EXCEL_SAMPLE_SECONDS = tuple(
    sorted(
        {
            *ALLOWED_EXCEL_SAMPLE_INTERVAL_VALUES,
            *(value * 60.0 for value in ALLOWED_EXCEL_SAMPLE_INTERVAL_VALUES),
        }
    )
)
logger = logging.getLogger(__name__)


def _get_resend_from_address() -> str:
    from_email = (settings.RESEND_FROM_EMAIL or "").strip()
    from_name = (settings.RESEND_FROM_NAME or "GlobalTech").strip()
    if not from_email:
        raise HTTPException(status_code=500, detail="RESEND_FROM_EMAIL is not configured")
    return f"{from_name} <{from_email}>"


class TrendReportTimeRange(BaseModel):
    start: datetime
    end: datetime


class TrendReportImageAttachment(BaseModel):
    filename: str
    mimeType: Literal["image/png", "image/jpeg"]
    contentBase64: str

    @field_validator("filename")
    @classmethod
    def _sanitize_filename(cls, v: str) -> str:
        cleaned = (v or "").strip()
        cleaned = re.sub(r"[\\/:*?\"<>|]+", "-", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if not cleaned:
            raise ValueError("Empty filename")
        return cleaned

    @field_validator("contentBase64")
    @classmethod
    def _validate_base64(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError("contentBase64 required")
        # Lightweight validation: check if it looks like base64 (without decoding full bytes)
        if len(v) < 8:
            raise ValueError("Invalid contentBase64")
        return v


class TrendReportSensorSeries(BaseModel):
    kind: Literal["sensor"]
    parameterId: str
    label: str
    unit: Optional[str] = None


class TrendReportManualSeries(BaseModel):
    kind: Literal["manual"]
    seriesId: int
    label: str
    unit: Optional[str] = None


TrendReportSeries = Annotated[
    Union[TrendReportSensorSeries, TrendReportManualSeries], Field(discriminator="kind")
]


class TrendReportWorkbookPoint(BaseModel):
    x: float
    y: float

    @field_validator("x", "y")
    @classmethod
    def _validate_finite_number(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("Point values must be finite numbers")
        return float(v)


class TrendReportWorkbookSensorSeries(TrendReportSensorSeries):
    points: List[TrendReportWorkbookPoint] = Field(default_factory=list)


class TrendReportWorkbookManualSeries(TrendReportManualSeries):
    points: List[TrendReportWorkbookPoint] = Field(default_factory=list)


TrendReportWorkbookSeries = Annotated[
    Union[TrendReportWorkbookSensorSeries, TrendReportWorkbookManualSeries],
    Field(discriminator="kind"),
]


class TrendReportWorkbookSheet(BaseModel):
    name: str
    series: List[TrendReportWorkbookSeries] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def _sanitize_name(cls, v: str) -> str:
        cleaned = re.sub(r"[\[\]:*?/\\]+", " ", (v or "").strip())
        cleaned = re.sub(r"\s+", " ", cleaned).strip().strip("'")
        if not cleaned:
            raise ValueError("Sheet name required")
        return cleaned[:31]


class ClientMotorExtraField(BaseModel):
    label: str = ""
    value: str = ""

    @field_validator("label", "value")
    @classmethod
    def _trim_strings(cls, v: str) -> str:
        if not isinstance(v, str):
            return ""
        return v.strip()


class ClientMotorInfo(BaseModel):
    customer: Optional[str] = None
    driveModel: Optional[str] = None
    model: Optional[str] = None
    catalog: Optional[str] = None
    hp: Optional[str] = None
    rpm: Optional[str] = None
    volts: Optional[str] = None
    amps: Optional[str] = None
    hz: Optional[str] = None
    frame: Optional[str] = None
    duty: Optional[str] = None
    enclosure: Optional[str] = None
    tempRise: Optional[str] = None
    serviceFactor: Optional[str] = None
    efficiency: Optional[str] = None
    inverterRating: Optional[str] = None
    connection: Optional[str] = None
    maxOperating: Optional[str] = None
    extras: List[ClientMotorExtraField] = Field(default_factory=list)

    @field_validator("extras")
    @classmethod
    def _sanitize_extras(cls, v: List[ClientMotorExtraField]) -> List[ClientMotorExtraField]:
        if not isinstance(v, list):
            return []
        cleaned: List[ClientMotorExtraField] = []
        for item in v:
            if not item:
                continue
            label = (item.label or "").strip()
            value = (item.value or "").strip()
            if not label:
                continue
            cleaned.append(ClientMotorExtraField(label=label, value=value))
        return cleaned[:30]


class DualClientMotorInfo(BaseModel):
    drive1: ClientMotorInfo = Field(default_factory=ClientMotorInfo)
    drive2: ClientMotorInfo = Field(default_factory=ClientMotorInfo)


class SendTrendReportEmailRequest(BaseModel):
    recipients: List[str] = Field(..., min_length=1)
    privateMode: bool = False
    excelSampleSeconds: Optional[float] = None
    subject: Optional[str] = None
    note: Optional[str] = None
    timeRange: TrendReportTimeRange
    series: List[TrendReportSeries] = Field(default_factory=list)
    workbookSheets: List[TrendReportWorkbookSheet] = Field(default_factory=list)
    images: List[TrendReportImageAttachment] = Field(default_factory=list)
    # TrendScreen currently works only with drive_avid, but we leave it optional for future use.
    deviceId: Optional[str] = None
    clientMotorInfo: Optional[ClientMotorInfo] = None
    dualClientMotorInfo: Optional[DualClientMotorInfo] = None

    @field_validator("recipients")
    @classmethod
    def _validate_recipients(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("recipients required")
        cleaned: List[str] = []
        seen = set()
        for raw in v:
            if not isinstance(raw, str):
                continue
            email = raw.strip().lower()
            if not email:
                continue
            if not EMAIL_REGEX.match(email):
                raise ValueError(f"Invalid email: {raw}")
            if email in seen:
                continue
            seen.add(email)
            cleaned.append(email)
        if not cleaned:
            raise ValueError("Empty recipients")
        return cleaned

    @field_validator("excelSampleSeconds")
    @classmethod
    def _validate_excel_sample_seconds(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return None

        sample_seconds = float(v)
        if any(abs(sample_seconds - allowed) < 1e-9 for allowed in ALLOWED_EXCEL_SAMPLE_SECONDS):
            return sample_seconds

        raise ValueError(
            "excelSampleSeconds must be one of: "
            + ", ".join(str(value) for value in ALLOWED_EXCEL_SAMPLE_SECONDS)
        )


def _to_utc_epoch_seconds(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _to_excel_datetime_utc(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).replace(tzinfo=None)


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2 == 1:
        return float(sorted_vals[mid])
    return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2)


def _derive_sample_seconds(
    sensor_points_by_pid: Dict[str, List[Tuple[float, float]]],
) -> float:
    # 1) Derivar desde DB (sensor) cuando haya puntos suficientes
    for _, pts in sensor_points_by_pid.items():
        if len(pts) < 3:
            continue
        diffs: List[float] = []
        for i in range(1, len(pts)):
            dt = pts[i][0] - pts[i - 1][0]
            if dt > 0:
                diffs.append(dt)
        med = _median(diffs)
        if med and med > 0:
            # Defensive clamps
            return float(max(0.5, min(med, 60.0)))

    # 2) Fallback to configuration
    if settings.DATA_LOG_INTERVAL and settings.DATA_LOG_INTERVAL > 0:
        return float(settings.DATA_LOG_INTERVAL)
    if settings.MODBUS_POLL_INTERVAL and settings.MODBUS_POLL_INTERVAL > 0:
        return float(settings.MODBUS_POLL_INTERVAL)
    return 1.0


def _build_grid_epochs(start_epoch: float, end_epoch: float, sample_seconds: float) -> Tuple[List[float], float]:
    if end_epoch <= start_epoch:
        return [], sample_seconds

    max_rows = 20000
    total_seconds = end_epoch - start_epoch
    if sample_seconds <= 0:
        sample_seconds = 1.0

    rows = int(math.floor(total_seconds / sample_seconds)) + 1
    if rows > max_rows:
        factor = int(math.ceil(rows / max_rows))
        sample_seconds = sample_seconds * factor
        rows = int(math.floor(total_seconds / sample_seconds)) + 1

    epochs = [start_epoch + i * sample_seconds for i in range(rows)]
    # Ensure the last one doesn't exceed too much (due to float precision)
    epochs = [e for e in epochs if e <= end_epoch + 1e-6]
    return epochs, sample_seconds


def _derive_sample_seconds_from_columns(columns: List[Dict[str, object]]) -> float:
    for col in columns:
        pts = col.get("points") or []
        if not isinstance(pts, list) or len(pts) < 3:
            continue

        diffs: List[float] = []
        prev_epoch: Optional[float] = None
        for raw_point in pts:
            if not isinstance(raw_point, tuple) or len(raw_point) < 2:
                continue
            epoch = float(raw_point[0])
            if prev_epoch is not None:
                dt = epoch - prev_epoch
                if dt > 0:
                    diffs.append(dt)
            prev_epoch = epoch

        med = _median(diffs)
        if med and med > 0:
            return float(max(0.5, min(med, 60.0)))

    if settings.DATA_LOG_INTERVAL and settings.DATA_LOG_INTERVAL > 0:
        return float(settings.DATA_LOG_INTERVAL)
    if settings.MODBUS_POLL_INTERVAL and settings.MODBUS_POLL_INTERVAL > 0:
        return float(settings.MODBUS_POLL_INTERVAL)
    return 1.0


def _build_columns_from_workbook_sheet(
    sheet: TrendReportWorkbookSheet,
) -> List[Dict[str, object]]:
    headers_seen: Dict[str, int] = {}
    columns: List[Dict[str, object]] = []

    for series in sheet.series:
        unit = (series.unit or "").strip()
        base = f"{series.label} ({unit})" if unit else series.label
        base = re.sub(r"\s+", " ", (base or "").strip()) or "Series"

        header = base
        if header in headers_seen:
            kind_suffix = "manual" if series.kind == "manual" else "sensor"
            header = f"{base} [{kind_suffix}]"
        if header in headers_seen:
            headers_seen[base] = headers_seen.get(base, 1) + 1
            header = f"{header} #{headers_seen[base]}"

        headers_seen[header] = 1

        points = sorted(
            [
                (float(point.x), float(point.y))
                for point in series.points
                if math.isfinite(point.x) and math.isfinite(point.y)
            ],
            key=lambda item: item[0],
        )
        columns.append({"header": header, "points": points})

    return columns


def _client_motor_info_rows(info: Optional[ClientMotorInfo]) -> List[Tuple[str, str]]:
    if not info:
        return []

    def esc(v: Optional[str]) -> str:
        txt = (v or "").strip()
        return html_utils.escape(txt) if txt else "—"

    return [
        ("Customer", esc(info.customer)),
        ("Drive Model", esc(info.driveModel)),
        ("Model No.", esc(info.model)),
        ("Serial No.", esc(info.catalog)),
        ("Catalog No.", esc(info.hp)),
        ("Ambient Air Max", esc(info.frame)),
        ("Phase", esc(info.duty)),
        ("Rated AC Volts", esc(info.enclosure)),
        ("Rated Amps AC", esc(info.tempRise)),
        ("Rated Power Factor", esc(info.serviceFactor)),
        ("Rated RPM / Hz", esc(info.efficiency)),
        ("Horse Power", esc(info.inverterRating)),
        ("Connection", esc(info.connection)),
        ("Max Operating RPM/Hz", esc(info.maxOperating)),
    ]


def _client_motor_extra_rows(info: Optional[ClientMotorInfo]) -> List[Tuple[str, str]]:
    if not info:
        return []

    extras: List[Tuple[str, str]] = []
    for e in (info.extras or []):
        label = (e.label or "").strip()
        if not label:
            continue
        value = (e.value or "").strip()
        extras.append((html_utils.escape(label), html_utils.escape(value) if value else "—"))
    return extras


def _build_client_motor_info_section_html(title: str, info: Optional[ClientMotorInfo]) -> str:
    if not info:
        return ""

    rows = _client_motor_info_rows(info)
    extras = _client_motor_extra_rows(info)

    def row_html(label: str, value: str) -> str:
        return (
            "<tr>"
            f'<td style="padding:6px 8px;border:1px solid #e5e7eb;background:#f1f5f9;width:35%;"><strong>{label}</strong></td>'
            f'<td style="padding:6px 8px;border:1px solid #e5e7eb;">{value}</td>'
            "</tr>"
        )

    body = "".join(row_html(label, value) for (label, value) in rows)
    if extras:
        body += (
            '<tr><td colspan="2" style="padding:8px;border:1px solid #e5e7eb;background:#111827;color:#ffffff;"><strong>Custom Fields</strong></td></tr>'
        )
        body += "".join(row_html(label, value) for (label, value) in extras)

    return f"""
      <h3 style="margin: 16px 0 8px 0;">{html_utils.escape(title)}</h3>
      <table style="border-collapse: collapse; width: 100%; max-width: 720px; font-size: 13px; margin-bottom: 16px;">
        <tbody>
          {body}
        </tbody>
      </table>
    """.strip()


def _build_client_motor_info_table_html(info: Optional[ClientMotorInfo]) -> str:
    return _build_client_motor_info_section_html("Client & Motor", info)


def _build_dual_client_motor_info_table_html(info: Optional[DualClientMotorInfo]) -> str:
    if not info:
        return ""

    sections = [
        _build_client_motor_info_section_html("Motor #1 Details", info.drive1),
        _build_client_motor_info_section_html("Motor #2 Details", info.drive2),
    ]
    sections = [section for section in sections if section]
    if not sections:
        return ""

    return (
        '<div style="margin: 16px 0 0 0;">'
        '<h3 style="margin: 0 0 8px 0;">Motor Details</h3>'
        f"{''.join(sections)}"
        "</div>"
    )


def _build_xlsx_bytes(
    *,
    start_iso: str,
    end_iso: str,
    generated_at_iso: str,
    sample_seconds: float,
    grid_epochs: List[float],
    columns: List[Dict[str, object]],
    client_motor_info: Optional[ClientMotorInfo] = None,
) -> bytes:
    """
    columns: [{ 'header': str, 'points': List[(epoch_seconds,value)] }]
    """
    try:
        import xlsxwriter  # type: ignore
    except Exception as err:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"xlsxwriter dependency not available: {err}")

    buffer = io.BytesIO()
    workbook = xlsxwriter.Workbook(buffer, {"in_memory": True})

    ws = workbook.add_worksheet("Trend Report")

    fmt_meta_key = workbook.add_format({"bold": True})
    fmt_meta_val = workbook.add_format({})
    fmt_header = workbook.add_format(
        {"bold": True, "bg_color": "#111827", "font_color": "#ffffff", "border": 1}
    )
    fmt_time = workbook.add_format({"num_format": "yyyy-mm-dd hh:mm:ss", "border": 1})
    fmt_num = workbook.add_format({"num_format": "0.00", "border": 1})

    # --- Client & Motor sheet (if provided) ---
    ws_info = workbook.add_worksheet("Client & Motor")
    ws_info.write(0, 0, "Field", fmt_header)
    ws_info.write(0, 1, "Value", fmt_header)
    ws_info.set_column(0, 0, 22)
    ws_info.set_column(1, 1, 56)

    if client_motor_info:
        def excel_val(v: Optional[str]) -> str:
            txt = (v or "").strip()
            return txt if txt else "—"

        info_rows: List[Tuple[str, str]] = [
            ("Customer", excel_val(client_motor_info.customer)),
            ("Model", excel_val(client_motor_info.model)),
            ("Catalog", excel_val(client_motor_info.catalog)),
            ("HP", excel_val(client_motor_info.hp)),
            ("RPM", excel_val(client_motor_info.rpm)),
            ("Volts", excel_val(client_motor_info.volts)),
            ("Amps", excel_val(client_motor_info.amps)),
            ("Hz", excel_val(client_motor_info.hz)),
            ("Frame", excel_val(client_motor_info.frame)),
            ("Duty", excel_val(client_motor_info.duty)),
            ("Enclosure", excel_val(client_motor_info.enclosure)),
            ("Temp Rise", excel_val(client_motor_info.tempRise)),
            ("Service Factor", excel_val(client_motor_info.serviceFactor)),
            ("Efficiency", excel_val(client_motor_info.efficiency)),
            ("Inverter", excel_val(client_motor_info.inverterRating)),
        ]

        row = 1
        for k, v in info_rows:
            ws_info.write(row, 0, k, fmt_meta_key)
            ws_info.write(row, 1, v, fmt_meta_val)
            row += 1

        extras = client_motor_info.extras or []
        if extras:
            row += 1
            ws_info.write(row, 0, "Custom Fields", fmt_header)
            ws_info.write(row, 1, "", fmt_header)
            row += 1
            for e in extras:
                label = (e.label or "").strip()
                if not label:
                    continue
                ws_info.write(row, 0, label, fmt_meta_key)
                ws_info.write(row, 1, excel_val(e.value), fmt_meta_val)
                row += 1
    else:
        ws_info.write(1, 0, "Info", fmt_meta_key)
        ws_info.write(1, 1, "No client motor info provided", fmt_meta_val)

    # Meta (single sheet, at the top)
    ws.write(0, 0, "Generated At (UTC)", fmt_meta_key)
    ws.write(0, 1, generated_at_iso, fmt_meta_val)
    ws.write(1, 0, "Start (UTC)", fmt_meta_key)
    ws.write(1, 1, start_iso, fmt_meta_val)
    ws.write(2, 0, "End (UTC)", fmt_meta_key)
    ws.write(2, 1, end_iso, fmt_meta_val)
    ws.write(3, 0, "Sample Seconds", fmt_meta_key)
    ws.write_number(3, 1, float(sample_seconds), fmt_meta_val)
    ws.write(4, 0, "Rows", fmt_meta_key)
    ws.write_number(4, 1, len(grid_epochs), fmt_meta_val)

    header_row = 6

    ws.write(header_row, 0, "Timestamp (UTC)", fmt_header)
    for col_idx, col in enumerate(columns, start=1):
        ws.write(header_row, col_idx, str(col["header"]), fmt_header)

    ws.freeze_panes(header_row + 1, 1)
    ws.autofilter(header_row, 0, header_row, len(columns))

    ws.set_column(0, 0, 22)
    ws.set_column(1, max(len(columns), 1), 18)

    # State per column for forward-fill
    states: List[Dict[str, object]] = []
    for col in columns:
        pts = col.get("points") or []
        states.append({"idx": 0, "last": None, "points": pts})

    for row_offset, epoch in enumerate(grid_epochs):
        excel_dt = _to_excel_datetime_utc(epoch)
        row = header_row + 1 + row_offset
        ws.write_datetime(row, 0, excel_dt, fmt_time)

        for col_idx, st in enumerate(states, start=1):
            points = st["points"]  # type: ignore[assignment]
            idx = int(st["idx"])  # type: ignore[arg-type]
            last = st["last"]  # type: ignore[assignment]

            # points: List[Tuple[float,float]]
            while idx < len(points) and points[idx][0] <= epoch + 1e-6:  # type: ignore[index]
                last = float(points[idx][1])  # type: ignore[index]
                idx += 1

            st["idx"] = idx
            st["last"] = last

            if last is None:
                continue
            ws.write_number(row, col_idx, float(last), fmt_num)

    workbook.close()
    buffer.seek(0)
    return buffer.getvalue()


def _sanitize_name_for_excel(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[\[\]:*?/\\]+", " ", (value or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip("'")
    return (cleaned or fallback)[:31]


def _write_client_motor_sheet_v2(
    workbook,
    *,
    fmt_header,
    fmt_meta_key,
    fmt_meta_val,
    client_motor_info: Optional[ClientMotorInfo],
    dual_client_motor_info: Optional[DualClientMotorInfo],
) -> None:
    ws_info = workbook.add_worksheet("Motor Details")
    ws_info.write(0, 0, "Field", fmt_header)
    ws_info.write(0, 1, "Value", fmt_header)
    ws_info.set_column(0, 0, 22)
    ws_info.set_column(1, 1, 56)

    def excel_val(v: str) -> str:
        txt = (v or "").strip()
        return txt if txt else "—"

    def write_section(start_row: int, title: str, info: Optional[ClientMotorInfo]) -> int:
        ws_info.write(start_row, 0, title, fmt_header)
        ws_info.write(start_row, 1, "", fmt_header)
        row = start_row + 1

        if not info:
            ws_info.write(row, 0, "Info", fmt_meta_key)
            ws_info.write(row, 1, "No client motor info provided", fmt_meta_val)
            return row + 2

        for key, value in _client_motor_info_rows(info):
            ws_info.write(row, 0, key, fmt_meta_key)
            ws_info.write(row, 1, excel_val(html_utils.unescape(value)), fmt_meta_val)
            row += 1

        extras = _client_motor_extra_rows(info)
        if extras:
            ws_info.write(row, 0, "Custom Fields", fmt_header)
            ws_info.write(row, 1, "", fmt_header)
            row += 1
            for label, value in extras:
                ws_info.write(row, 0, html_utils.unescape(label), fmt_meta_key)
                ws_info.write(row, 1, excel_val(html_utils.unescape(value)), fmt_meta_val)
                row += 1

        return row + 1

    if dual_client_motor_info:
        next_row = write_section(1, "Motor #1 Details", dual_client_motor_info.drive1)
        write_section(next_row, "Motor #2 Details", dual_client_motor_info.drive2)
    else:
        write_section(1, "Client & Motor", client_motor_info)


def _write_trend_sheet_v2(
    workbook,
    *,
    sheet_name: str,
    start_iso: str,
    end_iso: str,
    generated_at_iso: str,
    sample_seconds: float,
    grid_epochs: List[float],
    columns: List[Dict[str, object]],
    fmt_meta_key,
    fmt_meta_val,
    fmt_header,
    fmt_time,
    fmt_num,
) -> None:
    ws = workbook.add_worksheet(sheet_name)

    ws.write(0, 0, "Generated At (UTC)", fmt_meta_key)
    ws.write(0, 1, generated_at_iso, fmt_meta_val)
    ws.write(1, 0, "Start (UTC)", fmt_meta_key)
    ws.write(1, 1, start_iso, fmt_meta_val)
    ws.write(2, 0, "End (UTC)", fmt_meta_key)
    ws.write(2, 1, end_iso, fmt_meta_val)
    ws.write(3, 0, "Sample Seconds", fmt_meta_key)
    ws.write_number(3, 1, float(sample_seconds), fmt_meta_val)
    ws.write(4, 0, "Rows", fmt_meta_key)
    ws.write_number(4, 1, len(grid_epochs), fmt_meta_val)
    ws.write(5, 0, "Series", fmt_meta_key)
    ws.write_number(5, 1, len(columns), fmt_meta_val)

    header_row = 7
    ws.write(header_row, 0, "Timestamp (UTC)", fmt_header)
    for col_idx, col in enumerate(columns, start=1):
        ws.write(header_row, col_idx, str(col["header"]), fmt_header)

    ws.freeze_panes(header_row + 1, 1)
    ws.autofilter(header_row, 0, header_row, len(columns))
    ws.set_column(0, 0, 22)
    ws.set_column(1, max(len(columns), 1), 18)

    states: List[Dict[str, object]] = []
    for col in columns:
        pts = col.get("points") or []
        states.append({"idx": 0, "last": None, "points": pts})

    for row_offset, epoch in enumerate(grid_epochs):
        excel_dt = _to_excel_datetime_utc(epoch)
        row = header_row + 1 + row_offset
        ws.write_datetime(row, 0, excel_dt, fmt_time)

        for col_idx, st in enumerate(states, start=1):
            points = st["points"]  # type: ignore[assignment]
            idx = int(st["idx"])  # type: ignore[arg-type]
            last = st["last"]  # type: ignore[assignment]

            while idx < len(points) and points[idx][0] <= epoch + 1e-6:  # type: ignore[index]
                last = float(points[idx][1])  # type: ignore[index]
                idx += 1

            st["idx"] = idx
            st["last"] = last

            if last is None:
                continue
            ws.write_number(row, col_idx, float(last), fmt_num)


def _build_multi_sheet_xlsx_bytes(
    *,
    start_iso: str,
    end_iso: str,
    generated_at_iso: str,
    sheets: List[Dict[str, object]],
    client_motor_info: Optional[ClientMotorInfo] = None,
    dual_client_motor_info: Optional[DualClientMotorInfo] = None,
    include_client_motor_sheet: bool = True,
) -> bytes:
    try:
        import xlsxwriter  # type: ignore
    except Exception as err:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"xlsxwriter dependency not available: {err}")

    buffer = io.BytesIO()
    workbook = xlsxwriter.Workbook(buffer, {"in_memory": True})

    fmt_meta_key = workbook.add_format({"bold": True})
    fmt_meta_val = workbook.add_format({})
    fmt_header = workbook.add_format(
        {"bold": True, "bg_color": "#111827", "font_color": "#ffffff", "border": 1}
    )
    fmt_time = workbook.add_format({"num_format": "yyyy-mm-dd hh:mm:ss", "border": 1})
    fmt_num = workbook.add_format({"num_format": "0.00", "border": 1})

    if include_client_motor_sheet:
        _write_client_motor_sheet_v2(
            workbook,
            fmt_header=fmt_header,
            fmt_meta_key=fmt_meta_key,
            fmt_meta_val=fmt_meta_val,
            client_motor_info=client_motor_info,
            dual_client_motor_info=dual_client_motor_info,
        )

    for idx, sheet in enumerate(sheets, start=1):
        _write_trend_sheet_v2(
            workbook,
            sheet_name=_sanitize_name_for_excel(
                str(sheet.get("name") or f"Trend {idx}"), f"Trend {idx}"
            ),
            start_iso=start_iso,
            end_iso=end_iso,
            generated_at_iso=generated_at_iso,
            sample_seconds=float(sheet.get("sample_seconds") or 1.0),
            grid_epochs=list(sheet.get("grid_epochs") or []),
            columns=list(sheet.get("columns") or []),
            fmt_meta_key=fmt_meta_key,
            fmt_meta_val=fmt_meta_val,
            fmt_header=fmt_header,
            fmt_time=fmt_time,
            fmt_num=fmt_num,
        )

    workbook.close()
    buffer.seek(0)
    return buffer.getvalue()


@router.post("/trend-email")
async def send_trend_report_email(
    payload: SendTrendReportEmailRequest,
):
    logger.info(
        "[reports] request recipients=%s private=%s series=%s workbook_sheets=%s images=%s",
        len(payload.recipients),
        payload.privateMode,
        len(payload.series),
        len(payload.workbookSheets),
        len(payload.images),
    )
    logger.debug("[reports] subject=%s note_len=%s", payload.subject, len(payload.note or ""))

    if not settings.RESEND_API_KEY:
        raise HTTPException(status_code=500, detail="RESEND_API_KEY is not configured")

    # Local import so the app starts even if the dependency is missing
    try:
        import resend  # type: ignore
    except Exception as err:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Resend dependency not available: {err}")

    start = payload.timeRange.start
    end = payload.timeRange.end
    if start >= end:
        raise HTTPException(status_code=400, detail="timeRange.start must be before timeRange.end")

    device_id = payload.deviceId or "drive_avid"
    start_epoch = _to_utc_epoch_seconds(start)
    end_epoch = _to_utc_epoch_seconds(end)
    generated_at_iso = datetime.now(tz=timezone.utc).isoformat()
    start_iso = datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat()
    end_iso = datetime.fromtimestamp(end_epoch, tz=timezone.utc).isoformat()
    workbook_sheets: List[Dict[str, object]] = []
    selected_excel_sample_seconds = payload.excelSampleSeconds

    if payload.workbookSheets:
        for idx, sheet in enumerate(payload.workbookSheets, start=1):
            columns = _build_columns_from_workbook_sheet(sheet)
            sample_seconds = (
                selected_excel_sample_seconds
                if selected_excel_sample_seconds is not None
                else _derive_sample_seconds_from_columns(columns)
            )
            grid_epochs, sample_seconds = _build_grid_epochs(start_epoch, end_epoch, sample_seconds)
            if not grid_epochs:
                raise HTTPException(status_code=400, detail="Invalid or empty time range")

            workbook_sheets.append(
                {
                    "name": sheet.name or f"Trend {idx}",
                    "columns": columns,
                    "grid_epochs": grid_epochs,
                    "sample_seconds": sample_seconds,
                }
            )

        logger.info(
            "[reports] using client workbook data sheets=%s",
            len(workbook_sheets),
        )
    else:
        if not is_db_ready():
            raise HTTPException(
                status_code=503,
                detail="Database is not ready yet and no client workbook data was provided",
            )

        sensor_ids: List[str] = []
        manual_ids: List[int] = []
        for s in payload.series:
            if s.kind == "sensor":
                sensor_ids.append(s.parameterId)
            else:
                manual_ids.append(s.seriesId)

        sensor_ids = list(dict.fromkeys(sensor_ids))
        manual_ids = list(dict.fromkeys(manual_ids))

        sensor_points_by_pid: Dict[str, List[Tuple[float, float]]] = {pid: [] for pid in sensor_ids}
        manual_points_by_sid: Dict[int, List[Tuple[float, float]]] = {sid: [] for sid in manual_ids}

        async with async_session_factory() as session:
            if sensor_ids:
                logger.info("[reports] fetching sensor trend device=%s params=%s", device_id, len(sensor_ids))
                stmt = (
                    select(TrendData)
                    .where(
                        TrendData.device_id == device_id,
                        TrendData.parameter_id.in_(sensor_ids),
                        TrendData.time >= start,
                        TrendData.time <= end,
                    )
                    .order_by(TrendData.parameter_id, TrendData.time.asc())
                )
                result = await session.execute(stmt)
                rows = result.scalars().all()
                logger.info("[reports] sensor rows=%s", len(rows))
                for row in rows:
                    epoch = _to_utc_epoch_seconds(row.time)
                    sensor_points_by_pid.setdefault(row.parameter_id, []).append((epoch, float(row.value)))

                for pid in sensor_ids:
                    stmt_prev = (
                        select(TrendData)
                        .where(
                            TrendData.device_id == device_id,
                            TrendData.parameter_id == pid,
                            TrendData.time < start,
                        )
                        .order_by(TrendData.time.desc())
                        .limit(1)
                    )
                    prev_res = await session.execute(stmt_prev)
                    prev = prev_res.scalars().first()
                    if prev:
                        epoch = _to_utc_epoch_seconds(prev.time)
                        sensor_points_by_pid.setdefault(pid, [])
                        sensor_points_by_pid[pid].insert(0, (epoch, float(prev.value)))

            if manual_ids:
                logger.info("[reports] fetching manual points series=%s", len(manual_ids))
                stmt = (
                    select(ManualTrendPoint)
                    .where(
                        ManualTrendPoint.series_id.in_(manual_ids),
                        ManualTrendPoint.time >= start,
                        ManualTrendPoint.time <= end,
                    )
                    .order_by(ManualTrendPoint.series_id, ManualTrendPoint.time.asc())
                )
                result = await session.execute(stmt)
                rows = result.scalars().all()
                logger.info("[reports] manual rows=%s", len(rows))
                for row in rows:
                    epoch = _to_utc_epoch_seconds(row.time)
                    manual_points_by_sid.setdefault(row.series_id, []).append((epoch, float(row.value)))

                for sid in manual_ids:
                    stmt_prev = (
                        select(ManualTrendPoint)
                        .where(ManualTrendPoint.series_id == sid, ManualTrendPoint.time < start)
                        .order_by(ManualTrendPoint.time.desc())
                        .limit(1)
                    )
                    prev_res = await session.execute(stmt_prev)
                    prev = prev_res.scalars().first()
                    if prev:
                        epoch = _to_utc_epoch_seconds(prev.time)
                        manual_points_by_sid.setdefault(sid, [])
                        manual_points_by_sid[sid].insert(0, (epoch, float(prev.value)))

        sample_seconds = (
            selected_excel_sample_seconds
            if selected_excel_sample_seconds is not None
            else _derive_sample_seconds(sensor_points_by_pid)
        )
        grid_epochs, sample_seconds = _build_grid_epochs(start_epoch, end_epoch, sample_seconds)
        if not grid_epochs:
            raise HTTPException(status_code=400, detail="Invalid or empty time range")

        headers_seen: Dict[str, int] = {}
        columns: List[Dict[str, object]] = []
        for s in payload.series:
            unit = (s.unit or "").strip()
            base = f"{s.label} ({unit})" if unit else s.label
            base = re.sub(r"\s+", " ", (base or "").strip()) or "Series"

            header = base
            if header in headers_seen:
                kind_suffix = "manual" if s.kind == "manual" else "sensor"
                header = f"{base} [{kind_suffix}]"
            if header in headers_seen:
                headers_seen[base] = headers_seen.get(base, 1) + 1
                header = f"{header} #{headers_seen[base]}"

            headers_seen[header] = 1
            pts = sensor_points_by_pid.get(s.parameterId, []) if s.kind == "sensor" else manual_points_by_sid.get(s.seriesId, [])
            columns.append({"header": header, "points": pts})

        workbook_sheets.append(
            {
                "name": "Trend Report",
                "columns": columns,
                "grid_epochs": grid_epochs,
                "sample_seconds": sample_seconds,
            }
        )
        logger.info("[reports] fallback workbook sheets=%s", len(workbook_sheets))

    xlsx_bytes = _build_multi_sheet_xlsx_bytes(
        start_iso=start_iso,
        end_iso=end_iso,
        generated_at_iso=generated_at_iso,
        sheets=workbook_sheets,
        client_motor_info=payload.clientMotorInfo,
        dual_client_motor_info=payload.dualClientMotorInfo,
        include_client_motor_sheet=bool(payload.clientMotorInfo or payload.dualClientMotorInfo),
    )
    logger.info("[reports] xlsx bytes=%s sheets=%s", len(xlsx_bytes), len(workbook_sheets))
    report_series_count = sum(len(sheet.get("columns") or []) for sheet in workbook_sheets)
    sample_values = [float(sheet.get("sample_seconds") or 1.0) for sheet in workbook_sheets]
    rounded_sample_values = {round(value, 3) for value in sample_values}
    if not sample_values:
        sample_summary = "n/a"
    elif len(rounded_sample_values) == 1:
        sample_summary = f"{sample_values[0]:.2f}s"
    else:
        sample_summary = ", ".join(
            f'{_sanitize_name_for_excel(str(sheet.get("name") or f"Trend {idx + 1}"), f"Trend {idx + 1}")}: {sample_values[idx]:.2f}s'
            for idx, sheet in enumerate(workbook_sheets)
        )

    # --- Attachments ---
    xlsx_filename = re.sub(r"[\\/:*?\"<>|]+", "-", f"Trend Report - {start_iso}_to_{end_iso}.xlsx")
    xlsx_b64 = base64.b64encode(xlsx_bytes).decode("utf-8")

    attachments: List[Dict[str, str]] = [{"content": xlsx_b64, "filename": xlsx_filename}]
    attachments.extend(
        [
            {
                "content": img.contentBase64,
                "filename": img.filename,
            }
            for img in payload.images
        ]
    )

    # Limit: 40MB including base64
    total_base64_size = sum(len(a.get("content", "")) for a in attachments)
    max_base64 = 40 * 1024 * 1024
    if total_base64_size >= max_base64:
        raise HTTPException(
            status_code=413,
            detail=f"Attachments too large for Resend (base64={total_base64_size} bytes)",
        )
    logger.info("[reports] attachments=%s total_base64=%s", len(attachments), total_base64_size)
    logger.debug("[reports] attachment_names=%s", [a.get("filename") for a in attachments])

    # --- Email content ---
    subject = (payload.subject or "").strip() or f"GlobalTech Trend Report - {start_iso} to {end_iso}"
    note = (payload.note or "").strip()
    note_html = html_utils.escape(note)
    note_block = (
        f'<p style="margin: 0 0 12px 0;"><strong>Note:</strong> {note_html}</p>' if note else ""
    )
    client_motor_html = (
        _build_dual_client_motor_info_table_html(payload.dualClientMotorInfo)
        if payload.dualClientMotorInfo
        else _build_client_motor_info_table_html(payload.clientMotorInfo)
    )
    html = f"""
    <div style="font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;">
      <h2 style="margin: 0 0 8px 0;">Trend Report</h2>
      <p style="margin: 0 0 12px 0;">
        Generated at: <strong>{generated_at_iso}</strong><br/>
        Range (UTC): <strong>{start_iso}</strong> to <strong>{end_iso}</strong><br/>
        Sheets: <strong>{len(workbook_sheets)}</strong><br/>
        Sample: <strong>{sample_summary}</strong><br/>
        Series: <strong>{report_series_count}</strong><br/>
        Attachments: <strong>{len(attachments)}</strong>
      </p>
      {note_block}
      {client_motor_html}
      <p style="margin: 0;">This email includes the Excel report and trend snapshots as attachments.</p>
    </div>
    """.strip()

    from_address = _get_resend_from_address()
    resend.api_key = settings.RESEND_API_KEY
    params: Dict[str, object] = {
        "from": from_address,
        "subject": subject,
        "html": html,
        "attachments": attachments,
    }

    if payload.privateMode:
        # In private mode, we hide the list using BCC
        params["to"] = [settings.RESEND_FROM_EMAIL]
        params["bcc"] = payload.recipients
    else:
        params["to"] = payload.recipients

    try:
        result = None
        # Defensive retry for intermittent TLS failures (e.g., SSLV3_ALERT_BAD_RECORD_MAC)
        for attempt in range(1, 4):
            try:
                logger.info("[reports] resend attempt=%s", attempt)
                result = resend.Emails.send(params)
                logger.info("[reports] resend ok result=%s", result)
                break
            except Exception as send_err:
                msg = str(send_err)
                logger.warning("[reports] resend attempt=%s failed err=%s", attempt, msg)
                if attempt >= 3:
                    raise
                # Short backoff (doesn't block the UI too much)
                await asyncio.sleep(0.6 * attempt)
    except Exception as err:
        logger.exception("[reports] resend failed")
        raise HTTPException(status_code=502, detail=f"Error sending email with Resend: {err}")

    # result usually contains {"id": "..."}
    email_id = None
    if isinstance(result, dict):
        email_id = result.get("id")

    return {
        "status": "sent",
        "id": email_id,
        "recipients": payload.recipients,
        "attachments": len(attachments),
    }


@router.post("/trend-email/video")
async def send_video_report_email(
    file: UploadFile = File(...),
    email: str = Form(...),
):
    if not settings.RESEND_API_KEY:
        raise HTTPException(status_code=500, detail="RESEND_API_KEY is not configured")

    try:
        import resend  # type: ignore
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Resend dependency not available: {err}")

    # Clean email
    recipients = [e.strip() for e in email.split(",") if e.strip()]
    if not recipients:
        raise HTTPException(status_code=400, detail="Recipients required")

    # Read file content
    content = await file.read()

    # Limit requested by the project: 39MB file.
    # Note: Resend uses base64 attachments and may fail with large files,
    # but here we allow up to 39MB as requested.
    max_raw = 39 * 1024 * 1024
    if len(content) > max_raw:
        raise HTTPException(status_code=413, detail="Video too large (max 39MB)")

    generated_at_iso = datetime.now(tz=timezone.utc).isoformat()
    filename = file.filename or f"recording-{generated_at_iso}.webm"

    html = f"""
    <div style="font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;">
      <h2 style="margin: 0 0 8px 0;">Screen Recording Report</h2>
      <p style="margin: 0 0 12px 0;">
        Generated at: <strong>{generated_at_iso}</strong><br/>
      </p>
      <p style="margin: 0;">This email includes the screen recording as an attachment.</p>
    </div>
    """.strip()

    from_address = _get_resend_from_address()
    resend.api_key = settings.RESEND_API_KEY
    
    # Prepare attachments - MUST base64 encode it.
    content_b64 = base64.b64encode(content).decode("utf-8")

    params: Dict[str, object] = {
        "from": from_address,
        "subject": f"GlobalTech Screen Recording - {generated_at_iso}",
        "html": html,
        "to": recipients,
        "attachments": [
            {
                "content": content_b64,
                "filename": filename,
            }
        ],
    }

    try:
        result = resend.Emails.send(params)
        return {
            "status": "sent",
            "id": result.get("id") if isinstance(result, dict) else None,
            "recipients": recipients
        }
    except Exception as err:
        logger.exception("[reports] video resend failed")
        raise HTTPException(status_code=502, detail=f"Error sending email: {err}")
