import base64
import html as html_utils
import io
import math
import re
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Literal, Optional, Tuple, Union, Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.core.config import settings
from app.db.models import ManualTrendPoint, TrendData
from app.db.session import get_session

router = APIRouter(prefix="/reports", tags=["Reports"])

EMAIL_REGEX = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
logger = logging.getLogger(__name__)


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


class SendTrendReportEmailRequest(BaseModel):
    recipients: List[str] = Field(..., min_length=1)
    privateMode: bool = False
    subject: Optional[str] = None
    note: Optional[str] = None
    timeRange: TrendReportTimeRange
    series: List[TrendReportSeries] = Field(default_factory=list)
    images: List[TrendReportImageAttachment] = Field(default_factory=list)
    # TrendScreen currently works only with drive_avid, but we leave it optional for future use.
    deviceId: Optional[str] = None
    clientMotorInfo: Optional[ClientMotorInfo] = None

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


def _build_client_motor_info_table_html(info: Optional[ClientMotorInfo]) -> str:
    if not info:
        return ""

    def esc(v: Optional[str]) -> str:
        txt = (v or "").strip()
        return html_utils.escape(txt) if txt else "—"

    rows: List[Tuple[str, str]] = [
        ("Customer", esc(info.customer)),
        ("Model", esc(info.model)),
        ("Catalog", esc(info.catalog)),
        ("HP", esc(info.hp)),
        ("RPM", esc(info.rpm)),
        ("Volts", esc(info.volts)),
        ("Amps", esc(info.amps)),
        ("Hz", esc(info.hz)),
        ("Frame", esc(info.frame)),
        ("Duty", esc(info.duty)),
        ("Enclosure", esc(info.enclosure)),
        ("Temp Rise", esc(info.tempRise)),
        ("Service Factor", esc(info.serviceFactor)),
        ("Efficiency", esc(info.efficiency)),
        ("Inverter", esc(info.inverterRating)),
    ]

    extras: List[Tuple[str, str]] = []
    for e in (info.extras or []):
        label = (e.label or "").strip()
        if not label:
            continue
        extras.append((html_utils.escape(label), esc(e.value)))

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
      <h3 style="margin: 16px 0 8px 0;">Client &amp; Motor</h3>
      <table style="border-collapse: collapse; width: 100%; max-width: 720px; font-size: 13px;">
        <tbody>
          {body}
        </tbody>
      </table>
    """.strip()


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


@router.post("/trend-email")
async def send_trend_report_email(
    payload: SendTrendReportEmailRequest,
    session: AsyncSession = Depends(get_session),
):
    logger.info(
        "[reports] request recipients=%s private=%s series=%s images=%s",
        len(payload.recipients),
        payload.privateMode,
        len(payload.series),
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

    # --- Series selection ---
    sensor_ids: List[str] = []
    manual_ids: List[int] = []
    for s in payload.series:
        if s.kind == "sensor":
            sensor_ids.append(s.parameterId)
        else:
            manual_ids.append(s.seriesId)

    sensor_ids = list(dict.fromkeys(sensor_ids))
    manual_ids = list(dict.fromkeys(manual_ids))

    # --- Fetch data (range) ---
    sensor_points_by_pid: Dict[str, List[Tuple[float, float]]] = {pid: [] for pid in sensor_ids}
    manual_points_by_sid: Dict[int, List[Tuple[float, float]]] = {sid: [] for sid in manual_ids}

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

        # Last point before start for initial forward-fill (typically up to 15 queries)
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
                # Insert at the beginning (it's before start)
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

        # Last point before start for initial forward-fill
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

    # --- Sampling / grid ---
    sample_seconds = _derive_sample_seconds(sensor_points_by_pid)
    start_epoch = _to_utc_epoch_seconds(start)
    end_epoch = _to_utc_epoch_seconds(end)
    grid_epochs, sample_seconds = _build_grid_epochs(start_epoch, end_epoch, sample_seconds)
    if not grid_epochs:
        raise HTTPException(status_code=400, detail="Invalid or empty time range")
    logger.info("[reports] grid rows=%s sample_seconds=%.3f", len(grid_epochs), sample_seconds)

    # --- Columns (headers + points) ---
    headers_seen: Dict[str, int] = {}
    columns: List[Dict[str, object]] = []
    for s in payload.series:
        unit = (s.unit or "").strip()
        base = f"{s.label} ({unit})" if unit else s.label
        base = re.sub(r"\s+", " ", (base or "").strip()) or "Series"

        header = base
        if header in headers_seen:
            # Primero intentamos desambiguar por kind, luego con un contador
            kind_suffix = "manual" if s.kind == "manual" else "sensor"
            header = f"{base} [{kind_suffix}]"
        if header in headers_seen:
            headers_seen[base] = headers_seen.get(base, 1) + 1
            header = f"{header} #{headers_seen[base]}"

        headers_seen[header] = 1

        if s.kind == "sensor":
            pts = sensor_points_by_pid.get(s.parameterId, [])
        else:
            pts = manual_points_by_sid.get(s.seriesId, [])
        columns.append({"header": header, "points": pts})

    # --- XLSX ---
    generated_at_iso = datetime.now(tz=timezone.utc).isoformat()
    start_iso = datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat()
    end_iso = datetime.fromtimestamp(end_epoch, tz=timezone.utc).isoformat()

    xlsx_bytes = _build_xlsx_bytes(
        start_iso=start_iso,
        end_iso=end_iso,
        generated_at_iso=generated_at_iso,
        sample_seconds=sample_seconds,
        grid_epochs=grid_epochs,
        columns=columns,
        client_motor_info=payload.clientMotorInfo,
    )
    logger.info("[reports] xlsx bytes=%s columns=%s", len(xlsx_bytes), len(columns))

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
    client_motor_html = _build_client_motor_info_table_html(payload.clientMotorInfo)
    html = f"""
    <div style="font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;">
      <h2 style="margin: 0 0 8px 0;">Trend Report</h2>
      <p style="margin: 0 0 12px 0;">
        Generated at: <strong>{generated_at_iso}</strong><br/>
        Range (UTC): <strong>{start_iso}</strong> to <strong>{end_iso}</strong><br/>
        Sample: <strong>{sample_seconds:.2f}s</strong><br/>
        Series: <strong>{len(payload.series)}</strong><br/>
        Attachments: <strong>{len(attachments)}</strong>
      </p>
      {note_block}
      {client_motor_html}
      <p style="margin: 0;">This email includes the Excel report and trend snapshots as attachments.</p>
    </div>
    """.strip()

    resend.api_key = settings.RESEND_API_KEY
    params: Dict[str, object] = {
        "from": "GlobalTech <onboarding@resend.dev>",
        "subject": subject,
        "html": html,
        "attachments": attachments,
    }

    if payload.privateMode:
        # In private mode, we hide the list using BCC
        params["to"] = ["onboarding@resend.dev"]
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

    resend.api_key = settings.RESEND_API_KEY
    
    # Prepare attachments - MUST base64 encode it.
    content_b64 = base64.b64encode(content).decode("utf-8")

    params: Dict[str, object] = {
        "from": "GlobalTech <onboarding@resend.dev>",
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
