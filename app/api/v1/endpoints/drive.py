import json
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.config import parameter_registry
from app.core.paths import resolve_resource

router = APIRouter(prefix="/drive", tags=["Drive"])

MENUS_PATH = resolve_resource("config/drive_menus.json")
MENU_OPTIONS_CACHE: Optional[List[Dict[str, Any]]] = None
FAULT_CODES_PATH = resolve_resource("config/fault_codes.json")
FAULT_CODES_CACHE: Optional[List[Dict[str, Any]]] = None
ALLOWED_SCALE_FACTORS = {1, 10, 100, 1000}


class UpdateScaleFactorRequest(BaseModel):
    device_id: str
    scale_factor: int


def _load_menus() -> List[Dict[str, Any]]:
    global MENU_OPTIONS_CACHE
    if MENU_OPTIONS_CACHE is not None:
        return MENU_OPTIONS_CACHE
    if not MENUS_PATH.exists():
        raise HTTPException(status_code=500, detail="drive_menus.json no encontrado")
    with MENUS_PATH.open("r", encoding="utf-8") as f:
        MENU_OPTIONS_CACHE = json.load(f)
    return MENU_OPTIONS_CACHE or []


def _load_fault_codes() -> List[Dict[str, Any]]:
    global FAULT_CODES_CACHE
    if FAULT_CODES_CACHE is not None:
        return FAULT_CODES_CACHE
    if not FAULT_CODES_PATH.exists():
        raise HTTPException(status_code=500, detail="fault_codes.json no encontrado")
    with FAULT_CODES_PATH.open("r", encoding="utf-8") as f:
        FAULT_CODES_CACHE = json.load(f)
    return FAULT_CODES_CACHE or []


_range_list_regex = re.compile(r"^\s*\d+\s*=\s*[^,]+(,\s*\d+\s*=\s*[^,]+)+\s*$")


def _parse_range_options(range_text: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    if not range_text or not isinstance(range_text, str):
        return None
    if not _range_list_regex.match(range_text):
        return None
    options: List[Dict[str, Any]] = []
    for part in range_text.split(","):
        if "=" not in part:
            continue
        num_str, label = part.split("=", 1)
        try:
            value = int(num_str.strip())
        except ValueError:
            continue
        options.append({"value": value, "label": label.strip()})
    return options or None


def _matches_menu(param_menu: Any, target_menu: Optional[int]) -> bool:
    if target_menu is None:
        return True
    if isinstance(param_menu, int):
        return param_menu == target_menu
    if isinstance(param_menu, str):
        try:
            base = int(param_menu.split("-")[0].strip())
            return base == target_menu
        except (ValueError, IndexError):
            return False
    return False


def _get_available_menu_numbers(device_id: str) -> set[int]:
    params_by_id = parameter_registry.list_parameters(device_id)
    available_menus: set[int] = set()

    for meta in params_by_id.values():
        menu_value = meta.get("menu")
        if isinstance(menu_value, int):
            available_menus.add(menu_value)
            continue
        if isinstance(menu_value, str):
            try:
                available_menus.add(int(menu_value.split("-")[0].strip()))
            except (ValueError, IndexError):
                continue

    return available_menus


@router.get("/menus")
async def get_drive_menus(
    device_id: str = Query("drive_avid", description="Drive device ID"),
):
    """
    Devuelve el catálogo de menús del drive.
    """
    available_menus = _get_available_menu_numbers(device_id)
    return [
        menu
        for menu in _load_menus()
        if isinstance(menu, dict)
        and isinstance(menu.get("menu"), int)
        and menu["menu"] in available_menus
    ]


@router.get("/fault-codes")
async def get_fault_codes():
    """
    Devuelve el catálogo de códigos de falla (warnings/trips) del drive.
    Fuente: config/fault_codes.json
    """
    return _load_fault_codes()


@router.patch("/parameters/{parameter_id}/scale-factor")
async def update_drive_parameter_scale_factor(
    parameter_id: str,
    payload: UpdateScaleFactorRequest,
):
    if not parameter_registry.has_device(payload.device_id):
        raise HTTPException(status_code=404, detail=f"Device {payload.device_id} not found")

    if payload.scale_factor not in ALLOWED_SCALE_FACTORS:
        allowed = ", ".join(str(value) for value in sorted(ALLOWED_SCALE_FACTORS))
        raise HTTPException(
            status_code=400,
            detail=f"scale_factor must be one of: {allowed}",
        )

    if not parameter_registry.get_parameter(parameter_id, device_id=payload.device_id):
        raise HTTPException(
            status_code=404,
            detail=f"Parameter {parameter_id} not found for device {payload.device_id}",
        )

    try:
        updated = parameter_registry.update_scale_factor(
            parameter_id,
            payload.scale_factor,
            device_id=payload.device_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to update scale factor") from exc

    return {
        "device_id": payload.device_id,
        "parameter_id": parameter_id,
        "scale_factor": updated.get("scale_factor", payload.scale_factor),
    }


@router.get("/parameters")
async def get_drive_parameters(
    device_id: str = Query("drive_avid", description="Drive device ID"),
    menu: Optional[int] = Query(None, description="Menu number to filter by"),
    search: Optional[str] = Query(None, description="Buscar por id o nombre"),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=100),
):
    """
    Lista paginada de parámetros del drive con metadatos.
    """
    params_by_id = parameter_registry.list_parameters(device_id)
    if not params_by_id:
        raise HTTPException(status_code=404, detail=f"Device {device_id} has no parameters")

    # Filtro base
    search_text = (search or "").strip().lower()
    filtered: List[Dict[str, Any]] = []
    for pid, meta in params_by_id.items():
        if menu is not None and not _matches_menu(meta.get("menu"), menu):
            continue
        if search_text:
            name_val = str(meta.get("name") or "").lower()
            if search_text not in pid.lower() and search_text not in name_val:
                continue
        filtered.append({"id": pid, **meta})

    # Ordenar por id para consistencia
    filtered.sort(key=lambda p: p["id"])

    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]

    items: List[Dict[str, Any]] = []
    for item in page_items:
        pid = item.get("id")
        modbus_address = parameter_registry.get_address(pid, device_id=device_id)
        range_text = item.get("range_text")
        items.append(
            {
                "id": pid,
                "name": item.get("name"),
                "menu": item.get("menu"),
                "unit": item.get("unit"),
                "description": item.get("description"),
                "attributes": item.get("attributes") or [],
                "range_numeric": item.get("range_numeric"),
                "range_text": range_text,
                "default": item.get("default"),
                "modbus_address": modbus_address,
                "scale_factor": item.get("scale_factor", 100),
                "options": _parse_range_options(range_text),
            }
        )

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }

