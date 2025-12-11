import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.core.config import parameter_registry

router = APIRouter(prefix="/drive", tags=["Drive"])

MENUS_PATH = Path(__file__).resolve().parents[4] / "config" / "drive_menus.json"
MENU_OPTIONS_CACHE: Optional[List[Dict[str, Any]]] = None


def _load_menus() -> List[Dict[str, Any]]:
    global MENU_OPTIONS_CACHE
    if MENU_OPTIONS_CACHE is not None:
        return MENU_OPTIONS_CACHE
    if not MENUS_PATH.exists():
        raise HTTPException(status_code=500, detail="drive_menus.json no encontrado")
    with MENUS_PATH.open("r", encoding="utf-8") as f:
        MENU_OPTIONS_CACHE = json.load(f)
    return MENU_OPTIONS_CACHE or []


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


@router.get("/menus")
async def get_drive_menus():
    """
    Devuelve el catálogo de menús del drive.
    """
    return _load_menus()


@router.get("/parameters")
async def get_drive_parameters(
    device_id: str = Query("drive_avid", description="ID del dispositivo drive"),
    menu: Optional[int] = Query(None, description="Número de menú para filtrar"),
    search: Optional[str] = Query(None, description="Buscar por id o nombre"),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=100),
):
    """
    Lista paginada de parámetros del drive con metadatos.
    """
    params_by_id = parameter_registry.list_parameters(device_id)
    if not params_by_id:
        raise HTTPException(status_code=404, detail=f"Dispositivo {device_id} sin parámetros")

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
                "options": _parse_range_options(range_text),
            }
        )

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }

