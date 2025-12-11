"""
Script para limpiar config/drive_menus.json eliminando menús no usados.

Obtiene los menús referenciados en parameters/drive.parameters.json (campo "menu")
y sobrescribe config/drive_menus.json manteniendo solo los presentes. Antes de
escribir, genera un respaldo en config/drive_menus.backup.json.

Uso:
    python scripts/clean_drive_menus.py
"""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MENUS_PATH = ROOT / "config" / "drive_menus.json"
PARAMS_PATH = ROOT / "parameters" / "drive.parameters.json"
BACKUP_PATH = MENUS_PATH.with_suffix(".backup.json")


def parse_menu_value(value) -> int | None:
    """Convierte el valor de menú a int si es válido."""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.isdigit():
            return int(cleaned)
    return None


def load_drive_menus() -> tuple[list, str]:
    """Carga el archivo de menús y retorna (lista, contenido bruto)."""
    if not MENUS_PATH.exists():
        raise FileNotFoundError(f"No se encontró {MENUS_PATH}")

    content = MENUS_PATH.read_text(encoding="utf-8")
    data = json.loads(content)
    if not isinstance(data, list):
        raise ValueError(f"El archivo {MENUS_PATH} debe contener una lista JSON")

    return data, content


def load_menu_references() -> tuple[set[int], int]:
    """Obtiene el set de menús referenciados en drive.parameters.json."""
    if not PARAMS_PATH.exists():
        raise FileNotFoundError(f"No se encontró {PARAMS_PATH}")

    params = json.loads(PARAMS_PATH.read_text(encoding="utf-8"))
    if not isinstance(params, list):
        raise ValueError(f"El archivo {PARAMS_PATH} debe contener una lista JSON")

    used_menus: set[int] = set()
    skipped = 0

    for param in params:
        if not isinstance(param, dict):
            skipped += 1
            continue
        menu_val = parse_menu_value(param.get("menu"))
        if menu_val is None:
            skipped += 1
            continue
        used_menus.add(menu_val)

    return used_menus, skipped


def filter_menus(menu_entries: list, used_menus: set[int]) -> tuple[list, int, int]:
    """Retorna (menús retenidos, removidos por falta de referencia, removidos inválidos)."""
    retained = []
    removed_missing = 0
    removed_invalid = 0

    for entry in menu_entries:
        if not isinstance(entry, dict):
            removed_invalid += 1
            continue

        menu_val = parse_menu_value(entry.get("menu"))
        if menu_val is None:
            removed_invalid += 1
            continue

        if menu_val in used_menus:
            retained.append(entry)
        else:
            removed_missing += 1

    return retained, removed_missing, removed_invalid


def main():
    used_menus, skipped_params = load_menu_references()
    menu_entries, original_content = load_drive_menus()
    filtered, removed_missing, removed_invalid = filter_menus(menu_entries, used_menus)

    BACKUP_PATH.write_text(original_content, encoding="utf-8")
    MENUS_PATH.write_text(json.dumps(filtered, indent=2, ensure_ascii=False), encoding="utf-8")

    print(
        f"Menús referenciados encontrados: {len(used_menus)} "
        f"(parámetros sin menú válido: {skipped_params})"
    )
    print(
        f"Menús originales: {len(menu_entries)}, retenidos: {len(filtered)}, "
        f"eliminados sin referencia: {removed_missing}, eliminados inválidos: {removed_invalid}"
    )
    print(f"Backup guardado en: {BACKUP_PATH}")


if __name__ == "__main__":
    main()

