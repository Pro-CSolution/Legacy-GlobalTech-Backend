"""
Script para agregar los campos 'address' y 'register_type' a parameters.json.
Por defecto:
- register_type = "holding" si no existe.
- address se calcula a partir del ID con formato Pxx.yy -> (menu * 100) + param - 1.

Uso:
    python scripts/add_address_and_type.py
"""

import json
import re
from pathlib import Path


def calculate_modbus_address(parameter_id: str) -> int:
    clean_id = parameter_id.strip().upper()
    match = re.match(r"^P(\d+)\.(\d+)$", clean_id)
    if not match:
        raise ValueError(f"Formato de ID inválido: {parameter_id}. Se espera formato Pxx.yy")
    menu = int(match.group(1))
    parameter = int(match.group(2))
    return (menu * 100) + parameter - 1


def main():
    root = Path(__file__).resolve().parents[1]
    params_path = root / "parameters.json"

    if not params_path.exists():
        raise FileNotFoundError(f"No se encontró parameters.json en {params_path}")

    data = json.loads(params_path.read_text(encoding="utf-8"))

    added_address = 0
    added_type = 0
    skipped = 0

    for param in data:
        # register_type
        if "register_type" not in param:
            param["register_type"] = "holding"
            added_type += 1

        # address
        if "address" not in param or param["address"] in (None, ""):
            pid = param.get("id")
            if not pid:
                skipped += 1
                continue
            try:
                param["address"] = calculate_modbus_address(pid)
                added_address += 1
            except ValueError:
                # IDs que no siguen el patrón Pxx.yy se dejan sin address
                skipped += 1
        else:
            # Normalizar a int si viene en string
            try:
                param["address"] = int(param["address"])
            except (TypeError, ValueError):
                skipped += 1

    params_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    print(
        f"Listo. address agregados: {added_address}, register_type agregados: {added_type}, omitidos: {skipped}"
    )


if __name__ == "__main__":
    main()

