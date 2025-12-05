from datetime import datetime, timezone
import re

def calculate_modbus_address(parameter_id: str) -> int:
    """
    Convierte un ID de parámetro (ej. P32.58) a dirección Modbus.
    Fórmula: (Menu * 100) + Parametro - 1
    
    Ejemplo:
    P32.58 -> (32 * 100) + 58 - 1 = 3257
    """
    # Limpiar el ID de posibles espacios y mayúsculas
    clean_id = parameter_id.strip().upper()
    
    # Regex para capturar Menu y Parametro (P<Menu>.<Parametro>)
    match = re.match(r"^P(\d+)\.(\d+)$", clean_id)
    
    if not match:
        raise ValueError(f"Formato de ID inválido: {parameter_id}. Se espera formato Pxx.yy")
    
    menu = int(match.group(1))
    parameter = int(match.group(2))
    
    return (menu * 100) + parameter - 1

def utc_now() -> datetime:
    return datetime.now(timezone.utc)