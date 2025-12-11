import json
import os
from typing import Dict, Any, Optional
from pydantic_settings import BaseSettings
from functools import lru_cache
from app.core.utils import calculate_modbus_address

class Settings(BaseSettings):
    PROJECT_NAME: str = "GlobalTech IIoT"
    DATABASE_URL: str = "postgresql+asyncpg://user:password@localhost:5432/globaltech_db"
    MODBUS_POLL_INTERVAL: float = 5.0
    DATA_LOG_INTERVAL: float | None = None
    
    # Mock Configuration
    USE_MOCK_DATA: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache()
def get_settings():
    return Settings()

class ParameterRegistry:
    """
    Maneja parámetros por dispositivo. Cada device_id tiene su propio archivo.
    Estructuras:
      - _parameters[device_id][param_id] -> dict
      - _address_map[device_id][param_id] -> address
      - _reverse_address_map[device_id][register_type][address] -> param_id
    """
    _instance = None
    _default_device = "default"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ParameterRegistry, cls).__new__(cls)
            cls._instance._parameters: Dict[str, Dict[str, Dict[str, Any]]] = {}
            cls._instance._address_map: Dict[str, Dict[str, int]] = {}
            cls._instance._reverse_address_map: Dict[str, Dict[str, Dict[int, str]]] = {}
            cls._instance._device_files: Dict[str, str] = {}
            cls._instance._load_default()
        return cls._instance

    def _normalize_register_type(self, value: str | None) -> str:
        """
        Normaliza el tipo de registro. Valores soportados:
        - holding (por defecto)
        - input  (input register)
        - coil
        - discrete (discrete input)
        """
        if not value:
            return "holding"
        normalized = value.strip().lower()
        allowed = {"holding", "input", "coil", "discrete"}
        if normalized not in allowed:
            return "holding"
        return normalized

    def _load_default(self):
        """
        Carga el archivo por defecto:
        - Primero intenta parameters/default.parameters.json
        - Si no existe, usa parameters.json (compatibilidad)
        """
        cwd = os.getcwd()
        default_path = os.path.join(cwd, "parameters", "default.parameters.json")
        legacy_path = os.path.join(cwd, "parameters.json")

        if os.path.exists(default_path):
            self.register_device(self._default_device, default_path)
        elif os.path.exists(legacy_path):
            self.register_device(self._default_device, legacy_path)
        else:
            print("Warning: No se encontró archivo de parámetros por defecto.")

    def _normalize_menu(self, menu_value):
        """
        Normaliza el campo de menú a entero si es posible.
        Si viene en formato '70-72', se toma el primer segmento como entero.
        """
        if menu_value is None:
            return None
        if isinstance(menu_value, int):
            return menu_value
        if isinstance(menu_value, str):
            parts = menu_value.split("-")
            try:
                return int(parts[0].strip())
            except (ValueError, IndexError):
                return None
        try:
            return int(menu_value)
        except (TypeError, ValueError):
            return None

    def _load_parameters(self, device_id: str, file_path: str):
        try:
            if not os.path.exists(file_path):
                print(f"Warning: parameters file not found for {device_id} at {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            count = 0
            for param in data:
                pid = param.get("id")
                if not pid:
                    continue

                # Normaliza el menú si existe
                if "menu" in param:
                    normalized_menu = self._normalize_menu(param.get("menu"))
                    if normalized_menu is not None:
                        param["menu"] = normalized_menu

                self._parameters.setdefault(device_id, {})[pid] = param

                register_type = self._normalize_register_type(
                    param.get("register_type") or param.get("type")
                )

                address = param.get("address")
                if address is None:
                    try:
                        address = calculate_modbus_address(pid)
                    except ValueError:
                        # Si no se puede calcular y no hay address, se omite el mapeo
                        continue

                try:
                    address_int = int(address)
                except (TypeError, ValueError):
                    continue

                self._address_map.setdefault(device_id, {})[pid] = address_int
                reverse_map = self._reverse_address_map.setdefault(device_id, {}).setdefault(register_type, {})
                # Si hay colisión de dirección dentro del mismo tipo, el último gana
                reverse_map[address_int] = pid
                count += 1
            
            print(f"Loaded {count} parameters for device {device_id} from {file_path}")
            
        except Exception as e:
            print(f"Error loading parameters for {device_id} from {file_path}: {e}")

    def register_device(self, device_id: str, file_path: str):
        """
        Registra y carga parámetros para un dispositivo. Si ya está cargado y
        el archivo no cambió, no hace nada.
        """
        if device_id in self._parameters:
            return
        self._device_files[device_id] = file_path
        self._load_parameters(device_id, file_path)

    def _resolve_device(self, device_id: Optional[str]) -> str:
        return device_id or self._default_device

    def get_parameter(self, param_id: str, device_id: str | None = None) -> Optional[Dict[str, Any]]:
        did = self._resolve_device(device_id)
        return self._parameters.get(did, {}).get(param_id)

    def list_parameters(self, device_id: str | None = None) -> Dict[str, Dict[str, Any]]:
        """
        Retorna un dict de todos los parámetros para un dispositivo.
        """
        did = self._resolve_device(device_id)
        return self._parameters.get(did, {})

    def get_address(self, param_id: str, device_id: str | None = None) -> Optional[int]:
        did = self._resolve_device(device_id)
        return self._address_map.get(did, {}).get(param_id)

    def get_register_type(self, param_id: str, device_id: str | None = None) -> str:
        did = self._resolve_device(device_id)
        param = self._parameters.get(did, {}).get(param_id)
        return self._normalize_register_type(
            (param or {}).get("register_type") or (param or {}).get("type")
        )
        
    def get_id_by_address(self, address: int, register_type: str = "holding", device_id: str | None = None) -> Optional[str]:
        did = self._resolve_device(device_id)
        normalized = self._normalize_register_type(register_type)
        return self._reverse_address_map.get(did, {}).get(normalized, {}).get(address)

settings = get_settings()
parameter_registry = ParameterRegistry()

