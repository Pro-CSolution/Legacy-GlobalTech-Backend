import json
import os
from typing import Dict, Any, Optional
from pydantic_settings import BaseSettings
from functools import lru_cache
from app.core.utils import calculate_modbus_address

class Settings(BaseSettings):
    PROJECT_NAME: str = "GlobalTech IIoT"
    DATABASE_URL: str = "postgresql+asyncpg://user:password@localhost:5432/globaltech_db"
    MODBUS_POLL_INTERVAL: float = 1.0
    
    # Mock Configuration
    USE_MOCK_DATA: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache()
def get_settings():
    return Settings()

class ParameterRegistry:
    _instance = None
    _parameters: Dict[str, Dict[str, Any]] = {}
    _address_map: Dict[str, int] = {}
    _reverse_address_map: Dict[int, str] = {} # Address -> ID

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ParameterRegistry, cls).__new__(cls)
            cls._instance.load_parameters()
        return cls._instance

    def load_parameters(self):
        file_path = os.path.join(os.getcwd(), "parameters.json")
        try:
            if not os.path.exists(file_path):
                print(f"Warning: parameters.json not found at {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            count = 0
            for param in data:
                pid = param.get("id")
                if pid:
                    self._parameters[pid] = param
                    try:
                        address = calculate_modbus_address(pid)
                        self._address_map[pid] = address
                        # Nota: Si hay colisiones de direcciones, el último gana
                        self._reverse_address_map[address] = pid
                        count += 1
                    except ValueError:
                        continue
            
            print(f"Loaded {count} parameters from parameters.json")
            
        except Exception as e:
            print(f"Error loading parameters.json: {e}")

    def get_parameter(self, param_id: str) -> Optional[Dict[str, Any]]:
        return self._parameters.get(param_id)

    def get_address(self, param_id: str) -> Optional[int]:
        return self._address_map.get(param_id)
        
    def get_id_by_address(self, address: int) -> Optional[str]:
        return self._reverse_address_map.get(address)

settings = get_settings()
parameter_registry = ParameterRegistry()

