from typing import Dict, Any, Set, List, Callable
import asyncio
import logging

logger = logging.getLogger("app.modbus_engine.state")

class SystemState:
    _instance = None
    _data: Dict[str, Dict[str, Any]] = {} # device_id -> {param_id: value}
    _on_demand_subscriptions: Dict[str, Set[str]] = {} # device_id -> {param_id_1, param_id_2}
    _subscribers: List[Callable] = [] # Callbacks for updates
    _on_demand_limit: int = 18

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SystemState, cls).__new__(cls)
            # Initialize data structure
            cls._instance._data = {}
            cls._instance._on_demand_subscriptions = {}
            cls._instance._subscribers = []
            cls._instance._on_demand_limit = 18
        return cls._instance

    def update(self, device_id: str, values: Dict[str, Any]):
        """Actualiza el estado en memoria y notifica a los suscriptores"""
        if device_id not in self._data:
            self._data[device_id] = {}
        
        self._data[device_id].update(values)
        self._notify(device_id, values)

    def get_snapshot(self, device_id: str = None) -> Dict[str, Any]:
        """Retorna el estado actual completo o de un dispositivo"""
        if device_id:
            return self._data.get(device_id, {})
        return self._data

    def subscribe_parameter(self, device_id: str, param_id: str):
        """Añade un parámetro a la lista de monitoreo bajo demanda"""
        self.add_parameters(device_id, [param_id])

    def unsubscribe_parameter(self, device_id: str, param_id: str):
        """Elimina un parámetro de la lista de monitoreo bajo demanda"""
        if device_id in self._on_demand_subscriptions:
            self._on_demand_subscriptions[device_id].discard(param_id)

    def add_parameters(self, device_id: str, param_ids: List[str]) -> List[str]:
        """
        Añade múltiples parámetros respetando el límite configurado.
        Retorna la lista de parámetros que fueron agregados.
        """
        if not param_ids:
            return []

        current = self._on_demand_subscriptions.get(device_id, set())
        added: List[str] = []

        # Mantener límite
        space_left = max(self._on_demand_limit - len(current), 0)
        for pid in param_ids:
            if pid in current:
                continue
            if space_left <= 0:
                break
            current.add(pid)
            added.append(pid)
            space_left -= 1

        # En caso de overflow previo, recorta a límite
        if len(current) > self._on_demand_limit:
            current = set(list(current)[: self._on_demand_limit])

        self._on_demand_subscriptions[device_id] = current
        return added

    def remove_parameters(self, device_id: str, param_ids: List[str]):
        if device_id not in self._on_demand_subscriptions:
            return
        for pid in param_ids:
            self._on_demand_subscriptions[device_id].discard(pid)

    def get_on_demand_limit(self) -> int:
        return self._on_demand_limit

    def set_on_demand_limit(self, limit: int):
        if limit > 0:
            self._on_demand_limit = limit

    def get_active_parameters(self, device_id: str) -> Set[str]:
        """Retorna los parámetros que se están viendo actualmente en el frontend"""
        return self._on_demand_subscriptions.get(device_id, set())

    def add_listener(self, callback: Callable):
        """Registra un callback que se ejecutará cuando lleguen nuevos datos"""
        if callback not in self._subscribers:
            self._subscribers.append(callback)

    def _notify(self, device_id: str, values: Dict[str, Any]):
        for callback in self._subscribers:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(device_id, values))
                else:
                    callback(device_id, values)
            except Exception as e:
                logger.error("Error in state listener: %s", str(e))

state_manager = SystemState()

