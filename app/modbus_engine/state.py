from typing import Dict, Any, Set, List, Callable
import asyncio

class SystemState:
    _instance = None
    _data: Dict[str, Dict[str, Any]] = {} # device_id -> {param_id: value}
    _on_demand_subscriptions: Dict[str, Set[str]] = {} # device_id -> {param_id_1, param_id_2}
    _subscribers: List[Callable] = [] # Callbacks for updates

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SystemState, cls).__new__(cls)
            # Initialize data structure
            cls._instance._data = {}
            cls._instance._on_demand_subscriptions = {}
            cls._instance._subscribers = []
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
        if device_id not in self._on_demand_subscriptions:
            self._on_demand_subscriptions[device_id] = set()
        self._on_demand_subscriptions[device_id].add(param_id)

    def unsubscribe_parameter(self, device_id: str, param_id: str):
        """Elimina un parámetro de la lista de monitoreo bajo demanda"""
        if device_id in self._on_demand_subscriptions:
            self._on_demand_subscriptions[device_id].discard(param_id)

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
                print(f"Error in state listener: {e}")

state_manager = SystemState()

