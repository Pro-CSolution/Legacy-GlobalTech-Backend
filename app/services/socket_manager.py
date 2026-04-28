import logging
from typing import Any, Dict, Set

import socketio

from app.core.utils import utc_now
from app.modbus_engine.state import state_manager
from app.services.socket_events import (
    DEVICE_UPDATE,
    SUBSCRIBE_DEVICE,
    SUBSCRIBE_PARAMETER,
    SUBSCRIBE_TREND,
    TREND_UPDATE,
    UNSUBSCRIBE_PARAMETER,
    UNSUBSCRIBE_TREND,
)

logger = logging.getLogger(__name__)
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")

# sid -> {device_id: set(param_ids)}
trend_subscriptions_by_sid: Dict[str, Dict[str, Set[str]]] = {}
# device_id -> {sid, sid}
trend_subscribers_by_device: Dict[str, Set[str]] = {}
# device_id -> {param_id: ref_count}
trend_parameter_refcounts_by_device: Dict[str, Dict[str, int]] = {}


def _add_trend_polled_params(device_id: str, param_ids: Set[str]) -> None:
    if not param_ids:
        return

    device_counts = trend_parameter_refcounts_by_device.setdefault(device_id, {})
    newly_activated = []
    for pid in param_ids:
        previous = device_counts.get(pid, 0)
        device_counts[pid] = previous + 1
        if previous == 0:
            newly_activated.append(pid)

    if newly_activated:
        state_manager.add_trend_parameters(device_id, newly_activated)


def _remove_trend_polled_params(device_id: str, param_ids: Set[str]) -> None:
    if not param_ids:
        return

    device_counts = trend_parameter_refcounts_by_device.get(device_id)
    if not device_counts:
        return

    newly_deactivated = []
    for pid in param_ids:
        previous = device_counts.get(pid, 0)
        if previous <= 0:
            continue
        if previous == 1:
            device_counts.pop(pid, None)
            newly_deactivated.append(pid)
        else:
            device_counts[pid] = previous - 1

    if newly_deactivated:
        state_manager.remove_trend_parameters(device_id, list(newly_deactivated))

    if not device_counts:
        trend_parameter_refcounts_by_device.pop(device_id, None)


def _replace_trend_subscription(sid: str, device_id: str, next_params: Set[str]) -> None:
    device_map = trend_subscriptions_by_sid.setdefault(sid, {})
    previous_params = set(device_map.get(device_id, set()))

    to_add = next_params - previous_params
    to_remove = previous_params - next_params

    if to_remove:
        _remove_trend_polled_params(device_id, to_remove)
    if to_add:
        _add_trend_polled_params(device_id, to_add)

    if next_params:
        device_map[device_id] = set(next_params)
        trend_subscribers_by_device.setdefault(device_id, set()).add(sid)
    else:
        device_map.pop(device_id, None)
        if not device_map:
            trend_subscriptions_by_sid.pop(sid, None)
        if device_id in trend_subscribers_by_device:
            trend_subscribers_by_device[device_id].discard(sid)
            if not trend_subscribers_by_device[device_id]:
                trend_subscribers_by_device.pop(device_id, None)


def _cleanup_sid(sid: str) -> None:
    """Elimina todas las suscripciones asociadas a un sid."""
    device_map = trend_subscriptions_by_sid.pop(sid, {})
    for device_id, params in device_map.items():
        _remove_trend_polled_params(device_id, set(params))
        if device_id in trend_subscribers_by_device:
            trend_subscribers_by_device[device_id].discard(sid)
            if not trend_subscribers_by_device[device_id]:
                trend_subscribers_by_device.pop(device_id, None)


async def notify_clients(device_id: str, data: Dict[str, Any]):
    """Callback called by StateManager when new data arrives."""
    try:
        await sio.emit(DEVICE_UPDATE, {"device_id": device_id, "data": data}, room=device_id)

        if device_id in trend_subscribers_by_device:
            ts = utc_now().isoformat()
            for sid in list(trend_subscribers_by_device.get(device_id, [])):
                device_map = trend_subscriptions_by_sid.get(sid)
                if not device_map:
                    continue
                params = device_map.get(device_id)
                if not params:
                    continue
                filtered = {pid: val for pid, val in data.items() if pid in params}
                if filtered:
                    await sio.emit(
                        TREND_UPDATE,
                        {"device_id": device_id, "data": filtered, "ts": ts},
                        to=sid,
                    )
    except Exception as e:
        logger.error("Error emitting socket event: %s", e)


state_manager.add_listener(notify_clients)


@sio.event
async def connect(sid, environ):
    logger.info("Socket Client connected: %s", sid)


@sio.event
async def disconnect(sid):
    logger.info("Socket Client disconnected: %s", sid)
    _cleanup_sid(sid)


@sio.on(SUBSCRIBE_DEVICE)
async def subscribe_device(sid, device_id):
    """Frontend asks to subscribe to a device channel."""
    logger.info("Client %s subscribed to %s", sid, device_id)
    await sio.enter_room(sid, device_id)
    snapshot = state_manager.get_snapshot(device_id)
    if snapshot:
        await sio.emit(DEVICE_UPDATE, {"device_id": device_id, "data": snapshot}, to=sid)


@sio.on(SUBSCRIBE_PARAMETER)
async def subscribe_parameter(sid, data):
    """
    Frontend asks to monitor specific parameters (On-Demand).
    data = {'device_id': 'x', 'parameter_ids': ['y', ...]} o {'param_id': 'y'}
    """
    device_id = data.get("device_id")
    if not device_id:
        return

    raw_ids = data.get("parameter_ids") or data.get("param_ids") or data.get("param_id")
    if raw_ids is None:
        return

    if isinstance(raw_ids, (list, tuple, set)):
        param_ids = [pid for pid in raw_ids if isinstance(pid, str)]
    else:
        param_ids = [raw_ids] if isinstance(raw_ids, str) else []

    if not param_ids:
        return

    active_before = state_manager.get_on_demand_parameters(device_id)
    requested_unique = []
    seen = set()
    for pid in param_ids:
        if pid in seen:
            continue
        seen.add(pid)
        if pid in active_before:
            continue
        requested_unique.append(pid)

    if not requested_unique:
        return

    added = state_manager.add_parameters(device_id, requested_unique)
    skipped = len(requested_unique) - len(added)
    if added:
        logger.debug("Client %s added on-demand params for %s: %s", sid, device_id, added)
    if skipped > 0:
        logger.warning(
            "On-demand limit reached for %s. Added %s, skipped %s.",
            device_id,
            len(added),
            skipped,
        )


@sio.on(UNSUBSCRIBE_PARAMETER)
async def unsubscribe_parameter(sid, data):
    device_id = data.get("device_id")
    if not device_id:
        return

    raw_ids = data.get("parameter_ids") or data.get("param_ids") or data.get("param_id")
    if raw_ids is None:
        return

    if isinstance(raw_ids, (list, tuple, set)):
        param_ids = [pid for pid in raw_ids if isinstance(pid, str)]
    else:
        param_ids = [raw_ids] if isinstance(raw_ids, str) else []

    if not param_ids:
        return

    state_manager.remove_parameters(device_id, param_ids)


@sio.on(SUBSCRIBE_TREND)
async def subscribe_trend(sid, data):
    """
    Frontend solicita tendencias de parámetros específicos.
    Además de filtrar eventos websocket, activa polling backend para esos parámetros.
    """
    device_id = data.get("device_id")
    param_ids = data.get("parameter_ids") or []
    if not device_id or not isinstance(param_ids, list):
        return

    params_set = {p for p in param_ids if isinstance(p, str)}
    if not params_set:
        return

    _replace_trend_subscription(sid, device_id, params_set)


@sio.on(UNSUBSCRIBE_TREND)
async def unsubscribe_trend(sid, data):
    device_id = data.get("device_id")
    if not device_id:
        return

    _replace_trend_subscription(sid, device_id, set())
