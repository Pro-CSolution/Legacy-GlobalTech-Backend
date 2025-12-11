import socketio
from app.modbus_engine.state import state_manager
from typing import Dict, Any, Set
import logging
from app.core.utils import utc_now
from app.services.socket_events import (
    DEVICE_UPDATE,
    TREND_UPDATE,
    SUBSCRIBE_DEVICE,
    SUBSCRIBE_PARAMETER,
    UNSUBSCRIBE_PARAMETER,
    SUBSCRIBE_TREND,
    UNSUBSCRIBE_TREND,
)

logger = logging.getLogger(__name__)
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')

# Subscripción por sid -> {device_id: set(param_ids)}
trend_subscriptions_by_sid: Dict[str, Dict[str, Set[str]]] = {}
# Índice rápido device_id -> {sid, sid}
trend_subscribers_by_device: Dict[str, Set[str]] = {}


def _cleanup_sid(sid: str):
    """Elimina todas las subscripciones asociadas a un sid."""
    device_map = trend_subscriptions_by_sid.pop(sid, {})
    for device_id in device_map.keys():
        if device_id in trend_subscribers_by_device:
            trend_subscribers_by_device[device_id].discard(sid)
            if not trend_subscribers_by_device[device_id]:
                trend_subscribers_by_device.pop(device_id, None)


async def notify_clients(device_id: str, data: Dict[str, Any]):
    """Callback called by StateManager when new data arrives"""
    try:
        # Emit to specific device room
        # Broadcasting to a room is efficient
        await sio.emit(DEVICE_UPDATE, {'device_id': device_id, 'data': data}, room=device_id)

        # Trend-only updates filtrados por parámetros suscritos
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
                        {'device_id': device_id, 'data': filtered, 'ts': ts},
                        to=sid
                    )
    except Exception as e:
        logger.error(f"Error emitting socket event: {e}")

# Register callback
state_manager.add_listener(notify_clients)

@sio.event
async def connect(sid, environ):
    logger.info(f"Socket Client connected: {sid}")

@sio.event
async def disconnect(sid):
    logger.info(f"Socket Client disconnected: {sid}")
    _cleanup_sid(sid)

@sio.on(SUBSCRIBE_DEVICE)
async def subscribe_device(sid, device_id):
    """Frontend asks to subscribe to a device channel"""
    logger.info(f"Client {sid} subscribed to {device_id}")
    await sio.enter_room(sid, device_id)
    # Send initial snapshot
    snapshot = state_manager.get_snapshot(device_id)
    if snapshot:
        await sio.emit(DEVICE_UPDATE, {'device_id': device_id, 'data': snapshot}, to=sid)

@sio.on(SUBSCRIBE_PARAMETER)
async def subscribe_parameter(sid, data):
    """
    Frontend asks to monitor specific parameters (On-Demand).
    data = {'device_id': 'x', 'parameter_ids': ['y', ...]} o {'param_id': 'y'}
    """
    device_id = data.get('device_id')
    if not device_id:
        return

    raw_ids = data.get('parameter_ids') or data.get('param_ids') or data.get('param_id')
    if raw_ids is None:
        return

    if isinstance(raw_ids, (list, tuple, set)):
        param_ids = [pid for pid in raw_ids if isinstance(pid, str)]
    else:
        param_ids = [raw_ids] if isinstance(raw_ids, str) else []

    if not param_ids:
        return

    added = state_manager.add_parameters(device_id, param_ids)
    skipped = len(param_ids) - len(added)
    if added:
        logger.debug(f"Client {sid} added on-demand params for {device_id}: {added}")
    if skipped > 0:
        logger.warning(
            f"On-demand limit reached for {device_id}. Added {len(added)}, skipped {skipped}."
        )

@sio.on(UNSUBSCRIBE_PARAMETER)
async def unsubscribe_parameter(sid, data):
    device_id = data.get('device_id')
    if not device_id:
        return

    raw_ids = data.get('parameter_ids') or data.get('param_ids') or data.get('param_id')
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
    Frontend solicita tendencias de parámetros específicos (críticos).
    data = {'device_id': 'x', 'parameter_ids': ['P1.00', ...]}
    """
    device_id = data.get('device_id')
    param_ids = data.get('parameter_ids') or []
    if not device_id or not isinstance(param_ids, list):
        return

    params_set = {p for p in param_ids if isinstance(p, str)}
    if not params_set:
        return

    device_map = trend_subscriptions_by_sid.setdefault(sid, {})
    device_map[device_id] = params_set

    device_sids = trend_subscribers_by_device.setdefault(device_id, set())
    device_sids.add(sid)


@sio.on(UNSUBSCRIBE_TREND)
async def unsubscribe_trend(sid, data):
    device_id = data.get('device_id')
    if not device_id:
        return

    device_map = trend_subscriptions_by_sid.get(sid)
    if device_map and device_id in device_map:
        device_map.pop(device_id, None)

    if device_id in trend_subscribers_by_device:
        trend_subscribers_by_device[device_id].discard(sid)
        if not trend_subscribers_by_device[device_id]:
            trend_subscribers_by_device.pop(device_id, None)

