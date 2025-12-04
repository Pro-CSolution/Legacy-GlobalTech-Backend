import socketio
from app.modbus_engine.state import state_manager
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')

async def notify_clients(device_id: str, data: Dict[str, Any]):
    """Callback called by StateManager when new data arrives"""
    try:
        # Emit to specific device room
        # Broadcasting to a room is efficient
        await sio.emit('device_update', {'device_id': device_id, 'data': data}, room=device_id)
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

@sio.event
async def subscribe_device(sid, device_id):
    """Frontend asks to subscribe to a device channel"""
    logger.info(f"Client {sid} subscribed to {device_id}")
    sio.enter_room(sid, device_id)
    # Send initial snapshot
    snapshot = state_manager.get_snapshot(device_id)
    if snapshot:
        await sio.emit('device_update', {'device_id': device_id, 'data': snapshot}, to=sid)

@sio.event
async def subscribe_parameter(sid, data):
    """
    Frontend asks to monitor specific parameter (On-Demand).
    data = {'device_id': 'x', 'param_id': 'y'}
    """
    device_id = data.get('device_id')
    param_id = data.get('param_id')
    if device_id and param_id:
        state_manager.subscribe_parameter(device_id, param_id)
        logger.debug(f"Client {sid} added demand for {device_id}:{param_id}")

@sio.event
async def unsubscribe_parameter(sid, data):
    device_id = data.get('device_id')
    param_id = data.get('param_id')
    if device_id and param_id:
        state_manager.unsubscribe_parameter(device_id, param_id)

