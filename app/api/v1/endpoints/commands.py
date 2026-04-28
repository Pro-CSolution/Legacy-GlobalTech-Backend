from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel
from typing import Any, Dict, Optional
import logging

from app.modbus_engine.manager import modbus_manager

router = APIRouter()
logger = logging.getLogger(__name__)

class WriteCommand(BaseModel):
    device_id: str
    parameter_id: str
    value: Any

class CustomCommand(BaseModel):
    device_id: str
    action: str
    parameters: Optional[Dict[str, Any]] = {}

@router.post("/write")
async def write_parameter(cmd: WriteCommand):
    """
    Generic command to write a value to a parameter on a device.
    """
    try:
        logger.info(f"Command received: Write {cmd.parameter_id}={cmd.value} on {cmd.device_id}")
        await modbus_manager.write_parameter(cmd.device_id, cmd.parameter_id, cmd.value)
        return {"status": "success", "message": f"Written {cmd.value} to {cmd.parameter_id}"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        logger.error(f"Write command rejected by device: {e}")
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        logger.error(f"Write command failed: {e}")
        raise HTTPException(status_code=500, detail="Command execution failed")

@router.post("/action/{action_name}")
async def execute_action(action_name: str, cmd: CustomCommand):
    """
    Execute a custom complex action.
    This is a placeholder for complex logic (sequences, multiple writes, etc).
    """
    if action_name != cmd.action:
        raise HTTPException(status_code=400, detail="Action name mismatch")

    logger.info(f"Custom Action received: {action_name} on {cmd.device_id}")

    try:
        if action_name in {"trip-reset", "trip-reset-direct", "trip_reset", "trip_reset_direct"}:
            params = cmd.parameters or {}
            raw_pulse_ms = params.get("pulseMs", params.get("pulse_ms", 500))
            pulse_ms = int(raw_pulse_ms)
            result = await modbus_manager.pulse_trip_reset(cmd.device_id, pulse_ms=pulse_ms)
            return {
                "status": "success",
                "message": "Trip reset pulse sent directly to drive",
                "result": result,
            }

        raise HTTPException(status_code=404, detail=f"Unknown action: {action_name}")
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        logger.error(f"Custom action rejected by device: {e}")
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        logger.error(f"Custom action failed: {e}")
        raise HTTPException(status_code=500, detail="Action execution failed")


