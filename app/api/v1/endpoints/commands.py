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
    
    # TODO: Implement dispatch logic for custom actions here
    # Example: if action_name == "start_sequence": await sequence_manager.start(cmd.device_id)
    
    return {"status": "success", "message": f"Action {action_name} queued/executed"}


