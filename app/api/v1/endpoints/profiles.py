from typing import List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, Body
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from datetime import datetime
import logging

from app.db.session import get_session
from app.db.models import Profile, ProfileParameter
from app.core.utils import utc_now
from app.modbus_engine.manager import modbus_manager

logger = logging.getLogger(__name__)

router = APIRouter()

# --- Pydantic Schemas for API ---
from pydantic import BaseModel

class ProfileParameterBase(BaseModel):
    device_id: str
    parameter_id: str
    value: Any # Allow any type in input, convert to string for DB

class ProfileParameterCreate(ProfileParameterBase):
    pass

class ProfileParameterRead(ProfileParameterBase):
    id: int
    profile_id: int

class ProfileCreate(BaseModel):
    name: str
    color: str
    icon: str
    parameters: List[ProfileParameterCreate] = []

class ProfileRead(BaseModel):
    id: int
    name: str
    color: str
    icon: str
    created_at: datetime
    last_used: Optional[datetime]
    parameters: List[ProfileParameterRead]

class ProfileUpdate(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None
    icon: Optional[str] = None
    parameters: Optional[List[ProfileParameterCreate]] = None

class ApplyProfileResult(BaseModel):
    device_id: str
    parameter_id: str
    status: str  # 'pending', 'writing', 'success', 'error'
    message: Optional[str] = None

class ApplyProfileResponse(BaseModel):
    results: List[ApplyProfileResult]

# --- Endpoints ---

@router.get("/profiles", response_model=List[ProfileRead])
async def get_profiles(session: AsyncSession = Depends(get_session)):
    result = await session.execute(
        select(Profile).options(selectinload(Profile.parameters))
    )
    profiles = result.scalars().all()
    # Eager loading is automatic with SQLModel relationship access in loop if session is open,
    # or we can use .options(selectinload(Profile.parameters)) for efficiency.
    # For simplicity/scale here, lazy load is fine or explicit join.
    return profiles

@router.post("/profiles", response_model=ProfileRead, status_code=201)
async def create_profile(profile_in: ProfileCreate, session: AsyncSession = Depends(get_session)):
    # Create Profile
    db_profile = Profile(
        name=profile_in.name,
        color=profile_in.color,
        icon=profile_in.icon,
    )
    session.add(db_profile)
    await session.commit()
    await session.refresh(db_profile)

    # Create Parameters
    for param in profile_in.parameters:
        db_param = ProfileParameter(
            profile_id=db_profile.id,
            device_id=param.device_id,
            parameter_id=param.parameter_id,
            value=str(param.value) # Convert to string for storage
        )
        session.add(db_param)
    
    await session.commit()
    # Recargar con parámetros para evitar lazy-load en respuesta
    result = await session.execute(
        select(Profile).options(selectinload(Profile.parameters)).where(Profile.id == db_profile.id)
    )
    return result.scalars().one()

@router.put("/profiles/{profile_id}", response_model=ProfileRead)
async def update_profile(profile_id: int, profile_in: ProfileUpdate, session: AsyncSession = Depends(get_session)):
    db_profile = await session.get(Profile, profile_id)
    if not db_profile:
        raise HTTPException(status_code=404, detail="Profile not found")

    if profile_in.name is not None:
        db_profile.name = profile_in.name
    if profile_in.color is not None:
        db_profile.color = profile_in.color
    if profile_in.icon is not None:
        db_profile.icon = profile_in.icon

    if profile_in.parameters is not None:
        # Delete existing parameters
        # Note: In a more complex app we might diff them, but full replace is simpler/safer for this scope
        result = await session.execute(select(ProfileParameter).where(ProfileParameter.profile_id == profile_id))
        existing_params = result.scalars().all()
        for p in existing_params:
            session.delete(p)
        
        # Add new parameters
        for param in profile_in.parameters:
            db_param = ProfileParameter(
                profile_id=db_profile.id,
                device_id=param.device_id,
                parameter_id=param.parameter_id,
                value=str(param.value)
            )
            session.add(db_param)

    session.add(db_profile)
    await session.commit()
    result = await session.execute(
        select(Profile).options(selectinload(Profile.parameters)).where(Profile.id == db_profile.id)
    )
    return result.scalars().one()

@router.delete("/profiles/{profile_id}", response_model=dict)
async def delete_profile(profile_id: int, session: AsyncSession = Depends(get_session)):
    db_profile = await session.get(Profile, profile_id)
    if not db_profile:
        raise HTTPException(status_code=404, detail="Profile not found")
    
    await session.delete(db_profile)
    await session.commit()
    return {"ok": True}

@router.post("/profiles/{profile_id}/apply", response_model=ApplyProfileResponse)
async def apply_profile(profile_id: int, session: AsyncSession = Depends(get_session)):
    """
    Applies the profile by writing all parameters.
    Returns the list of attempted writes with initial status.
    The frontend should verify success via realtime data or polling if needed,
    but here we attempt the write via modbus_manager.
    """
    print(f"APPLY_PROFILE: Request to apply profile {profile_id}")
    
    result = await session.execute(
        select(Profile).options(selectinload(Profile.parameters)).where(Profile.id == profile_id)
    )
    db_profile = result.scalars().one_or_none()
    if not db_profile:
        print(f"APPLY_PROFILE: Profile {profile_id} not found")
        raise HTTPException(status_code=404, detail="Profile not found")

    # Update last_used
    db_profile.last_used = utc_now()
    session.add(db_profile)
    await session.commit()

    results = []
    
    print(f"APPLY_PROFILE: Profile loaded '{db_profile.name}' with {len(db_profile.parameters)} parameters")
    
    # We process writes.
    # Note: modbus_manager.write_parameter is usually async.
    # We will attempt them one by one or in parallel groups.
    # For now, let's process sequentially to ensure order/safety and return results.
    
    for param in db_profile.parameters:
        print(f"APPLY_PROFILE: Processing {param.parameter_id} for {param.device_id} with val={param.value}")
        try:
            # Determine value type. Simple heuristic: try int, then float, else string
            # Or reliance on modbus manager to handle string conversion if needed.
            # Usually parameters need specific types. 
            # We'll try to parse numeric if possible as most modbus regs are numeric.
            val_to_write = param.value
            if val_to_write.lower() == 'true': val_to_write = True
            elif val_to_write.lower() == 'false': val_to_write = False
            else:
                try:
                    if '.' in val_to_write:
                        val_to_write = float(val_to_write)
                    else:
                        val_to_write = int(val_to_write)
                except ValueError:
                    pass # Keep as string
            
            print(f"APPLY_PROFILE: Writing {val_to_write} to {param.device_id}:{param.parameter_id}")
            await modbus_manager.write_parameter(param.device_id, param.parameter_id, val_to_write)
            
            results.append(ApplyProfileResult(
                device_id=param.device_id,
                parameter_id=param.parameter_id,
                status="success", # Or 'writing' if async queued
                message="Command sent"
            ))
            print(f"APPLY_PROFILE: Write success for {param.parameter_id}")
        except Exception as e:
            print(f"APPLY_PROFILE: Write failed for {param.parameter_id}: {e}")
            results.append(ApplyProfileResult(
                device_id=param.device_id,
                parameter_id=param.parameter_id,
                status="error",
                message=str(e)
            ))

    print(f"APPLY_PROFILE: Completed with {len(results)} results")
    return ApplyProfileResponse(results=results)
