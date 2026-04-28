import asyncio
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import socketio

from sqlalchemy.exc import OperationalError

from app.core.logging_config import configure_logging

# Configure logging early (Windows service / PyInstaller friendly: file logging by default)
configure_logging()

# Usamos un logger específico de la app (propaga al root configurado arriba)
logger = logging.getLogger("app.main")

from app.core.config import settings
from app.db.session import (
    DatabaseUnavailableError,
    close_db,
    get_db_status,
    init_db_with_retry,
    is_db_ready,
)
from app.modbus_engine.manager import modbus_manager
from app.services.socket_manager import sio
from app.services.data_logger import data_logger
from app.services.trip_event_logger import trip_event_logger
from app.api.v1.endpoints import (
    commands,
    data,
    drive,
    manual_trend,
    monitor,
    profiles,
    reports,
    system,
    trend,
    wago_live,
)
from app.api.v1.endpoints.wago_live import close_live_clients

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup Events
    logger.info("System Starting up...")

    # 1. Init DB (non-blocking): retry in background until DB is ready.
    logger.info("Starting DB initialization retry loop (background task)...")
    app.state.db_init_task = asyncio.create_task(init_db_with_retry())
    
    # 2. Start Modbus Engine
    logger.info("Initializing Modbus Manager...")
    await modbus_manager.start()

    # 3. Start Trip Event Logger
    logger.info("Initializing Trip Event Logger...")
    await trip_event_logger.start()
    
    # 4. Start Data Logger
    logger.info("Initializing Data Logger...")
    await data_logger.start()
    
    yield
    
    # Shutdown Events
    logger.info("System Shutting down...")
    # Stop background DB init task (if still running)
    db_task = getattr(app.state, "db_init_task", None)
    if db_task is not None:
        db_task.cancel()
        try:
            await db_task
        except asyncio.CancelledError:
            pass

    await modbus_manager.stop()
    await trip_event_logger.stop()
    await data_logger.stop()
    await close_live_clients()
    await close_db()

app = FastAPI(
    title=settings.PROJECT_NAME, 
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routers
app.include_router(data.router, prefix="/api/v1", tags=["Data"])
app.include_router(monitor.router, prefix="/api/v1")
app.include_router(commands.router, prefix="/api/v1/commands", tags=["Commands"])
app.include_router(trend.router, prefix="/api/v1")
app.include_router(drive.router, prefix="/api/v1")
app.include_router(profiles.router, prefix="/api/v1", tags=["Profiles"])
app.include_router(manual_trend.router, prefix="/api/v1")
app.include_router(reports.router, prefix="/api/v1")
app.include_router(system.router, prefix="/api/v1/system", tags=["System"])
app.include_router(wago_live.router, prefix="/api/v1")

@app.exception_handler(DatabaseUnavailableError)
async def database_unavailable_handler(_: Request, exc: DatabaseUnavailableError):
    return JSONResponse(
        status_code=503,
        content={
            "detail": str(exc),
            "db": get_db_status(),
        },
    )

@app.exception_handler(OperationalError)
async def database_operational_error_handler(_: Request, __: OperationalError):
    # DB went down or connection failed mid-request.
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database is unavailable.",
            "db": get_db_status(),
        },
    )

@app.get("/")
async def root():
    return {"status": "online", "project": settings.PROJECT_NAME}

@app.get("/health/live")
async def health_live():
    return {"status": "live"}

@app.get("/health")
async def health():
    """
    General health summary.
    - Always returns 200 so operators can see *why* it's degraded.
    - Use /health/ready for readiness checks (returns 503 until DB is ready).
    """
    db = get_db_status()
    modbus = {
        "running": bool(getattr(modbus_manager, "running", False)),
        "device_count": len(getattr(modbus_manager, "devices", {}) or {}),
        "devices": list((getattr(modbus_manager, "devices", {}) or {}).keys()),
    }

    status = "ok"
    if not is_db_ready() or not modbus["running"]:
        status = "degraded"

    return {"status": status, "db": db, "modbus": modbus}

@app.get("/health/ready")
async def health_ready():
    if not is_db_ready():
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "db": get_db_status(),
                "modbus": {
                    "running": bool(getattr(modbus_manager, "running", False)),
                    "device_count": len(getattr(modbus_manager, "devices", {}) or {}),
                },
            },
        )
    return {
        "status": "ready",
        "db": get_db_status(),
        "modbus": {
            "running": bool(getattr(modbus_manager, "running", False)),
            "device_count": len(getattr(modbus_manager, "devices", {}) or {}),
        },
    }

# Mount Socket.IO
# Note: We wrap the FastAPI app with the Socket.IO ASGI app
# This means 'app_entry' should be the target for uvicorn
app_entry = socketio.ASGIApp(sio, other_asgi_app=app, socketio_path="/ws/socket.io")
