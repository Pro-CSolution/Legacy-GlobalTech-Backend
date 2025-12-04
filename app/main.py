from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import socketio
import logging

# Usamos el logger de uvicorn para consistencia, o uno específico
logger = logging.getLogger("app.main")

from app.core.config import settings
from app.db.session import init_db
from app.modbus_engine.manager import modbus_manager
from app.services.socket_manager import sio
from app.services.data_logger import data_logger
from app.api.v1.endpoints import data

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup Events
    logger.info("System Starting up...")
    
    # 1. Init DB
    await init_db()
    
    # 2. Start Modbus Engine
    logger.info("Initializing Modbus Manager...")
    await modbus_manager.start()
    
    # 3. Start Data Logger
    logger.info("Initializing Data Logger...")
    await data_logger.start()
    
    yield
    
    # Shutdown Events
    logger.info("System Shutting down...")
    await modbus_manager.stop()
    await data_logger.stop()

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

@app.get("/")
async def root():
    return {"status": "online", "project": settings.PROJECT_NAME}

# Mount Socket.IO
# Note: We wrap the FastAPI app with the Socket.IO ASGI app
# This means 'app_entry' should be the target for uvicorn
app_entry = socketio.ASGIApp(sio, other_asgi_app=app, socketio_path='/ws/socket.io')
