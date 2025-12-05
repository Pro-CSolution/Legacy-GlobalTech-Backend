import asyncio
import logging
from datetime import datetime	
from app.modbus_engine.state import state_manager
from app.db.session import async_session_factory
from app.db.models import TrendData
from app.modbus_engine.manager import modbus_manager
from app.core.utils import utc_now

logger = logging.getLogger(__name__)

class DataLogger:
    def __init__(self):
        self.running = False
        # Intervalo de guardado en DB (independiente del polling de Modbus)
        self.log_interval = 5.0 
        
    async def start(self):
        self.running = True
        logger.info("Starting Data Logger...")
        asyncio.create_task(self._loop())
        
    async def stop(self):
        self.running = False
        logger.info("Data Logger Stopped")
        
    async def _loop(self):
        while self.running:
            try:
                await asyncio.sleep(self.log_interval)
                
                async with async_session_factory() as session:
                    entries = []
                    timestamp = utc_now()
                    
                    current_data = state_manager.get_snapshot()
                    
                    for device_id, data in current_data.items():
                        # Obtener IDs críticos desde la configuración del manager
                        critical_ids = []
                        if device_id in modbus_manager.devices:
                             critical_ids = modbus_manager.devices[device_id].get("critical_parameters", [])
                        
                        for pid in critical_ids:
                            if pid in data:
                                val = data[pid]
                                # Validar que sea numérico antes de guardar
                                try:
                                    val_float = float(val)
                                    entry = TrendData(
                                        time=timestamp,
                                        device_id=device_id,
                                        parameter_id=pid,
                                        value=val_float
                                    )
                                    entries.append(entry)
                                except (ValueError, TypeError):
                                    pass
                    
                    if entries:
                        session.add_all(entries)
                        await session.commit()
                        logger.debug(f"Logged {len(entries)} data points to DB")
                        
            except Exception as e:
                logger.error(f"Error in DataLogger: {e}")
                await asyncio.sleep(1)

data_logger = DataLogger()

