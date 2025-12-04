from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, text
from app.core.config import settings

# Create Async Engine
engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True)

# Create Async Session Factory
async_session_factory = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

async def get_session() -> AsyncSession:
    """Dependency for FastAPI to get DB session"""
    async with async_session_factory() as session:
        yield session

async def init_db():
    """Initialize database tables and hypertable for TimescaleDB"""
    
    # Detectar y corregir esquema antiguo si es necesario
    # Esto es una migración simple para desarrollo. En prod usaríamos Alembic.
    async with engine.connect() as conn:
        try:
            # Verificar cuántas columnas PK tiene la tabla trend_data
            result = await conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.key_column_usage 
                WHERE table_name = 'trend_data' 
                AND constraint_name = (
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_name = 'trend_data' 
                    AND constraint_type = 'PRIMARY KEY'
                    LIMIT 1
                )
            """))
            pk_count = result.scalar()
            
            # Si existe y tiene menos de 3 PKs (el modelo viejo tenía solo 'time'), la borramos
            if pk_count is not None and pk_count > 0 and pk_count < 3:
                print("Migración DB: Detectada estructura antigua (PK simple). Recreando tabla trend_data...")
                # Borrar hypertable primero si existe
                try:
                    await conn.execute(text("SELECT drop_chunks(interval '1 day', 'trend_data');"))
                except Exception:
                    pass # Puede fallar si no es hypertable aún
                
                await conn.execute(text("DROP TABLE IF EXISTS trend_data CASCADE;"))
                await conn.commit()
                
        except Exception as e:
            # La tabla puede no existir, lo cual está bien
            print(f"Chequeo de migración (no crítico): {e}")
            
    async with engine.begin() as conn:
        # Create tables defined in SQLModel
        await conn.run_sync(SQLModel.metadata.create_all)
        
    # Setup TimescaleDB Hypertable in a separate connection/transaction
    async with engine.connect() as conn:
        try:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
            await conn.commit()
            
            # Convert to hypertable
            await conn.execute(text("SELECT create_hypertable('trend_data', 'time', if_not_exists => TRUE);"))
            await conn.commit()
            print("TimescaleDB hypertable initialized successfully.")
        except Exception as e:
            print(f"Warning initializing TimescaleDB: {e}")
