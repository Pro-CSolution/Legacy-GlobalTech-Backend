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

    async with engine.connect() as conn:
        try:
            await _ensure_trend_data_schema(conn)
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


async def _ensure_trend_data_schema(conn):
    """Correcciones rápidas para ambientes locales sin migraciones."""
    await _enforce_composite_pk(conn)
    await _ensure_time_timestamptz(conn)


async def _enforce_composite_pk(conn):
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

    if pk_count is not None and pk_count > 0 and pk_count < 3:
        print("Migración DB: Detectada estructura antigua (PK simple). Recreando tabla trend_data...")
        try:
            await conn.execute(text("SELECT drop_chunks(interval '1 day', 'trend_data');"))
        except Exception:
            pass  # Puede fallar si no es hypertable aún

        await conn.execute(text("DROP TABLE IF EXISTS trend_data CASCADE;"))
        await conn.commit()


async def _ensure_time_timestamptz(conn):
    result = await conn.execute(text("""
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = 'trend_data' 
        AND column_name = 'time'
        LIMIT 1;
    """))

    data_type = result.scalar()
    if data_type and data_type.strip().lower() == "timestamp without time zone":
        print("Migración DB: Recreando trend_data con columna time TIMESTAMPTZ...")
        # Para evitar conflictos de índices en hypertables, se recrea la tabla.
        try:
            await conn.execute(text("SELECT drop_chunks(interval '1 day', 'trend_data');"))
        except Exception:
            # Si falla, limpiamos la transacción para poder continuar con el drop.
            await conn.rollback()

        await conn.execute(text("DROP TABLE IF EXISTS trend_data CASCADE;"))
        await conn.commit()
