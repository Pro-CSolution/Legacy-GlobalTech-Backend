import asyncio
import logging
import random
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, text

from app.core.config import settings

logger = logging.getLogger("app.db.session")

# Create Async Engine
engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True)

# Create Async Session Factory
async_session_factory = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

class DatabaseUnavailableError(RuntimeError):
    """Raised when the database is not ready/available for request handling."""


# Simple readiness signal for the rest of the app (health endpoints, dependencies, services)
_db_ready_event: asyncio.Event = asyncio.Event()
_db_status: dict = {
    "ready": False,
    "attempt": 0,
    "last_error": None,
    "last_error_at": None,
    "last_success_at": None,
}


def is_db_ready() -> bool:
    return _db_ready_event.is_set()


def get_db_status() -> dict:
    # Return a copy safe for JSON serialization.
    return dict(_db_status)


async def get_session() -> AsyncSession:
    """Dependency for FastAPI to get DB session"""
    if not _db_ready_event.is_set():
        raise DatabaseUnavailableError("Database is not ready yet.")
    async with async_session_factory() as session:
        yield session

async def init_db() -> None:
    """
    One-shot DB initialization:
    - Ensures expected schema/table shapes (local helper)
    - Creates SQLModel tables
    - Ensures TimescaleDB extension + hypertable (best-effort)

    IMPORTANT (production): this function must NOT terminate the process.
    It raises on failure so a caller can retry/backoff without killing the service.
    """
    async with engine.connect() as conn:
        await _ensure_trend_data_schema(conn)

    async with engine.begin() as conn:
        # Create tables defined in SQLModel
        await conn.run_sync(SQLModel.metadata.create_all)

    # Ensure performance indexes (best-effort; no migrations in this project)
    async with engine.connect() as conn:
        await _ensure_trend_data_indexes(conn)
        await _ensure_trip_event_indexes(conn)

    # Setup TimescaleDB Hypertable in a separate connection/transaction (best-effort)
    async with engine.connect() as conn:
        try:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
            await conn.commit()

            # Convert to hypertable (idempotent with if_not_exists)
            await conn.execute(
                text("SELECT create_hypertable('trend_data', 'time', if_not_exists => TRUE);")
            )
            await conn.commit()
            logger.info("TimescaleDB hypertable initialized successfully.")
        except Exception:
            logger.warning("Warning initializing TimescaleDB (continuing).", exc_info=True)

    _db_ready_event.set()
    _db_status["ready"] = True
    _db_status["last_error"] = None
    _db_status["last_error_at"] = None
    _db_status["last_success_at"] = datetime.now(timezone.utc).isoformat()


async def init_db_with_retry(
    *,
    max_attempts: int | None = None,
    initial_delay_s: float = 1.0,
    max_delay_s: float = 4,
    backoff: float = 1.7,
    jitter_ratio: float = 0.2,
) -> None:
    """
    Production-safe retry loop:
    - Never exits the process
    - Retries with backoff + jitter
    - Marks readiness when initialization succeeds
    """
    delay = max(0.1, float(initial_delay_s))
    attempt = 0

    # Start as NOT ready
    _db_ready_event.clear()
    _db_status["ready"] = False

    while max_attempts is None or attempt < max_attempts:
        attempt += 1
        _db_status["attempt"] = attempt
        try:
            logger.info("Initializing DB (attempt %d)...", attempt)
            await init_db()
            logger.info("Database is ready.")
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _db_ready_event.clear()
            _db_status["ready"] = False
            _db_status["last_error"] = str(e)
            _db_status["last_error_at"] = datetime.now(timezone.utc).isoformat()

            # Log compact message every attempt; full traceback only at DEBUG level.
            logger.warning(
                "Database not available yet (attempt %d). Retrying in %.1fs. Error: %s",
                attempt,
                delay,
                str(e),
            )
            logger.debug("DB init failure details:", exc_info=True)

            # Backoff + jitter
            jitter = delay * jitter_ratio * random.random()
            await asyncio.sleep(delay + jitter)
            delay = min(max_delay_s, delay * backoff)

    logger.error(
        "Database not ready after %s attempts. Service will keep running, but DB endpoints remain unavailable.",
        max_attempts,
    )


async def close_db() -> None:
    """Graceful shutdown hook."""
    try:
        await engine.dispose()
    except Exception:
        logger.debug("Error disposing DB engine.", exc_info=True)


async def _ensure_trend_data_schema(conn):
    """Correcciones rápidas para ambientes locales sin migraciones."""
    await _enforce_composite_pk(conn)
    await _ensure_time_timestamptz(conn)
    # Índices se aseguran luego de `create_all` (ver init_db)


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
        logger.warning(
            "DB Migration: Detected old structure (simple PK). Recreating trend_data table..."
        )
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
        logger.warning("DB Migration: Recreating trend_data with TIMESTAMPTZ time column...")
        # Para evitar conflictos de índices en hypertables, se recrea la tabla.
        try:
            await conn.execute(text("SELECT drop_chunks(interval '1 day', 'trend_data');"))
        except Exception:
            # Si falla, limpiamos la transacción para poder continuar con el drop.
            await conn.rollback()

        await conn.execute(text("DROP TABLE IF EXISTS trend_data CASCADE;"))
        await conn.commit()


async def _ensure_trend_data_indexes(conn):
    """
    Índices recomendados para queries típicas de tendencia:
    - Filtrado por device_id + parameter_id
    - Rango temporal (time) + ORDER BY time DESC + LIMIT
    """
    try:
        await conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_trend_data_device_param_time_desc
                ON trend_data (device_id, parameter_id, time DESC);
                """
            )
        )
        await conn.commit()
        logger.info("DB Migration: ensured idx_trend_data_device_param_time_desc")
    except Exception:
        # Nunca debe bloquear el arranque (especialmente si el usuario no tiene permisos)
        await conn.rollback()
        logger.warning(
            "DB Migration: could not ensure trend_data indexes (continuing).", exc_info=True
        )


async def _ensure_trip_event_indexes(conn):
    """Indexes for trip history lookups and retention cleanup."""
    try:
        await conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_trip_events_device_time_desc
                ON trip_events (device_id, time DESC);
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_trip_events_time
                ON trip_events (time);
                """
            )
        )
        await conn.commit()
        logger.info("DB Migration: ensured trip_events indexes")
    except Exception:
        await conn.rollback()
        logger.warning(
            "DB Migration: could not ensure trip_events indexes (continuing).",
            exc_info=True,
        )
