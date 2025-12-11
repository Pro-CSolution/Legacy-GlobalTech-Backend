import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from app.core.config import settings

async def reset_db():
    print(f"Connecting to {settings.DATABASE_URL}")
    engine = create_async_engine(settings.DATABASE_URL, echo=True)
    async with engine.begin() as conn:
        print("Dropping tables...")
        await conn.execute(text("DROP TABLE IF EXISTS profile_parameters CASCADE;"))
        await conn.execute(text("DROP TABLE IF EXISTS profiles CASCADE;"))
        print("Tables dropped.")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(reset_db())

