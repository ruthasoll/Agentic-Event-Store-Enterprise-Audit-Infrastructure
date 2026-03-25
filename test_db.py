# test_db.py
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def test_connection():
    try:
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        print("✅ SUCCESS! Connected to Neon database")
        print(f"Database version: {await conn.fetchval('SELECT version()')}")
        await conn.close()
    except Exception as e:
        print("❌ Connection failed:")
        print(e)

asyncio.run(test_connection())