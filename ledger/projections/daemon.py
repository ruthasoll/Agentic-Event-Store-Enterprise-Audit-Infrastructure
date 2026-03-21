import asyncio
import logging
from typing import Protocol, List
from concurrent.futures import CancelledError

logger = logging.getLogger(__name__)

class Projector(Protocol):
    name: str
    async def handle_event(self, event: dict, conn) -> None:
        ...

class ProjectionDaemon:
    def __init__(self, store, db_pool, projectors: List[Projector], batch_size: int = 500, poll_interval: float = 0.5):
        self.store = store
        self.db_pool = db_pool
        self.projectors = projectors
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self._task = None

    async def start(self):
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_loop(self):
        while True:
            try:
                async with self.db_pool.acquire() as conn:
                    # simplistic check: we assume a single combined checkpoint or one per projector
                    for projector in self.projectors:
                        checkpoint = await self._get_checkpoint(conn, projector.name)
                        new_pos = checkpoint
                        
                        # Process batch
                        async for event in self.store.load_all(from_position=checkpoint, batch_size=self.batch_size):
                            await projector.handle_event(event, conn)
                            new_pos = event["global_position"]

                        if new_pos > checkpoint:
                            await self._save_checkpoint(conn, projector.name, new_pos)
                            
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Daemon error: {e}")
                await asyncio.sleep(self.poll_interval * 2) # Backoff

    async def _get_checkpoint(self, conn, name: str) -> int:
        row = await conn.fetchrow(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1", 
            name
        )
        return row["last_position"] if row else 0

    async def _save_checkpoint(self, conn, name: str, position: int):
        await conn.execute(
            """
            INSERT INTO projection_checkpoints (projection_name, last_position)
            VALUES ($1, $2)
            ON CONFLICT (projection_name) DO UPDATE SET last_position = excluded.last_position, updated_at = NOW()
            """,
            name, position
        )

    async def get_lag(self) -> dict:
        """Expose lag metric for health checks."""
        lags = {}
        async with self.db_pool.acquire() as conn:
            max_pos = await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events")
            for projector in self.projectors:
                cp = await self._get_checkpoint(conn, projector.name)
                lags[projector.name] = max_pos - cp
        return lags
