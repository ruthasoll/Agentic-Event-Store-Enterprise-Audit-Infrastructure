import asyncio
import logging
from typing import Protocol, List
from ledger.schema.events import StoredEvent

logger = logging.getLogger(__name__)

class Projector(Protocol):
    name: str
    async def setup(self, conn) -> None:
        ...
    async def handle_event(self, event: StoredEvent, conn) -> None:
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
        if self.db_pool:
            async with self.db_pool.acquire() as conn:
                for p in self.projectors:
                    if hasattr(p, "setup"):
                        await p.setup(conn)
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
                    checkpoints = {}
                    for p in self.projectors:
                        cp = await self._get_checkpoint(conn, p.name)
                        checkpoints[p.name] = cp
                    
                    if not checkpoints:
                        await asyncio.sleep(self.poll_interval)
                        continue
                        
                    min_cp = min(checkpoints.values())
                    
                    async for event in self.store.load_all(from_position=min_cp, batch_size=self.batch_size):
                        for projector in self.projectors:
                            if checkpoints[projector.name] < event.global_position:
                                try:
                                    await projector.handle_event(event, conn)
                                except asyncio.CancelledError:
                                    raise
                                except Exception as e:
                                    logger.error(f"Projector {projector.name} failed on event {event.event_id}: {e}")
                                else:
                                    checkpoints[projector.name] = event.global_position
                        
                        for pname, cval in checkpoints.items():
                            if cval > await self._get_checkpoint(conn, pname):
                                await self._save_checkpoint(conn, pname, cval)
                            
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Daemon error: {e}")
                await asyncio.sleep(self.poll_interval * 2)

    async def _get_checkpoint(self, conn, name: str) -> int:
        if not self.db_pool:
            return await self.store.load_checkpoint(name) if hasattr(self.store, "load_checkpoint") else 0
        row = await conn.fetchrow(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1", 
            name
        )
        return row["last_position"] if row else 0

    async def _save_checkpoint(self, conn, name: str, position: int):
        if not self.db_pool:
            if hasattr(self.store, "save_checkpoint"):
                await self.store.save_checkpoint(name, position)
            return
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
        if not self.db_pool:
            max_pos = len(self.store._global) if hasattr(self.store, "_global") else 0
            for projector in self.projectors:
                cp = await self._get_checkpoint(None, projector.name)
                lags[projector.name] = max_pos - cp
            return lags
            
        async with self.db_pool.acquire() as conn:
            max_pos = await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events")
            for projector in self.projectors:
                cp = await self._get_checkpoint(conn, projector.name)
                lags[projector.name] = max_pos - cp
        return lags
