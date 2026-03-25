import asyncio
import logging
from typing import Protocol, List, Dict
from concurrent.futures import CancelledError

logger = logging.getLogger(__name__)

class Projector(Protocol):
    name: str
    async def setup(self, conn) -> None:
        ...
    async def handle_event(self, event: dict, conn) -> None:
        ...

class ProjectionDaemon:
    def __init__(self, store, db_pool, projectors: List[Projector], batch_size: int = 500, poll_interval: float = 1.0):
        self.store = store
        self.db_pool = db_pool
        self.projectors = projectors
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self):
        self._running = True
        # Ensure all projectors are setup
        async with self.db_pool.acquire() as conn:
            for projector in self.projectors:
                if hasattr(projector, "setup"):
                    await projector.setup(conn)
                
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"ProjectionDaemon started with {len(self.projectors)} projectors")

    async def stop(self):
        self._running = False
        task = self._task
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, CancelledError):
                pass
            self._task = None
        logger.info("ProjectionDaemon stopped")

    async def _run_loop(self):
        while self._running:
            try:
                # Process each projector
                for projector in self.projectors:
                    if not self._running: break
                    await self._process_projector(projector)
                
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Daemon loop error: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval * 5) # Backoff

    async def _process_projector(self, projector: Projector):
        async with self.db_pool.acquire() as conn:
            checkpoint = await self._get_checkpoint(conn, projector.name)
            last_pos = checkpoint
            
            # Use a transaction for the batch update
            async with conn.transaction():
                count = 0
                async for event in self.store.load_all(from_position=checkpoint, batch_size=self.batch_size):
                    # event is a StoredEvent object (Pydantic model)
                    if hasattr(event, "model_dump"):
                        event_dict = event.model_dump()
                        current_pos = event.global_position
                    else:
                        event_dict = event
                        current_pos = event["global_position"]
                    
                    await projector.handle_event(event_dict, conn)
                    last_pos = current_pos
                    count += 1
                    if count >= self.batch_size:
                        break

                if last_pos > checkpoint:
                    await self._save_checkpoint(conn, projector.name, last_pos)
                    logger.debug(f"Projector {projector.name} advanced to {last_pos}")

    async def rebuild(self, projector_name: str):
        """Wipes a projection and rebuilds from global position 0."""
        projector = next((p for p in self.projectors if p.name == projector_name), None)
        if not projector:
            raise ValueError(f"Unknown projector: {projector_name}")
            
        logger.info(f"Rebuilding projector: {projector_name}")
        async with self.db_pool.acquire() as conn:
            await conn.execute("UPDATE projection_checkpoints SET last_position = 0 WHERE projection_name = $1", projector_name)

    async def _get_checkpoint(self, conn, name: str) -> int:
        row = await conn.fetchrow(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1", 
            name
        )
        if not row:
            await self._save_checkpoint(conn, name, 0)
            return 0
        return row["last_position"]

    async def _save_checkpoint(self, conn, name: str, position: int):
        await conn.execute(
            """
            INSERT INTO projection_checkpoints (projection_name, last_position)
            VALUES ($1, $2)
            ON CONFLICT (projection_name) DO UPDATE SET last_position = excluded.last_position, updated_at = NOW()
            """,
            name, position
        )

    async def get_lag(self) -> Dict[str, int]:
        lags = {}
        async with self.db_pool.acquire() as conn:
            max_pos = await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events")
            for projector in self.projectors:
                cp = await self._get_checkpoint(conn, projector.name)
                lags[projector.name] = max_pos - (cp or 0)
        return lags
