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
    async def clear_state(self, conn) -> None:
        """Wipe the projection table for a fresh rebuild."""
        ...

class ProjectionDaemon:
    def __init__(self, store, db_pool, projectors: List[Projector], batch_size: int = 500, poll_interval: float = 1.0):
        self.store = store
        self.db_pool = db_pool
        self.projectors = projectors
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self._task: asyncio.Task | None = None
        self._rebuild_tasks: Dict[str, asyncio.Task] = {}
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

    async def async_rebuild(self, projector_name: str):
        """
        Starts a background rebuild of a projector from position 0.
        Does not block the main loop's processing of other projectors.
        """
        if projector_name in self._rebuild_tasks and not self._rebuild_tasks[projector_name].done():
            logger.warning(f"Rebuild already in progress for {projector_name}")
            return
            
        task = asyncio.create_task(self._perform_rebuild(projector_name))
        self._rebuild_tasks[projector_name] = task
        logger.info(f"Started background rebuild task for {projector_name}")

    async def _perform_rebuild(self, projector_name: str):
        try:
            projector = next((p for p in self.projectors if p.name == projector_name), None)
            if not projector: raise ValueError(f"Unknown projector: {projector_name}")
            
            async with self.db_pool.acquire() as conn:
                # 1. Clear current state (Mastery: Implement shadow tables if true zero-downtime needed)
                if hasattr(projector, "clear_state"):
                    await projector.clear_state(conn)
                
                # 2. Reset checkpoint
                await self._save_checkpoint(conn, projector_name, 0)
                
            # 3. The main loop will now pick it up from 0 in the next cycle.
            # Alternatively, we can force-process it here in a separate loop if we want it to be faster.
            logger.info(f"Rebuild initialized for {projector_name}. Main loop will re-process from 0.")
            
        except Exception as e:
            logger.error(f"Rebuild task failed for {projector_name}: {e}", exc_info=True)

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
