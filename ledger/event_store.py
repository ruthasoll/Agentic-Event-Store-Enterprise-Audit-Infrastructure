"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
COMPLETION CHECKLIST (implement in order):
  [ ] Phase 1, Day 1: append() + stream_version()
  [ ] Phase 1, Day 1: load_stream()
  [ ] Phase 1, Day 2: load_all()  (needed for projection daemon)
  [ ] Phase 1, Day 2: get_event() (needed for causation chain)
  [ ] Phase 4:        UpcasterRegistry.upcast() integration in load_stream/load_all
"""
from __future__ import annotations
import json
from datetime import datetime, UTC
from typing import AsyncGenerator, Callable, Any, List
from uuid import UUID
import asyncpg
from ledger.schema.events import StoredEvent, StreamMetadata
from ledger.integrity import calculate_event_hash


class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id; self.expected = expected; self.actual = actual
        super().__init__(f"OCC on '{stream_id}': expected v{expected}, actual v{actual}")


class EventStore:
    """
    Append-only PostgreSQL event store. All agents and projections use this class.

    IMPLEMENT IN ORDER — see inline guides in each method:
      1. stream_version()   — simplest, needed immediately
      2. append()           — most critical; OCC correctness is the exam
      3. load_stream()      — needed for aggregate replay
      4. load_all()         — async generator, needed for projection daemon
      5. get_event()        — needed for causation chain audit
    """

    def __init__(self, db_url: str, upcaster_registry=None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=10)

    async def close(self) -> None:
        if self._pool: await self._pool.close()

    async def stream_version(self, stream_id: str) -> int:
        """
        Returns current version, or -1 if stream doesn't exist.
        """
        if not self._pool: raise RuntimeError("EventStore not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id)
            return row["current_version"] if row else -1
        raise RuntimeError("Failed to get stream version")

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,    # -1=new stream, 0+=expected current
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        """
        Appends events atomically with OCC. Returns list of positions assigned.

        FULL IMPLEMENTATION GUIDE — copy, uncomment, and complete:

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock stream row (prevents concurrent appends)
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE", stream_id)

                # 2. OCC check
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current)

                # 3. Create stream if new
                if row is None:
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version)"
                        " VALUES($1, $2, 0)",
                        stream_id, stream_id.split("-")[0])

                # 4. Insert each event
                positions = []
                meta = {**(metadata or {})}
                if causation_id: meta["causation_id"] = causation_id
                for i, event in enumerate(events):
                    pos = expected_version + 1 + i
                    await conn.execute(
                        "INSERT INTO events(stream_id, stream_position, event_type,"
                        " event_version, payload, metadata, recorded_at)"
                        " VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7)",
                        stream_id, pos,
                        event["event_type"], event["event_version"],
                        json.dumps(event["payload"]),
                        json.dumps(meta),
                        datetime.now(UTC))
                    positions.append(pos)

                # 5. Update stream version
                await conn.execute(
                    "UPDATE event_streams SET current_version=$1 WHERE stream_id=$2",
                    expected_version + len(events), stream_id)
                return positions
        """
        if not self._pool: raise RuntimeError("EventStore not connected")
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock stream row
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE", stream_id)

                # 2. OCC check
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current)

                if row and row.get("archived_at"):
                    raise ValueError(f"Stream '{stream_id}' is archived and cannot be appended to.")

                # 3. Create stream if new
                if row is None:
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version)"
                        " VALUES($1, $2, 0)",
                        stream_id, stream_id.split("-")[0])

                # 3. Get last event's hash for integrity chain
                last_hash_row = await conn.fetchrow(
                    "SELECT integrity_hash FROM events WHERE stream_id = $1 "
                    "ORDER BY stream_position DESC LIMIT 1", stream_id)
                prev_hash = last_hash_row["integrity_hash"] if last_hash_row else None

                # 4. Insert each event
                positions = []
                meta = {**(metadata or {})}
                if causation_id: meta["causation_id"] = causation_id
                
                for i, event in enumerate(events):
                    pos = expected_version + 1 + i
                    payload = event.get("payload", {})
                    event_type = event["event_type"]
                    event_version = event.get("event_version", 1)
                    
                    # Calculate integrity hash
                    integrity_hash = calculate_event_hash(
                        prev_hash, stream_id, pos, event_type, event_version, payload, meta
                    )
                    
                    event_id = await conn.fetchval(
                        "INSERT INTO events(stream_id, stream_position, event_type,"
                        " event_version, payload, metadata, recorded_at, integrity_hash, previous_hash)"
                        " VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9) RETURNING event_id",
                        stream_id, pos,
                        event_type, event_version,
                        json.dumps(payload),
                        json.dumps(meta),
                        datetime.now(UTC),
                        integrity_hash,
                        prev_hash)
                    
                    positions.append(pos)
                    prev_hash = integrity_hash
                    
                    # 4.5 Insert outbox entry natively
                    await conn.execute(
                        "INSERT INTO outbox(event_id, destination, payload)"
                        " VALUES($1, $2, $3::jsonb)",
                        event_id, f"{stream_id.split('-')[0]}-events", json.dumps(event.get("payload", {}))
                    )

                # 5. Update stream version
                await conn.execute(
                    "UPDATE event_streams SET current_version=$1 WHERE stream_id=$2",
                    expected_version + len(events), stream_id)
                return positions
        raise RuntimeError("Failed to append events")

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """
        Loads events from a stream in stream_position order.
        Applies upcasters if self.upcasters is set.

        """
        if not self._pool: raise RuntimeError("EventStore not connected")
        async with self._pool.acquire() as conn:
            q = ("SELECT event_id, stream_id, stream_position, event_type,"
                 " event_version, payload, metadata, recorded_at"
                 " FROM events WHERE stream_id=$1 AND stream_position>=$2")
            params = [stream_id, from_position]
            if to_position is not None:
                q += " AND stream_position<=$3"; params.append(to_position)
            q += " ORDER BY stream_position ASC"
            rows = await conn.fetch(q, *params)
            events = []
            for row in rows:
                e = {**dict(row), "payload": json.loads(row["payload"]),
                                   "metadata": json.loads(row["metadata"])}
                if self.upcasters: e = self.upcasters.upcast(e)
                events.append(StoredEvent(**e))
            return events
        raise RuntimeError("Failed to load stream")

    async def load_all(
        self, from_position: int = 0, batch_size: int = 500
    ) -> AsyncGenerator[StoredEvent, None]:
        """
        Async generator yielding all events by global_position.
        Used by the ProjectionDaemon.

        """
        if not self._pool: raise RuntimeError("EventStore not connected")
        async with self._pool.acquire() as conn:
            pos = from_position
            while True:
                rows: List[Any] = await conn.fetch(
                    "SELECT global_position, event_id, stream_id, stream_position,"
                    " event_type, event_version, payload, metadata, recorded_at"
                    " FROM events WHERE global_position > $1"
                    " ORDER BY global_position ASC LIMIT $2",
                    pos, batch_size)
                if not rows: break
                for row in rows:
                    e = {**dict(row), "payload": json.loads(row["payload"]),
                                       "metadata": json.loads(row["metadata"])}
                    if self.upcasters: e = self.upcasters.upcast(e)
                    yield StoredEvent(**e)
                pos = rows[-1]["global_position"]
                if len(rows) < batch_size: break
        # No return needed for AsyncGenerator path completion

    async def get_event(self, event_id: UUID) -> StoredEvent | None:
        """
        Loads one event by UUID. Used for causation chain lookups.

        """
        if not self._pool: raise RuntimeError("EventStore not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM events WHERE event_id=$1", event_id)
            if not row: return None
            e = {**dict(row), "payload": json.loads(row["payload"]),
                              "metadata": json.loads(row["metadata"])}
            if self.upcasters: e = self.upcasters.upcast(e)
            return StoredEvent(**e)
        raise RuntimeError("Failed to get event")

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        """
        Returns metadata for an event stream.
        """
        if not self._pool: raise RuntimeError("EventStore not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM event_streams WHERE stream_id = $1",
                stream_id)
            if not row: return None
            return StreamMetadata(
                stream_id=row["stream_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                created_at=row["created_at"],
                archived_at=row["archived_at"],
                metadata=json.loads(row["metadata"])
            )
        raise RuntimeError("Failed to get stream metadata")

    async def list_streams(self) -> list[StreamMetadata]:
        """
        Returns metadata for all streams in the store.
        """
        if not self._pool: raise RuntimeError("EventStore not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM event_streams ORDER BY stream_id ASC")
            return [StreamMetadata(
                stream_id=row["stream_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                created_at=row["created_at"],
                archived_at=row["archived_at"],
                metadata=json.loads(row["metadata"])
            ) for row in rows]

    async def archive_stream(self, stream_id: str, expected_version: int) -> None:
        """
        Archives a stream, preventing further appends.
        Respects OCC to ensure we archive the version we think we are archiving.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock and check version
                row = await conn.fetchrow(
                    "SELECT current_version, archived_at FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE", stream_id)
                
                if not row:
                    raise ValueError(f"Stream '{stream_id}' not found.")
                
                if row["archived_at"]:
                    return # Already archived
                
                if row["current_version"] != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, row["current_version"])
                
                # 2. Set archived_at
                await conn.execute(
                    "UPDATE event_streams SET archived_at = $1 WHERE stream_id = $2",
                    datetime.now(UTC), stream_id)

    async def verify_stream_integrity(self, stream_id: str) -> bool:
        """
        Re-calculates the hash chain for a stream and returns True if valid.
        """
        if not self._pool: raise RuntimeError("EventStore not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM events WHERE stream_id = $1 ORDER BY stream_position ASC",
                stream_id)
            
            prev_hash = None
            for row in rows:
                if row["previous_hash"] != prev_hash:
                    return False
                
                calc = calculate_event_hash(
                    prev_hash,
                    row["stream_id"],
                    row["stream_position"],
                    row["event_type"],
                    row["event_version"],
                    json.loads(row["payload"]),
                    json.loads(row["metadata"])
                )
                if row["integrity_hash"] != calc:
                    return False
                prev_hash = calc
            return True


# ─────────────────────────────────────────────────────────────────────────────
# UPCASTER REGISTRY — Phase 4
# ─────────────────────────────────────────────────────────────────────────────

class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.

    REGISTER AN UPCASTER:
        registry = UpcasterRegistry()

        @registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
        def upcast_credit_v1_v2(payload: dict) -> dict:
            # v2 adds model_versions dict
            payload.setdefault("model_versions", {})
            return payload

    REQUIRED FOR PHASE 4:
        - DecisionGenerated        v1 → v2  (adds model_versions: dict)
    """

    def __init__(self):
        self._upcasters: dict[str, dict[int, Callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    def upcast(self, event: dict) -> dict:
        """Apply chain of upcasters until latest version reached."""
        et = event["event_type"]
        v = event.get("event_version", 1)
        chain = self._upcasters.get(et, {})
        while v in chain:
            event["payload"] = chain[v](dict(event["payload"]))
            v += 1
            event["event_version"] = v
        return event

def get_default_upcaster_registry() -> UpcasterRegistry:
    """Returns registry with Phase 4 standard upcasters."""
    registry = UpcasterRegistry()

    @registry.upcaster("DecisionGenerated", from_version=1, to_version=2)
    def upcast_decision_v1_v2(payload: dict) -> dict:
        # v2 adds model_versions dict
        payload.setdefault("model_versions", {})
        return payload

    @registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
    def upcast_credit_v1_v2(payload: dict) -> dict:
        # v2 adds regulatory_basis list
        payload.setdefault("regulatory_basis", [])
        return payload

    return registry


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for tests only
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for Phase 1 tests only
# Identical interface to EventStore. Drop-in for tests; never use in production.
# ─────────────────────────────────────────────────────────────────────────────

import asyncio as _asyncio
from collections import defaultdict as _defaultdict
from datetime import datetime as _datetime
from uuid import uuid4 as _uuid4

class InMemoryEventStore:
    """
    Thread-safe (asyncio-safe) in-memory event store.
    Used exclusively in Phase 1 tests and conftest fixtures.
    Same interface as EventStore — swap one for the other with no code changes.
    """

    def __init__(self, upcaster_registry=None):
        # stream_id -> list of event dicts
        self._streams: dict[str, list[dict]] = _defaultdict(list)
        # stream_id -> current version (position of last event, -1 if empty)
        self._versions: dict[str, int] = {}
        self.upcasters = upcaster_registry
        # global append log (ordered by insertion)
        self._global: list[dict] = []
        # projection checkpoints
        self._checkpoints: dict[str, int] = {}
        # asyncio lock per stream for OCC
        self._locks: dict[str, _asyncio.Lock] = _defaultdict(_asyncio.Lock)
        # stream_id -> archived_at (datetime)
        self._archived: dict[str, _datetime] = {}

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, 0)

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, 0)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            if self._archived.get(stream_id):
                raise ValueError(f"Stream '{stream_id}' is archived and cannot be appended to.")

            positions = []
            meta = {**(metadata or {})}
            if causation_id:
                meta["causation_id"] = causation_id

            for i, event in enumerate(events):
                pos = current + 1 + i
                payload = dict(event.get("payload", {}))
                event_type = event["event_type"]
                event_version = event.get("event_version", 1)

                # Fetch last hash for stream
                stream_events = self._streams.get(stream_id, [])
                last_hash = stream_events[-1]["integrity_hash"] if stream_events else None

                integrity_hash = calculate_event_hash(
                    last_hash, stream_id, pos, event_type, event_version, payload, meta
                )

                stored = {
                    "event_id": str(_uuid4()),
                    "stream_id": stream_id,
                    "stream_position": pos,
                    "global_position": len(self._global),
                    "event_type": event_type,
                    "event_version": event_version,
                    "payload": payload,
                    "metadata": meta,
                    "recorded_at": _datetime.now(UTC).isoformat(),
                    "integrity_hash": integrity_hash,
                    "previous_hash": last_hash,
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                positions.append(pos)

            self._versions[stream_id] = current + len(events)
            return positions
        return []

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        events = [
            e for e in self._streams.get(stream_id, [])
            if e["stream_position"] >= from_position
            and (to_position is None or e["stream_position"] <= to_position)
        ]
        sorted_events = sorted(events, key=lambda e: e["stream_position"])
        if self.upcasters:
            sorted_events = [self.upcasters.upcast(dict(e)) for e in sorted_events]
        return [StoredEvent(**e) for e in sorted_events]

    async def load_all(self, from_position: int = 0, batch_size: int = 500) -> AsyncGenerator[StoredEvent, None]:
        for e in self._global:
            if e["global_position"] >= from_position:
                event_to_yield = dict(e)
                if self.upcasters:
                    event_to_yield = self.upcasters.upcast(event_to_yield)
                yield StoredEvent(**event_to_yield)

    async def get_event(self, event_id: str) -> StoredEvent | None:
        for e in self._global:
            if e["event_id"] == str(event_id):
                event_to_yield = dict(e)
                if self.upcasters:
                    event_to_yield = self.upcasters.upcast(event_to_yield)
                return StoredEvent(**event_to_yield)
        return None

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        async with self._locks[stream_id]:
            if stream_id not in self._streams:
                return None
            
            # Find first event for created_at
            events = self._streams[stream_id]
            created_at = events[0]["recorded_at"] if events else _datetime.utcnow()
            
            return StreamMetadata(
                stream_id=stream_id,
                aggregate_type=stream_id.split("-")[0],
                current_version=self._versions.get(stream_id, 0),
                created_at=created_at,
                archived_at=self._archived.get(stream_id),
                metadata={}
            )

    async def list_streams(self) -> list[StreamMetadata]:
        async with _asyncio.Lock(): # Simple global lock for metadata listing
            streams = []
            for sid in self._streams.keys():
                meta = await self.get_stream_metadata(sid)
                if meta: streams.append(meta)
            return sorted(streams, key=lambda x: x.stream_id)

    async def archive_stream(self, stream_id: str, expected_version: int) -> None:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, 0)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)
            
            if self._archived.get(stream_id):
                return
            
            self._archived[stream_id] = _datetime.utcnow()

    async def verify_stream_integrity(self, stream_id: str) -> bool:
        events = self._streams.get(stream_id, [])
        prev_hash = None
        for e in sorted(events, key=lambda x: x["stream_position"]):
            if e["previous_hash"] != prev_hash:
                return False
            calc = calculate_event_hash(
                prev_hash, e["stream_id"], e["stream_position"],
                e["event_type"], e["event_version"], e["payload"], e["metadata"]
            )
            if e["integrity_hash"] != calc:
                return False
            prev_hash = calc
        return True

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
