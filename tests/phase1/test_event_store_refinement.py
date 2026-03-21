import pytest
import asyncio
from uuid import uuid4
from datetime import datetime
from ledger.event_store import InMemoryEventStore, EventStore, OptimisticConcurrencyError
from ledger.schema.events import StoredEvent, StreamMetadata

@pytest.mark.asyncio
async def test_in_memory_event_store_refinements():
    store = InMemoryEventStore()
    stream_id = "test-stream-1"
    
    # 1. Test append and load_stream returns StoredEvent
    events = [{"event_type": "TestEvent", "payload": {"foo": "bar"}}]
    await store.append(stream_id, events, 0)
    
    loaded = await store.load_stream(stream_id)
    assert len(loaded) == 1
    assert isinstance(loaded[0], StoredEvent)
    assert loaded[0].payload == {"foo": "bar"}
    assert loaded[0].stream_position == 1
    
    # 2. Test get_stream_metadata
    meta = await store.get_stream_metadata(stream_id)
    assert isinstance(meta, StreamMetadata)
    assert meta.stream_id == stream_id
    assert meta.current_version == 1
    assert meta.archived_at is None
    
    # 3. Test archive_stream
    await store.archive_stream(stream_id, 1)
    meta_after = await store.get_stream_metadata(stream_id)
    assert meta_after.archived_at is not None
    
    # 4. Test append to archived stream fails
    with pytest.raises(ValueError, match="is archived"):
        await store.append(stream_id, events, 1)
        
    # 5. Test archive_stream OCC
    with pytest.raises(OptimisticConcurrencyError):
        await store.archive_stream(stream_id, 0) # Wrong version

@pytest.mark.asyncio
async def test_real_event_store_refinements(db_url):
    # This test requires a running Postgres with schema applied.
    # It will skip if db connection fails.
    try:
        store = EventStore(db_url)
        await store.connect()
    except Exception:
        pytest.skip("Database not available for real EventStore test")
        return

    try:
        stream_id = f"refine-{uuid4()}"
        
        # 1. Test append and load_stream returns StoredEvent
        events = [{"event_type": "TestEvent", "payload": {"foo": "bar"}}]
        await store.append(stream_id, events, -1)
        
        loaded = await store.load_stream(stream_id)
        assert len(loaded) == 1
        assert isinstance(loaded[0], StoredEvent)
        assert loaded[0].payload == {"foo": "bar"}
        
        # 2. Test get_stream_metadata
        meta = await store.get_stream_metadata(stream_id)
        assert meta.stream_id == stream_id
        assert meta.current_version == 1
        assert meta.archived_at is None
        
        # 3. Test archive_stream
        await store.archive_stream(stream_id, 1)
        meta_after = await store.get_stream_metadata(stream_id)
        assert meta_after.archived_at is not None
        
        # 4. Test append to archived stream fails
        with pytest.raises(ValueError, match="is archived"):
            await store.append(stream_id, events, 1)
            
    finally:
        await store.close()
