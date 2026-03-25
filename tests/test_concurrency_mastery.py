import pytest
import asyncio
from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError

@pytest.mark.asyncio
async def test_optimistic_concurrency_race_condition():
    """
    Test that two concurrent appends to the same stream version 
    correctly result in one winner and one OptimisticConcurrencyError.
    """
    store = InMemoryEventStore()
    stream_id = "test-race-stream"
    
    # 1. Initialize stream
    await store.append(stream_id, [{"event_type": "Started", "payload": {}}], expected_version=-1)
    
    # 2. Prepare two concurrent tasks targeting version 0
    event1 = [{"event_type": "UpdateA", "payload": {"val": 1}}]
    event2 = [{"event_type": "UpdateB", "payload": {"val": 2}}]
    
    async def task_a():
        try:
            await store.append(stream_id, event1, expected_version=0)
            return "SUCCESS"
        except OptimisticConcurrencyError:
            return "OCC_ERROR"
            
    async def task_b():
        try:
            await store.append(stream_id, event2, expected_version=0)
            return "SUCCESS"
        except OptimisticConcurrencyError:
            return "OCC_ERROR"
            
    results = await asyncio.gather(task_a(), task_b())
    
    # 3. Assertions
    assert "SUCCESS" in results
    assert "OCC_ERROR" in results
    
    # 4. Verify stream length and final version
    events = await store.load_stream(stream_id)
    assert len(events) == 2  # Started + Winner
    
    version = await store.stream_version(stream_id)
    assert version == 1
    
    # Verify the winner's data is actually there
    winner_type = "UpdateA" if results[0] == "SUCCESS" else "UpdateB"
    assert events[1].event_type == winner_type
