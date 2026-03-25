import asyncio
import time
import pytest
from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError

@pytest.mark.asyncio
async def test_high_load_slo_concurrent_appends():
    """
    SLO Test: 50 concurrent commands appending to various streams.
    Ensures no deadlocks, pool exhaustion (mocked here), or data loss.
    """
    store = InMemoryEventStore()
    num_commands = 50
    num_streams = 10
    
    async def run_command(idx):
        stream_id = f"loan-slo-{idx % num_streams}"
        # Each command does a load -> decide -> append cycle
        for _ in range(3): # Each "user" does 3 sequential actions
            try:
                version = await store.stream_version(stream_id)
                event = {
                    "event_type": "CommandExecuted",
                    "payload": {"cmd_idx": idx, "ts": time.time()}
                }
                await store.append(stream_id, [event], expected_version=version)
                break
            except OptimisticConcurrencyError:
                # Concurrent conflict expected on small number of streams
                await asyncio.sleep(0.01) # Small backoff
                continue

    start_time = time.time()
    # Run 50 commands concurrently
    await asyncio.gather(*(run_command(i) for i in range(num_commands)))
    end_time = time.time()
    
    duration = end_time - start_time
    print(f"\nSLO Test: 50 commands on {num_streams} streams took {duration:.4f}s")
    
    # Assertions
    total_events = 0
    for i in range(num_streams):
        events = await store.load_stream(f"loan-slo-{i}")
        total_events += len(events)
        
    assert total_events == num_commands
    assert duration < 2.0  # Should be very fast for in-memory
    
@pytest.mark.asyncio
async def test_high_load_occ_stress():
    """
    Stress test OCC specifically with many tasks competing for the SAME stream.
    """
    store = InMemoryEventStore()
    stream_id = "contest-stream"
    num_competitors = 20
    
    # Initialize stream
    await store.append(stream_id, [{"event_type": "Init", "payload": {}}], expected_version=-1)
    
    success_count = 0
    error_count = 0
    
    async def compete():
        nonlocal success_count, error_count
        try:
            # All try to append to version 0 (the version after Init)
            await store.append(stream_id, [{"event_type": "Win", "payload": {}}], expected_version=0)
            success_count += 1
        except OptimisticConcurrencyError:
            error_count += 1
            
    await asyncio.gather(*(compete() for _ in range(num_competitors)))
    
    assert success_count == 1
    assert error_count == num_competitors - 1
    
    events = await store.load_stream(stream_id)
    assert len(events) == 2 # Init + one winner
