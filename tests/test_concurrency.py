import pytest
import asyncio
from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError

@pytest.fixture
def store():
    """Use the InMemoryEventStore which safely mocks database OCC via asyncio Locks."""
    return InMemoryEventStore()

@pytest.mark.asyncio
async def test_double_decision_concurrency(store):
    """
    Two AI agents simultaneously attempt to append a CreditAnalysisCompleted event
    to the same loan application stream. Both read the stream at version 3 and pass
    expected_version=3. One succeeds (stream_position=4), one raises OptimisticConcurrencyError.
    """
    stream_id = "loan-test-123"
    
    # 1. Setup: Stream needs 3 events to reach version 3.
    # We simulate an existing stream by appending 3 initial events.
    # The InMemoryEventStore initializes at 0 (after our fix) so appending 3 events sets version to 3.
    initial_events = [
        {"event_type": "ApplicationSubmitted", "payload": {"amount": 50000}},
        {"event_type": "DocumentUploadRequested", "payload": {}},
        {"event_type": "DocumentUploaded", "payload": {}},
    ]
    await store.append(stream_id, initial_events, expected_version=0)
    
    # Verify stream is exactly at version 3 before concurrent appends
    assert await store.stream_version(stream_id) == 3
    
    event_a = {"event_type": "CreditAnalysisCompleted", "payload": {"agent": "A", "confidence": 0.9}}
    event_b = {"event_type": "CreditAnalysisCompleted", "payload": {"agent": "B", "confidence": 0.8}}
    
    results = []
    
    async def agent_task(event_payload):
        try:
            # Yield control back to event loop to maximize concurrency overlap
            await asyncio.sleep(0.01)
            # Both agents expect version 3
            positions = await store.append(stream_id, [event_payload], expected_version=3)
            results.append(("SUCCESS", positions[0]))
        except OptimisticConcurrencyError as e:
            results.append(("OCC_ERROR", e))

    # 2. Spawn concurrent tasks
    await asyncio.gather(
        asyncio.create_task(agent_task(event_a)),
        asyncio.create_task(agent_task(event_b)),
    )
    
    # 3. Assertions
    successes = [r for r in results if r[0] == "SUCCESS"]
    errors = [r for r in results if r[0] == "OCC_ERROR"]
    
    # (c) The losing task's OptimisticConcurrencyError is raised, not silently swallowed.
    assert len(successes) == 1, "Exactly one agent should succeed."
    assert len(errors) == 1, "Exactly one agent should encounter an OCC error."
    error_obj = errors[0][1]
    assert isinstance(error_obj, OptimisticConcurrencyError)
    assert error_obj.expected == 3
    assert error_obj.actual == 4
    
    # (b) The winning task's event has stream_position=4
    winning_pos = successes[0][1]
    assert winning_pos == 4
    
    # (a) Total events appended to the stream = 4 (not 5)
    all_events = await store.load_stream(stream_id)
    assert len(all_events) == 4
    assert await store.stream_version(stream_id) == 4
