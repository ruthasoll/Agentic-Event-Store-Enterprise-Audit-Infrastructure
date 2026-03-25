import pytest
import asyncio
from ledger.event_store import InMemoryEventStore, get_default_upcaster_registry
from ledger.schema.events import StoredEvent

@pytest.mark.asyncio
async def test_upcasting_decision_generated():
    registry = get_default_upcaster_registry()
    store = InMemoryEventStore(upcaster_registry=registry)
    
    # Append a v1 DecisionGenerated event manually (skipping normal append to simulate old data)
    stream_id = "loan-123"
    v1_payload = {
        "application_id": "123",
        "orchestrator_session_id": "S1",
        "recommendation": "APPROVE",
        "confidence": 0.9,
        "executive_summary": "Looks good",
        "generated_at": "2026-01-01T00:00:00Z"
    }
    
    # We use a lower-level append or just mock the stored event
    # To truly test UpcasterRegistry.upcast in load_stream, we need it in the store.
    await store.append(stream_id, [
        {"event_type": "DecisionGenerated", "event_version": 1, "payload": v1_payload}
    ], expected_version=0)
    
    # Load the stream
    events = await store.load_stream(stream_id)
    assert len(events) == 1
    event = events[0]
    
    # Verify upcasted to v2
    assert event.event_version == 2
    assert "model_versions" in event.payload
    assert event.payload["model_versions"] == {}

@pytest.mark.asyncio
async def test_upcasting_credit_analysis_completed():
    registry = get_default_upcaster_registry()
    store = InMemoryEventStore(upcaster_registry=registry)
    
    stream_id = "credit-123"
    v1_payload = {
        "application_id": "123",
        "session_id": "S1",
        "decision": {"risk_tier": "LOW", "recommended_limit_usd": 10000, "confidence": 0.9, "rationale": "ok"},
        "model_version": "v1",
        "model_deployment_id": "d1",
        "input_data_hash": "h1",
        "analysis_duration_ms": 100,
        "completed_at": "2026-01-01T00:00:00Z"
    }
    
    await store.append(stream_id, [
        {"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": v1_payload}
    ], expected_version=0)
    
    events = await store.load_stream(stream_id)
    assert len(events) == 1
    event = events[0]
    
    assert event.event_version == 2
    assert "regulatory_basis" in event.payload
    assert event.payload["regulatory_basis"] == []

@pytest.mark.asyncio
async def test_integrity_chain_valid():
    store = InMemoryEventStore()
    stream_id = "loan-456"
    
    await store.append(stream_id, [
        {"event_type": "ApplicationSubmitted", "payload": {"id": 1}},
        {"event_type": "DocumentUploadRequested", "payload": {"id": 2}}
    ], expected_version=0)
    
    # Verify integrity
    is_valid = await store.verify_stream_integrity(stream_id)
    assert is_valid is True
    
    # Verify hashes exist
    events = store._streams[stream_id]
    assert events[0]["previous_hash"] is None
    assert events[0]["integrity_hash"] is not None
    assert events[1]["previous_hash"] == events[0]["integrity_hash"]
    assert events[1]["integrity_hash"] is not None

@pytest.mark.asyncio
async def test_integrity_chain_tamper_detection():
    store = InMemoryEventStore()
    stream_id = "loan-789"
    
    await store.append(stream_id, [
        {"event_type": "ApplicationSubmitted", "payload": {"amount": 1000}}
    ], expected_version=0)
    
    # Verify initially valid
    assert await store.verify_stream_integrity(stream_id) is True
    
    # Tamper with the payload in the internal storage
    store._streams[stream_id][0]["payload"]["amount"] = 999999 # Evil hacker!
    
    # Verify integrity check fails
    assert await store.verify_stream_integrity(stream_id) is False

@pytest.mark.asyncio
async def test_integrity_chain_wrong_previous_hash():
    store = InMemoryEventStore()
    stream_id = "loan-abc"
    
    await store.append(stream_id, [
        {"event_type": "Event1", "payload": {}},
        {"event_type": "Event2", "payload": {}}
    ], expected_version=0)
    
    # Tamper with previous_hash
    store._streams[stream_id][1]["previous_hash"] = "wrong-hash"
    
    assert await store.verify_stream_integrity(stream_id) is False
