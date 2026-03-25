import pytest
import asyncio
from ledger.event_store import InMemoryEventStore, get_default_upcaster_registry
from ledger.mcp_server import inspect_loan_history, run_integrity_check, get_compliance_summary
import ledger.mcp_server

@pytest.mark.asyncio
async def test_mcp_tool_logic():
    # Setup InMemoryStore for tools
    store = InMemoryEventStore(upcaster_registry=get_default_upcaster_registry())
    
    # Mock the get_store function in mcp_server
    async def mock_get_store():
        return store
    
    # Patch the global _store and get_store function
    ledger.mcp_server.get_store = mock_get_store
    
    # 1. Seed some data
    app_id = "APP-1"
    stream_id = f"loan-{app_id}"
    await store.append(stream_id, [
        {"event_type": "ApplicationSubmitted", "payload": {"application_id": app_id}}
    ], expected_version=0)
    
    # 2. Test inspect_loan_history
    history = await inspect_loan_history(app_id)
    assert len(history) == 1
    assert history[0]["event_type"] == "ApplicationSubmitted"
    
    # 3. Test run_integrity_check
    verify = await run_integrity_check(stream_id)
    assert verify["integrity_check_passed"] is True
    assert verify["stream_id"] == stream_id
    
    # 4. Test get_compliance_summary
    comp_stream = f"compliance-{app_id}"
    await store.append(comp_stream, [
        {"event_type": "ComplianceRulePassed", "payload": {"rule_name": "KYC"}},
        {"event_type": "ComplianceCheckCompleted", "payload": {"overall_verdict": "CLEAR"}}
    ], expected_version=0)
    
    summary = await get_compliance_summary(app_id)
    assert summary["verdict"] == "CLEAR"
    assert "KYC" in summary["passed_rules"]
