import pytest
import asyncio
import os
from uuid import uuid4
from ledger.event_store import EventStore
from ledger.registry.client import ApplicantRegistryClient
from ledger.agents.fraud_detection_agent import FraudDetectionAgent
from ledger.agents.compliance_agent import ComplianceAgent
from ledger.agents.decision_orchestrator_agent import DecisionOrchestratorAgent

@pytest.fixture
async def store():
    url = os.environ.get("TEST_DB_URL", "postgresql://postgres:123post@localhost:5432/the_ledger")
    s = EventStore(url)
    await s.connect()
    yield s
    await s.close()

@pytest.fixture
async def registry(store):
    return ApplicantRegistryClient(store._pool)

@pytest.mark.asyncio
async def test_fraud_agent_logic(store, registry):
    agent = FraudDetectionAgent("fraud_1", store, registry)
    app_id = f"unit-fraud-{uuid4().hex[:8]}"
    
    # Run agent
    await agent.process_application(app_id)
    
    # Verify events in fraud stream
    events = await store.load_stream(f"fraud-{app_id}")
    types = [e["event_type"] for e in events]
    assert "FraudScreeningInitiated" in types
    assert "FraudScreeningCompleted" in types
    
    # Verify trigger in loan stream
    loan_events = await store.load_stream(f"loan-{app_id}")
    assert any(e["event_type"] == "ComplianceRuleRequested" or e["event_type"] == "ComplianceCheckRequested" for e in loan_events)

@pytest.mark.asyncio
async def test_compliance_agent_rules(store, registry):
    agent = ComplianceAgent("compliance_1", store, registry)
    app_id = f"unit-compliance-{uuid4().hex[:8]}"
    
    # Run agent
    await agent.process_application(app_id)
    
    # Verify rules were evaluated
    events = await store.load_stream(f"compliance-{app_id}")
    passed = [e for e in events if e["event_type"] == "ComplianceRulePassed"]
    noted = [e for e in events if e["event_type"] == "ComplianceRuleNoted"]
    assert len(passed) == 5
    assert len(noted) == 1
    
    # Verify completion
    assert any(e["event_type"] == "ComplianceCheckCompleted" for e in events)

@pytest.mark.asyncio
async def test_decision_orchestrator(store):
    agent = DecisionOrchestratorAgent("orch_1", store)
    app_id = f"unit-orch-{uuid4().hex[:8]}"
    
    # 1. Seed some prerequisite events (simplified)
    await store.append(f"credit-{app_id}", [{"event_type": "CreditAnalysisCompleted", "payload": {"application_id": app_id, "decision": {"risk_tier": "LOW", "recommended_limit_usd": 50000}}}], expected_version=-1)
    
    # 2. Run orchestrator
    await agent.process_application(app_id)
    
    # 3. Verify final decision
    events = await store.load_stream(f"loan-{app_id}")
    assert any(e["event_type"] == "DecisionGenerated" for e in events)
