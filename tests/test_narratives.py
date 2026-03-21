"""
tests/test_narratives.py
========================
The 5 narrative scenario tests. These are the primary correctness gate.
"""
import pytest
import asyncio
import asyncpg
import os
from uuid import uuid4

from ledger.event_store import EventStore
from ledger.registry.client import ApplicantRegistryClient
from ledger.agents.document_processing_agent import DocumentProcessingAgent
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from ledger.agents.fraud_detection_agent import FraudDetectionAgent

@pytest.fixture
async def store():
    url = os.environ.get("TEST_DB_URL", "postgresql://postgres:123post@localhost:5432/the_ledger")
    s = EventStore(url)
    await s.connect()
    yield s
    await s.close()

@pytest.fixture
async def registry():
    url = os.environ.get("TEST_DB_URL", "postgresql://postgres:123post@localhost:5432/the_ledger")
    pool = await asyncpg.create_pool(url)
    client = ApplicantRegistryClient(pool)
    yield client
    await pool.close()

@pytest.mark.asyncio
async def test_narr01_concurrent_occ_collision(store, registry):
    # NARR-01: Concurrent agents attempting to finalize credit analysis.
    # The system must ensure exactly ONE CreditAnalysisCompleted event is written.
    app_id = f"NARR-01-{uuid4().hex[:8]}"
    
    # 1. Seed ApplicationSubmitted
    await store.append(f"loan-{app_id}", [{
        "event_type": "ApplicationSubmitted",
        "payload": {"application_id": app_id, "applicant_id": "COMP-TEST", "requested_amount_usd": 100000}
    }], expected_version=-1)
    
    # 2. Extract Document package
    doc_agent = DocumentProcessingAgent("doc_agent_1", store)
    await doc_agent.process_application(app_id)
    
    # 3. Two concurrent Credit Agents
    agent1 = CreditAnalysisAgent("credit_1", store, registry)
    agent2 = CreditAnalysisAgent("credit_2", store, registry)
    
    await asyncio.gather(
        agent1.process_application(app_id),
        agent2.process_application(app_id),
        return_exceptions=True
    )
    
    # Verify exact constraints
    events = await store.load_stream(f"credit-{app_id}")
    completions = [e for e in events if e["event_type"] == "CreditAnalysisCompleted"]
    assert len(completions) == 1, f"Expected exactly 1 complete event despite concurrent execution, found {len(completions)}"


@pytest.mark.asyncio
async def test_narr02_document_extraction_failure(store, registry):
    # NARR-02: EBITDA field is missing from documents.
    # DocumentProcessingAgent must flag quality exception.
    app_id = f"NARR-02-{uuid4().hex[:8]}"
    
    await store.append(f"loan-{app_id}", [{
        "event_type": "ApplicationSubmitted",
        "payload": {"application_id": app_id, "applicant_id": "COMP-TEST", "requested_amount_usd": 100000}
    }], expected_version=-1)
    
    doc_agent = DocumentProcessingAgent("doc_agent_1", store)
    await doc_agent.process_application(app_id)
    
    pkg_events = await store.load_stream(f"docpkg-{app_id}")
    flags = [e for e in pkg_events if e["event_type"] == "DocumentQualityFlagged"]
    assert len(flags) > 0, "Missing DocumentQualityFlagged"
    assert "ebitda" in flags[0]["payload"]["critical_missing_fields"]
    
    credit_agent = CreditAnalysisAgent("credit_1", store, registry)
    await credit_agent.process_application(app_id)
    
    credit_events = await store.load_stream(f"credit-{app_id}")
    comps = [e for e in credit_events if e["event_type"] == "CreditAnalysisCompleted"]
    assert len(comps) == 1
    assert comps[0]["payload"]["decision"]["confidence"] == 0.75
    assert "ebitda" in comps[0]["payload"]["decision"]["data_quality_caveats"]


@pytest.mark.asyncio
async def test_narr03_agent_crash_recovery(store, registry):
    # NARR-03: Agent crashes mid-run. Restart recovers session.
    app_id = f"NARR-03-{uuid4().hex[:8]}"
    
    await store.append(f"loan-{app_id}", [{
        "event_type": "ApplicationSubmitted",
        "payload": {"application_id": app_id, "applicant_id": "COMP-TEST"}
    }], expected_version=-1)
    await store.append(f"credit-{app_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {"decision": {"confidence": 0.9}}
    }], expected_version=-1)
    await store.append(f"docpkg-{app_id}", [{
        "event_type": "ExtractionCompleted",
        "payload": {"facts": {"total_revenue": 500}}
    }], expected_version=-1)
    
    # 1. Start agent and crash it intentionally
    agent1 = FraudDetectionAgent("fraud_1", store, registry)
    
    # Mock node to crash
    original = agent1._node_analyze
    async def crash_node(state):
        raise ValueError("Simulated infrastructure crash!")
    agent1._node_analyze = crash_node
    
    with pytest.raises(ValueError):
        await agent1.process_application(app_id)
        
    # Check that session started but never output
    stream_events = await store.load_stream(f"fraud-{app_id}")
    assert not any(e["event_type"] == "FraudScreeningCompleted" for e in stream_events)
    
    agent_stream = await store.load_stream(f"agent-fraud_1-{app_id}")
    starts = [e for e in agent_stream if e["event_type"] == "AgentSessionStarted"]
    assert len(starts) == 1
    session_1 = starts[0]["payload"]["session_id"]
    
    # 2. Recover with new agent
    agent2 = FraudDetectionAgent("fraud_1", store, registry)
    await agent2.process_application(app_id)
    
    agent_stream_2 = await store.load_stream(f"agent-fraud_1-{app_id}")
    starts_2 = [e for e in agent_stream_2 if e["event_type"] == "AgentSessionStarted"]
    assert len(starts_2) == 2
    
    # Validate the replay context string
    ctx2 = starts_2[1]["payload"]["context_source"]
    assert ctx2 == f"prior_session_replay: {session_1}"

@pytest.mark.asyncio
async def test_narr04_compliance_hard_block():
    pytest.skip("Implement after ComplianceAgent is working")

@pytest.mark.asyncio
async def test_narr05_human_override():
    pytest.skip("Implement after all agents + HumanReviewCompleted")
