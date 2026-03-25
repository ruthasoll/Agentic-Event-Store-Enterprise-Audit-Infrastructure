import pytest
import asyncio
import os
import asyncpg
from datetime import datetime, timezone
from uuid import uuid4

from ledger.event_store import EventStore
from ledger.projections.daemon import ProjectionDaemon
from ledger.projections.application_summary import ApplicationSummaryProjector
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjector
from ledger.projections.compliance_audit_view import ComplianceAuditView

@pytest.fixture
async def db_pool():
    url = os.environ.get("TEST_DB_URL", "postgresql://neondb_owner:npg_AFNcwjr8W3gp@ep-gentle-dust-amq8uo8h-pooler.c-5.us-east-1.aws.neon.tech/ledger?sslmode=require&channel_binding=require")  
    pool = await asyncpg.create_pool(url)
    yield pool
    await pool.close()

@pytest.fixture
async def store():
    url = os.environ.get("TEST_DB_URL", "postgresql://postgres:123post@localhost:5432/apex_ledger")
    s = EventStore(url)
    await s.connect()
    yield s
    await s.close()

@pytest.mark.asyncio
async def test_projection_lag_slo(store, db_pool):
    """Test lag metric under concurrent updates."""
    app_proj = ApplicationSummaryProjector()
    agent_proj = AgentPerformanceLedgerProjector()
    comp_proj = ComplianceAuditView(store)
    
    daemon = ProjectionDaemon(store, db_pool, [app_proj, agent_proj, comp_proj], poll_interval=0.1)
    await daemon.start()
    
    # Simulate 50 concurrent command handlers writing events
    async def write_events():
        app_id = f"APP-{uuid4().hex[:6]}"
        await store.append(f"loan-{app_id}", [{"event_type": "ApplicationSubmitted", "payload": {"application_id": app_id}}], -1)
        
    await asyncio.gather(*(write_events() for _ in range(50)))
    
    await asyncio.sleep(0.5) # allow daemon to catch up
    lags = await daemon.get_lag()
    
    await daemon.stop()
    
    assert all(lag == 0 for lag in lags.values()), f"Projections should be caught up, lags: {lags}"

@pytest.mark.asyncio
async def test_rebuild_from_scratch(store, db_pool):
    comp_proj = ComplianceAuditView(store)
    await comp_proj.setup(db_pool)
    
    app_id = f"APP-REBUILD-{uuid4().hex[:6]}"
    await store.append(f"compliance-{app_id}", [
        {"event_type": "ComplianceCheckInitiated", "payload": {"application_id": app_id, "rules_to_evaluate": ["R1"]}},
        {"event_type": "ComplianceRulePassed", "payload": {"application_id": app_id, "rule_id": "R1"}},
    ], -1)
    
    async with db_pool.acquire() as conn:
        await comp_proj.rebuild_from_scratch(conn)
        state = await comp_proj.get_state_at(app_id, datetime.now(timezone.utc), conn)
        
    assert "R1" in state["passed"]
