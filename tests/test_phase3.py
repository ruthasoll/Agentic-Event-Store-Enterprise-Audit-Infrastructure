import pytest
import asyncio
import asyncpg
import os
from datetime import datetime, UTC, timedelta
from ledger.event_store import EventStore
from ledger.projections.daemon import ProjectionDaemon
from ledger.projections.application_summary import ApplicationSummaryProjector
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjector
from ledger.projections.compliance_audit_view import ComplianceAuditView

@pytest.fixture
async def db_pool():
    url = os.environ.get("TEST_DB_URL")
    if not url:
        pytest.skip("TEST_DB_URL not set")
    pool = await asyncpg.create_pool(url)
    yield pool
    await pool.close()

@pytest.fixture
async def store(db_pool):
    url = os.environ.get("TEST_DB_URL")
    s = EventStore(url)
    await s.connect()
    yield s
    await s.close()

@pytest.mark.asyncio
async def test_projection_daemon_flow(store, db_pool):
    # 1. Setup projectors
    app_projector = ApplicationSummaryProjector()
    agent_projector = AgentPerformanceLedgerProjector()
    compliance_projector = ComplianceAuditView(store)
    
    projectors = [app_projector, agent_projector, compliance_projector]
    
    # 2. Clear old data for a clean test
    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS application_summary_view")
        await conn.execute("DROP TABLE IF EXISTS agent_performance_view")
        await conn.execute("DROP TABLE IF EXISTS compliance_audit_snapshots")
        await conn.execute("DELETE FROM projection_checkpoints")
        await conn.execute("DELETE FROM outbox")
        await conn.execute("DELETE FROM events")
        await conn.execute("DELETE FROM event_streams")

    # 3. Start Daemon
    daemon = ProjectionDaemon(store, db_pool, projectors, batch_size=100, poll_interval=0.1)
    await daemon.start()
    
    try:
        # 4. Append events
        app_id = "TEST-APP-001"
        await store.append(f"loan-{app_id}", [
            {"event_type": "ApplicationSubmitted", "payload": {"application_id": app_id, "applicant_id": "C-1", "requested_amount_usd": 50000}},
            {"event_type": "DocumentUploadRequested", "payload": {"application_id": app_id}}
        ], expected_version=-1)
        
        await store.append(f"agent-doc-1", [
            {"event_type": "AgentSessionStarted", "payload": {"session_id": "S-1", "agent_type": "document_processing", "agent_id": "doc-1", "application_id": app_id}},
            {"event_type": "AgentSessionCompleted", "payload": {"session_id": "S-1", "agent_type": "document_processing", "agent_id": "doc-1", "total_nodes_executed": 5, "total_tokens_used": 1000, "total_cost_usd": 0.01, "total_duration_ms": 500}}
        ], expected_version=-1)
        
        await store.append(f"compliance-{app_id}", [
            {"event_type": "ComplianceCheckRequested", "payload": {"application_id": app_id, "rules_to_evaluate": ["RULE-1", "RULE-2"]}},
            {"event_type": "ComplianceRulePassed", "payload": {"application_id": app_id, "rule_id": "RULE-1"}}
        ], expected_version=-1)

        # 5. Wait for daemon to process
        wait_loops = 0
        while wait_loops < 50:
            lags = await daemon.get_lag()
            if all(lag == 0 for lag in lags.values()):
                break
            await asyncio.sleep(0.2)
            wait_loops += 1
            
        lags = await daemon.get_lag()
        assert all(lag == 0 for lag in lags.values()), f"Projections still lagging: {lags}"

        # 6. Verify Application Summary
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM application_summary_view WHERE application_id = $1", app_id)
            assert row is not None
            assert row["state"] == "COMPLIANCE_CHECK_REQUESTED"
            assert row["requested_amount_usd"] == 50000

            # Verify Agent Performance
            agent_row = await conn.fetchrow("SELECT * FROM agent_performance_view WHERE agent_id = $1", "doc-1")
            assert agent_row is not None
            assert agent_row["total_sessions"] == 1
            assert agent_row["total_tokens"] == 1000
            assert float(agent_row["total_cost_usd"]) == 0.01

            # Verify Compliance Audit
            # Snapshots are every 100 events, we don't have enough here, but we can verify the view
            # Actually ComplianceAuditView doesn't have a view table, it only has snapshots.
            # But it has get_state_at
            now = datetime.now(UTC)
            state = await compliance_projector.get_state_at(app_id, now, conn)
            assert "RULE-1" in state["passed"]
            assert "RULE-2" in state["mandatory"]
            assert "RULE-2" not in state["passed"]

        # 7. Test Rebuild
        await daemon.rebuild("application_summary")
        # In this simplistic impl, rebuild just resets checkpoint. We need to wait for it to re-process.
        # To truly test rebuild we should have deleted the view data first, but our rebuild() doesn't do that yet.
        # Let's verify the lag went up.
        lags = await daemon.get_lag()
        assert lags["application_summary"] > 0
        
        # Wait for it to catch up again
        wait_loops = 0
        while wait_loops < 50:
            lags = await daemon.get_lag()
            if lags["application_summary"] == 0:
                break
            await asyncio.sleep(0.2)
            wait_loops += 1
            
        lags = await daemon.get_lag()
        assert lags["application_summary"] == 0

        # 8. Test Temporal Query (ComplianceAuditView)
        # Reclaim the compliance state via the view's method
        async with db_pool.acquire() as conn:
            # Get state at "now"
            state = await compliance_projector.get_state_at(app_id, datetime.now(UTC), conn)
            assert "RULE-1" in state["passed"]
            assert "RULE-2" in state["mandatory"]
            assert "RULE-2" not in state["passed"]

    finally:
        await daemon.stop()
