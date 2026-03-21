import json

class AgentPerformanceLedgerProjector:
    name = "agent_performance_ledger"

    async def setup(self, conn):
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_performance_view (
                agent_id TEXT PRIMARY KEY,
                total_tasks INT DEFAULT 0,
                total_analysis_time_ms BIGINT DEFAULT 0,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    async def handle_event(self, event: dict, conn):
        et = event["event_type"]
        p = event.get("payload", {})
        
        agent_id = p.get("agent_id")
        
        # Some events use different fields for agents
        if not agent_id and et == "DecisionGenerated":
            # The orchestrator might be derivable from orchestrator_session_id
            agent_id = p.get("orchestrator_session_id", "").split("-")[0] # Mock split

        if not agent_id:
            return
            
        dur = int(p.get("analysis_duration_ms", 0)) if "analysis_duration_ms" in p else 0
        
        await conn.execute("""
            INSERT INTO agent_performance_view (agent_id, total_tasks, total_analysis_time_ms)
            VALUES ($1, 1, $2)
            ON CONFLICT (agent_id) DO UPDATE SET 
                total_tasks = agent_performance_view.total_tasks + 1,
                total_analysis_time_ms = agent_performance_view.total_analysis_time_ms + excluded.total_analysis_time_ms,
                updated_at = NOW()
        """, agent_id, dur)
