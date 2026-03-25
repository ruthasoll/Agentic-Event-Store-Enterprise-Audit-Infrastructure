import json

class AgentPerformanceLedgerProjector:
    name = "agent_performance"
    
    async def setup(self, conn):
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_performance_view (
                agent_id TEXT PRIMARY KEY,
                agent_type TEXT,
                total_sessions INT DEFAULT 0,
                total_nodes_executed INT DEFAULT 0,
                total_llm_calls INT DEFAULT 0,
                total_tokens INT DEFAULT 0,
                total_cost_usd NUMERIC(15,6) DEFAULT 0,
                avg_latency_ms NUMERIC(15,2) DEFAULT 0,
                last_active_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    async def handle_event(self, event: dict, conn):
        et = event["event_type"]
        p = event.get("payload", {})
        
        # Agent events usually have session_id and agent_type. agent_id might be in metadata or p.
        agent_id = p.get("agent_id") or event.get("metadata", {}).get("agent_id")
        agent_type = p.get("agent_type")
        
        if not agent_id:
            # Fallback for session-based events where agent_id might be session_id
            agent_id = p.get("session_id")
            
        if not agent_id:
            return

        recorded_at = event.get("recorded_at")

        if et == "AgentSessionStarted":
            await conn.execute("""
                INSERT INTO agent_performance_view (agent_id, agent_type, total_sessions, last_active_at)
                VALUES ($1, $2, 1, $3)
                ON CONFLICT (agent_id) DO UPDATE SET
                    total_sessions = agent_performance_view.total_sessions + 1,
                    last_active_at = excluded.last_active_at,
                    updated_at = NOW()
            """, agent_id, agent_type, recorded_at)

        elif et == "AgentSessionCompleted":
            nodes = int(p.get("total_nodes_executed", 0))
            llm_calls = int(p.get("total_llm_calls", 0))
            tokens = int(p.get("total_tokens_used", 0))
            cost = float(p.get("total_cost_usd", 0.0))
            duration = float(p.get("total_duration_ms", 0.0))
            
            await conn.execute("""
                UPDATE agent_performance_view SET
                    total_nodes_executed = total_nodes_executed + $2,
                    total_llm_calls = total_llm_calls + $3,
                    total_tokens = total_tokens + $4,
                    total_cost_usd = total_cost_usd + $5,
                    avg_latency_ms = CASE 
                        WHEN total_sessions > 0 THEN (avg_latency_ms * (total_sessions - 1) + $6) / total_sessions 
                        ELSE $6 END,
                    last_active_at = $7,
                    updated_at = NOW()
                WHERE agent_id = $1
            """, agent_id, nodes, llm_calls, tokens, cost, duration, recorded_at)

        elif et == "AgentSessionFailed":
            await conn.execute("""
                UPDATE agent_performance_view SET
                last_active_at = $2,
                updated_at = NOW()
                WHERE agent_id = $1
            """, agent_id, recorded_at)

