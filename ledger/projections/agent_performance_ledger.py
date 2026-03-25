import json
from ledger.schema.events import StoredEvent

class AgentPerformanceLedgerProjector:
    name = "agent_performance_ledger"

    async def setup(self, conn):
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_performance_session_lookup (
                session_id TEXT PRIMARY KEY,
                agent_id TEXT,
                model_version TEXT,
                application_id TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_performance_view (
                agent_id TEXT,
                model_version TEXT,
                analyses_completed INT DEFAULT 0,
                decisions_generated INT DEFAULT 0,
                avg_confidence_score NUMERIC DEFAULT 0,
                avg_duration_ms NUMERIC DEFAULT 0,
                approve_rate NUMERIC DEFAULT 0,
                decline_rate NUMERIC DEFAULT 0,
                refer_rate NUMERIC DEFAULT 0,
                human_overrides INT DEFAULT 0,
                first_seen_at TIMESTAMPTZ,
                last_seen_at TIMESTAMPTZ,
                total_confidence_score NUMERIC DEFAULT 0,
                total_duration_ms NUMERIC DEFAULT 0,
                total_approves INT DEFAULT 0,
                total_declines INT DEFAULT 0,
                total_refers INT DEFAULT 0,
                PRIMARY KEY (agent_id, model_version)
            )
        """)

    async def _ensure_agent(self, conn, agent_id, model_version, seen_at):
        await conn.execute("""
            INSERT INTO agent_performance_view (agent_id, model_version, first_seen_at, last_seen_at)
            VALUES ($1, $2, $3, $3)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET last_seen_at = excluded.last_seen_at
        """, agent_id, model_version, seen_at)

    async def handle_event(self, event: StoredEvent, conn):
        et = event.event_type
        p = event.payload
        
        if et == "AgentSessionStarted":
            sess = p.get("session_id")
            aid = p.get("agent_id", "unknown")
            mv = p.get("model_version", "unknown")
            app_id = p.get("application_id")
            if sess:
                await conn.execute("""
                    INSERT INTO agent_performance_session_lookup (session_id, agent_id, model_version, application_id)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING
                """, sess, aid, mv, app_id)
                await self._ensure_agent(conn, aid, mv, event.recorded_at)

        elif et == "CreditAnalysisCompleted":
            sess = p.get("session_id")
            if sess:
                row = await conn.fetchrow("SELECT agent_id, model_version FROM agent_performance_session_lookup WHERE session_id = $1", sess)
                if row:
                    aid, mv = row["agent_id"], row["model_version"]
                    conf = p.get("decision", {}).get("confidence", 0)
                    dur = p.get("analysis_duration_ms", 0)
                    await conn.execute("""
                        UPDATE agent_performance_view
                        SET analyses_completed = analyses_completed + 1,
                            total_confidence_score = total_confidence_score + $3,
                            total_duration_ms = total_duration_ms + $4,
                            avg_confidence_score = (total_confidence_score + $3) / (analyses_completed + 1),
                            avg_duration_ms = (total_duration_ms + $4) / (analyses_completed + 1),
                            last_seen_at = $5
                        WHERE agent_id = $1 AND model_version = $2
                    """, aid, mv, float(conf), int(dur), event.recorded_at)

        elif et == "DecisionGenerated":
            sess = p.get("orchestrator_session_id")
            if sess:
                row = await conn.fetchrow("SELECT agent_id, model_version FROM agent_performance_session_lookup WHERE session_id = $1", sess)
                if row:
                    aid, mv = row["agent_id"], row["model_version"]
                    rec = p.get("recommendation", "")
                    conf = float(p.get("confidence", 0))
                    
                    is_app = 1 if rec == "APPROVE" else 0
                    is_dec = 1 if rec == "DECLINE" else 0
                    is_ref = 1 if rec == "REFER" else 0
                    
                    await conn.execute("""
                        UPDATE agent_performance_view
                        SET decisions_generated = decisions_generated + 1,
                            total_confidence_score = total_confidence_score + $3,
                            avg_confidence_score = (total_confidence_score + $3) / (decisions_generated + 1),
                            total_approves = total_approves + $4,
                            total_declines = total_declines + $5,
                            total_refers = total_refers + $6,
                            approve_rate = (total_approves + $4)::NUMERIC / (decisions_generated + 1),
                            decline_rate = (total_declines + $5)::NUMERIC / (decisions_generated + 1),
                            refer_rate = (total_refers + $6)::NUMERIC / (decisions_generated + 1),
                            last_seen_at = $7
                        WHERE agent_id = $1 AND model_version = $2
                    """, aid, mv, conf, is_app, is_dec, is_ref, event.recorded_at)

        elif et == "HumanReviewCompleted":
            # Map back to the decision generator agent. 
            app_id = p.get("application_id")
            override = p.get("override", False)
            if app_id and override:
                # Find the most recent decision generated for this application to attribute the override
                # Since we don't have direct linkage to session in HumanReviewCompleted, we use the `decision_event_id` 
                # Wait, does the payload have `decision_event_id`? 
                # `HumanReviewRequested` has it. Let's just lookup by app_id in lookup table ? No, multiple sessions might exist.
                # In SQL we can find the most recent orchestrator session for this app.
                row = await conn.fetchrow("""
                    SELECT agent_id, model_version FROM agent_performance_session_lookup
                    WHERE application_id = $1 AND agent_id LIKE '%orchestrator%'
                    ORDER BY session_id DESC LIMIT 1
                """, app_id)
                if not row:
                    # Generic fallback if agent_id wasn't orchestrator explicitly
                    row = await conn.fetchrow("""
                        SELECT agent_id, model_version FROM agent_performance_session_lookup
                        WHERE application_id = $1
                        ORDER BY session_id DESC LIMIT 1
                    """, app_id)
                if row:
                    await conn.execute("""
                        UPDATE agent_performance_view
                        SET human_overrides = human_overrides + 1,
                            human_override_rate = (human_overrides + 1)::NUMERIC / GREATEST(decisions_generated, 1)
                        WHERE agent_id = $1 AND model_version = $2
                    """, row["agent_id"], row["model_version"])
