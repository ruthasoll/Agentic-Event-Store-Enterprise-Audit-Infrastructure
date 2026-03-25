import json
from ledger.schema.events import StoredEvent

class ApplicationSummaryProjector:
    name = "application_summary"
    
    async def setup(self, conn):
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS application_summary_view (
                application_id TEXT PRIMARY KEY,
                applicant_id TEXT,
                state TEXT,
                requested_amount_usd NUMERIC,
                approved_amount_usd NUMERIC,
                risk_tier TEXT,
                fraud_score NUMERIC,
                compliance_status TEXT,
                decision TEXT,
                agent_sessions_completed TEXT[],
                last_event_type TEXT,
                last_event_at TIMESTAMPTZ,
                human_reviewer_id TEXT,
                final_decision_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    async def handle_event(self, event: StoredEvent, conn):
        et = event.event_type
        p = event.payload
        app_id = p.get("application_id")
        if not app_id: return
        
        if et == "ApplicationSubmitted":
            await conn.execute("""
                INSERT INTO application_summary_view (
                    application_id, applicant_id, state, requested_amount_usd, last_event_type, last_event_at
                ) VALUES ($1, $2, 'SUBMITTED', $3, $4, $5)
                ON CONFLICT (application_id) DO UPDATE SET
                    state = 'SUBMITTED',
                    last_event_type = $4,
                    last_event_at = $5,
                    updated_at = NOW()
            """, app_id, p.get("applicant_id"), float(p.get("requested_amount_usd", 0) or 0), et, event.recorded_at)
            
        elif et in ["CreditAnalysisRequested"]:
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'AWAITING_ANALYSIS', last_event_type = $2, last_event_at = $3, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, et, event.recorded_at)

        elif et == "CreditAnalysisCompleted":
            decision = p.get("decision", {})
            risk = decision.get("risk_tier") if isinstance(decision, dict) else None
            await conn.execute("""
                UPDATE application_summary_view 
                SET risk_tier = $2, state = 'ANALYSIS_COMPLETE', last_event_type = $3, last_event_at = $4, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, risk, et, event.recorded_at)

        elif et == "FraudScreeningCompleted":
            score = float(p.get("fraud_score", 0))
            await conn.execute("""
                UPDATE application_summary_view 
                SET fraud_score = $2, last_event_type = $3, last_event_at = $4, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, score, et, event.recorded_at)
            
        elif et == "ComplianceCheckRequested":
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'COMPLIANCE_REVIEW', last_event_type = $2, last_event_at = $3, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, et, event.recorded_at)

        elif et == "ComplianceCheckCompleted":
            status = p.get("overall_verdict")
            await conn.execute("""
                UPDATE application_summary_view 
                SET compliance_status = $2, last_event_type = $3, last_event_at = $4, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, status, et, event.recorded_at)

        elif et == "DecisionGenerated":
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'PENDING_DECISION', decision = $2, last_event_type = $3, last_event_at = $4, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, p.get("recommendation", "UNKNOWN"), et, event.recorded_at)
            
        elif et == "HumanReviewRequested":
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'PENDING_HUMAN_REVIEW', last_event_type = $2, last_event_at = $3, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, et, event.recorded_at)

        elif et == "HumanReviewCompleted":
            reviewer = p.get("reviewer_id")
            await conn.execute("""
                UPDATE application_summary_view 
                SET human_reviewer_id = $2, last_event_type = $3, last_event_at = $4, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, reviewer, et, event.recorded_at)
            
        elif et == "ApplicationApproved":
            amount = float(p.get("approved_amount_usd", 0) or 0)
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'FINAL_APPROVED', approved_amount_usd = $2, final_decision_at = $3, last_event_type = $4, last_event_at = $3, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, amount, event.recorded_at, et)

        elif et == "ApplicationDeclined":
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'FINAL_DECLINED', final_decision_at = $2, last_event_type = $3, last_event_at = $2, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, event.recorded_at, et)

        elif et == "AgentSessionCompleted":
            sess = p.get("session_id")
            if sess:
                await conn.execute("""
                    UPDATE application_summary_view 
                    SET agent_sessions_completed = array_append(agent_sessions_completed, $2), last_event_type = $3, last_event_at = $4, updated_at = NOW()
                    WHERE application_id = $1 AND NOT ($2 = ANY(COALESCE(agent_sessions_completed, ARRAY[]::TEXT[])))
                """, app_id, sess, et, event.recorded_at)
