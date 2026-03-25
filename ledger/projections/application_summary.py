import json
from datetime import datetime

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
                agent_sessions_completed JSONB DEFAULT '[]'::jsonb,
                last_event_type TEXT,
                last_event_at TIMESTAMPTZ,
                human_reviewer_id TEXT,
                final_decision_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    async def handle_event(self, event: dict, conn):
        et = event["event_type"]
        p = event.get("payload", {})
        
        # Some events use different paths to application_id, but the main ones have it direct.
        app_id = p.get("application_id")
        if not app_id: 
            return
            
        recorded_at = event.get("recorded_at")
        if isinstance(recorded_at, str):
            recorded_at = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))

        # Basic upsert for all events to maintain `last_event_type` and `last_event_at`
        await conn.execute("""
            INSERT INTO application_summary_view (application_id, last_event_type, last_event_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (application_id) DO UPDATE SET
                last_event_type = excluded.last_event_type,
                last_event_at = excluded.last_event_at,
                updated_at = NOW()
        """, app_id, et, recorded_at)
        
        # State transitions based on event
        if et == "ApplicationSubmitted":
            req_amount = float(p.get("requested_amount_usd", 0)) if p.get("requested_amount_usd") else None
            await conn.execute("""
                UPDATE application_summary_view SET
                applicant_id = $2, state = 'SUBMITTED', requested_amount_usd = $3
                WHERE application_id = $1
            """, app_id, p.get("applicant_id"), req_amount)
            
        elif et == "DocumentUploadRequested":
            await conn.execute("UPDATE application_summary_view SET state = 'DOCUMENTS_PENDING' WHERE application_id = $1", app_id)
        elif et == "DocumentUploaded":
            await conn.execute("UPDATE application_summary_view SET state = 'DOCUMENTS_UPLOADED' WHERE application_id = $1", app_id)
        elif et == "PackageReadyForAnalysis":
            await conn.execute("UPDATE application_summary_view SET state = 'DOCUMENTS_PROCESSED' WHERE application_id = $1", app_id)
        elif et == "CreditAnalysisRequested":
            await conn.execute("UPDATE application_summary_view SET state = 'CREDIT_ANALYSIS_REQUESTED' WHERE application_id = $1", app_id)
        elif et == "CreditAnalysisCompleted":
            decision = p.get("decision", {})
            risk_tier = decision.get("risk_tier")
            await conn.execute("UPDATE application_summary_view SET state = 'CREDIT_ANALYSIS_COMPLETE', risk_tier = $2 WHERE application_id = $1", app_id, risk_tier)
        elif et == "FraudScreeningRequested":
            await conn.execute("UPDATE application_summary_view SET state = 'FRAUD_SCREENING_REQUESTED' WHERE application_id = $1", app_id)
        elif et == "FraudScreeningCompleted":
            fraud_score = float(p.get("fraud_score", 0)) if p.get("fraud_score") is not None else None
            await conn.execute("UPDATE application_summary_view SET state = 'FRAUD_SCREENING_COMPLETE', fraud_score = $2 WHERE application_id = $1", app_id, fraud_score)
        elif et == "ComplianceCheckRequested":
            await conn.execute("UPDATE application_summary_view SET state = 'COMPLIANCE_CHECK_REQUESTED' WHERE application_id = $1", app_id)
        elif et == "ComplianceCheckCompleted":
            verdict = p.get("overall_verdict")
            await conn.execute("UPDATE application_summary_view SET state = 'COMPLIANCE_CHECK_COMPLETE', compliance_status = $2 WHERE application_id = $1", app_id, verdict)
        elif et == "DecisionRequested":
            await conn.execute("UPDATE application_summary_view SET state = 'PENDING_DECISION' WHERE application_id = $1", app_id)
        elif et == "DecisionGenerated":
            rec = p.get("recommendation", "UNKNOWN")
            await conn.execute("UPDATE application_summary_view SET decision = $2 WHERE application_id = $1", app_id, rec)
            if rec == "REFER":
                await conn.execute("UPDATE application_summary_view SET state = 'REFERRED' WHERE application_id = $1", app_id)
        elif et == "HumanReviewRequested":
            await conn.execute("UPDATE application_summary_view SET state = 'PENDING_HUMAN_REVIEW' WHERE application_id = $1", app_id)
        elif et == "HumanReviewCompleted":
            reviewer = p.get("reviewer_id")
            await conn.execute("UPDATE application_summary_view SET human_reviewer_id = $2 WHERE application_id = $1", app_id, reviewer)
        elif et == "ApplicationApproved":
            amt_str = p.get("approved_amount_usd")
            amt = float(amt_str) if amt_str is not None else None
            dt_str = p.get("approved_at")
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00")) if dt_str else None
            await conn.execute("UPDATE application_summary_view SET state = 'APPROVED', approved_amount_usd = $2, final_decision_at = $3 WHERE application_id = $1", app_id, amt, dt)
        elif et == "ApplicationDeclined":
            dt_str = p.get("declined_at")
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00")) if dt_str else None
            reasons = p.get("decline_reasons", [])
            state = 'DECLINED_COMPLIANCE' if any("REG-" in r for r in reasons) else 'DECLINED'
            await conn.execute("UPDATE application_summary_view SET state = $2, final_decision_at = $3 WHERE application_id = $1", app_id, state, dt)

        # Handle agent sessions completed
        if et == "AgentSessionCompleted":
            session_id = p.get("session_id")
            agent_type = p.get("agent_type")
            if session_id:
                new_item = json.dumps([{"session_id": session_id, "agent_type": agent_type}])
                await conn.execute("""
                    UPDATE application_summary_view 
                    SET agent_sessions_completed = COALESCE(agent_sessions_completed, '[]'::jsonb) || $2::jsonb 
                    WHERE application_id = $1
                """, app_id, new_item)

