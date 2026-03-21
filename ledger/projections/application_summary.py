import json

class ApplicationSummaryProjector:
    name = "application_summary"
    
    # In a real app, this projector would update a SQL table (e.g., `application_summary_view`)
    # For this exercise, we can demonstrate the DDL generation and update logic
    
    async def setup(self, conn):
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS application_summary_view (
                application_id TEXT PRIMARY KEY,
                applicant_id TEXT,
                state TEXT,
                requested_amount_usd NUMERIC,
                decision TEXT,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    async def handle_event(self, event: dict, conn):
        et = event["event_type"]
        p = event["payload"]
        app_id = p.get("application_id")
        if not app_id: return
        
        if et == "ApplicationSubmitted":
            await conn.execute("""
                INSERT INTO application_summary_view (application_id, applicant_id, state, requested_amount_usd)
                VALUES ($1, $2, 'SUBMITTED', $3)
                ON CONFLICT (application_id) DO NOTHING
            """, app_id, p.get("applicant_id"), float(p.get("requested_amount_usd", 0)))
            
        elif et in [
            "DocumentUploadRequested", "DocumentUploaded", "CreditAnalysisRequested", 
            "FraudScreeningRequested", "ComplianceCheckRequested"
        ]:
            # For simplicity, map to generic IN_PROGRESS
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'IN_PROGRESS', updated_at = NOW() 
                WHERE application_id = $1
            """, app_id)
            
        elif et == "DecisionGenerated":
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'PENDING_DECISION', decision = $2, updated_at = NOW() 
                WHERE application_id = $1
            """, app_id, p.get("recommendation", "UNKNOWN"))
            
        elif et == "ApplicationApproved":
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'APPROVED', updated_at = NOW() 
                WHERE application_id = $1
            """, app_id)

        elif et == "ApplicationDeclined":
            await conn.execute("""
                UPDATE application_summary_view 
                SET state = 'DECLINED', updated_at = NOW() 
                WHERE application_id = $1
            """, app_id)
