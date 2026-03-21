import json
from fastmcp import FastMCP
from pydantic import ValidationError

from ledger.commands.handlers import (
    handle_submit_application, SubmitApplicationCmd,
    handle_credit_analysis_completed, CreditAnalysisCompletedCmd,
    handle_fraud_screening_completed, FraudScreeningCompletedCmd,
    handle_compliance_check, ComplianceCheckCmd,
    handle_generate_decision, GenerateDecisionCmd
)
from ledger.event_store import OptimisticConcurrencyError
from ledger.schema.events import DomainError

def format_error(e: Exception) -> str:
    if isinstance(e, OptimisticConcurrencyError):
        return json.dumps({
            "status": "error",
            "error_type": "OptimisticConcurrencyError",
            "message": f"Conflict on stream {e.stream_id}. Expected {e.expected}, actual {e.actual}.",
            "action_required": "Fetch the latest state resource and rethink your action."
        })
    elif isinstance(e, DomainError):
        return json.dumps({
            "status": "error",
            "error_type": "DomainError",
            "message": str(e),
            "action_required": "Correct your command constraints based on the rule violations documented above."
        })
    elif isinstance(e, ValidationError):
        return json.dumps({
            "status": "error",
            "error_type": "ValidationError",
            "message": str(e)
        })
    return json.dumps({"status": "error", "error_type": "SystemError", "message": str(e)})

def register_tools(mcp: FastMCP, store):

    @mcp.tool()
    async def submit_application(
        correlation_id: str,
        application_id: str,
        applicant_id: str,
        requested_amount_usd: float,
        loan_purpose: str,
        submission_channel: str,
        contact_name: str,
        contact_email: str
    ) -> str:
        """Submit a new loan application to initialize the aggregate stream."""
        try:
            cmd = SubmitApplicationCmd(
                correlation_id=correlation_id, application_id=application_id, applicant_id=applicant_id,
                requested_amount_usd=requested_amount_usd, loan_purpose=loan_purpose,
                submission_channel=submission_channel, contact_name=contact_name, contact_email=contact_email
            )
            pos = await handle_submit_application(cmd, store)
            return json.dumps({"status": "success", "stream_position": pos})
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    async def record_credit_analysis(
        correlation_id: str,
        application_id: str,
        agent_id: str,
        session_id: str,
        model_version: str,
        confidence_score: float,
        risk_tier: str,
        recommended_limit_usd: float,
        duration_ms: int,
        input_data: dict,
        causation_id: str = None
    ) -> str:
        """Agentic recording of credit analysis execution."""
        try:
            cmd = CreditAnalysisCompletedCmd(
                correlation_id=correlation_id, causation_id=causation_id, application_id=application_id, 
                agent_id=agent_id, session_id=session_id, model_version=model_version,
                confidence_score=confidence_score, risk_tier=risk_tier, 
                recommended_limit_usd=recommended_limit_usd, duration_ms=duration_ms, input_data=input_data
            )
            await handle_credit_analysis_completed(cmd, store)
            return json.dumps({"status": "success", "message": "Credit analysis logged successfully. Await further orchestrator bounds."})
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    async def record_fraud_screening(
        correlation_id: str,
        application_id: str,
        agent_id: str,
        session_id: str,
        screening_model_version: str,
        fraud_score: float,
        recommendation: str,
        anomalies_found: int,
        input_data: dict,
        causation_id: str = None
    ) -> str:
        """Agentic recording of a fraud screening model execution."""
        try:
            cmd = FraudScreeningCompletedCmd(
                correlation_id=correlation_id, causation_id=causation_id, application_id=application_id,
                agent_id=agent_id, session_id=session_id, screening_model_version=screening_model_version,
                fraud_score=fraud_score, recommendation=recommendation, anomalies_found=anomalies_found, input_data=input_data
            )
            await handle_fraud_screening_completed(cmd, store)
            return json.dumps({"status": "success"})
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    async def record_compliance_check(
        correlation_id: str,
        application_id: str,
        session_id: str,
        rule_id: str,
        rule_name: str,
        rule_version: str,
        passed: bool,
        evidence: dict,
        failure_reason: str = None,
        causation_id: str = None
    ) -> str:
        """Agentic tool to record outcomes of regulatory checks."""
        try:
            cmd = ComplianceCheckCmd(
                correlation_id=correlation_id, causation_id=causation_id, application_id=application_id,
                session_id=session_id, rule_id=rule_id, rule_name=rule_name, rule_version=rule_version,
                passed=passed, evidence=evidence, failure_reason=failure_reason
            )
            await handle_compliance_check(cmd, store)
            return json.dumps({"status": "success"})
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    async def search_applications(
        applicant_id: str = None,
        status: str = None,
        limit: int = 20
    ) -> str:
        """Search for loan applications by applicant ID or current state."""
        try:
            async with store._pool.acquire() as conn:
                where = []
                args = []
                if applicant_id:
                    where.append(f"applicant_id = ${len(args)+1}")
                    args.append(applicant_id)
                if status:
                    where.append(f"state = ${len(args)+1}")
                    args.append(status)
                
                query = "SELECT * FROM application_summary_view"
                if where:
                    query += " WHERE " + " AND ".join(where)
                query += f" ORDER BY updated_at DESC LIMIT {limit}"
                
                rows = await conn.fetch(query, *args)
                return json.dumps([dict(r) for r in rows], default=str)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    async def generate_decision(
        correlation_id: str,
        application_id: str,
        orchestrator_agent_id: str,
        session_id: str,
        recommendation: str,
        confidence_score: float,
        executive_summary: str,
        contributing_agent_sessions: list[str],
        model_versions: dict,
        causation_id: str = None
    ) -> str:
        """Orchestrator generates final automated decision over all checks."""
        try:
            cmd = GenerateDecisionCmd(
                correlation_id=correlation_id, causation_id=causation_id, application_id=application_id,
                orchestrator_agent_id=orchestrator_agent_id, session_id=session_id, recommendation=recommendation,
                confidence_score=confidence_score, executive_summary=executive_summary, 
                contributing_agent_sessions=contributing_agent_sessions, model_versions=model_versions
            )
            await handle_generate_decision(cmd, store)
            return json.dumps({"status": "success"})
        except Exception as e:
            return format_error(e)
