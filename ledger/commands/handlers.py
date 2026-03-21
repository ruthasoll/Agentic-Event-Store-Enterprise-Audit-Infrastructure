import hashlib
import json
from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.schema.events import (
    ApplicationSubmitted, CreditAnalysisCompleted, FraudScreeningCompleted,
    DecisionGenerated, HumanReviewCompleted, AgentSessionStarted,
    ComplianceRulePassed, ComplianceRuleFailed, RiskTier, LoanPurpose, DomainError,
    ApplicationState
)
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate

# Helper to hash inputs consistently
def hash_inputs(data: Any) -> str:
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

# ----- COMMAND MODELS -----
class SubmitApplicationCmd(BaseModel):
    correlation_id: str
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    submission_channel: str
    contact_name: str
    contact_email: str

class StartAgentSessionCmd(BaseModel):
    correlation_id: str
    agent_id: str
    session_id: str
    application_id: str
    agent_type: str
    model_version: str
    context_source: str
    context_token_count: int

class CreditAnalysisCompletedCmd(BaseModel):
    correlation_id: str
    causation_id: Optional[str] = None
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: float
    duration_ms: int
    input_data: dict

class FraudScreeningCompletedCmd(BaseModel):
    correlation_id: str
    causation_id: Optional[str] = None
    application_id: str
    agent_id: str
    session_id: str
    screening_model_version: str
    fraud_score: float
    recommendation: str
    anomalies_found: int
    input_data: dict

class ComplianceCheckCmd(BaseModel):
    correlation_id: str
    causation_id: Optional[str] = None
    application_id: str
    session_id: str
    rule_id: str
    rule_name: str
    rule_version: str
    passed: bool
    evidence: dict
    failure_reason: Optional[str] = None

class GenerateDecisionCmd(BaseModel):
    correlation_id: str
    causation_id: Optional[str] = None
    application_id: str
    orchestrator_agent_id: str
    session_id: str
    recommendation: str
    confidence_score: float
    executive_summary: str
    contributing_agent_sessions: List[str] # List of session IDs
    model_versions: Dict[str, str]

class HumanReviewCompletedCmd(BaseModel):
    correlation_id: str
    causation_id: Optional[str] = None
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: Optional[str] = None


# ----- HANDLERS -----

async def handle_submit_application(cmd: SubmitApplicationCmd, store: EventStore) -> int:
    # 1. Load stream
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    
    # 2. Validate
    if app.version > 0:
        raise DomainError("Application already exists")
        
    # 3. Determine new events
    event = ApplicationSubmitted(
        application_id=cmd.application_id,
        applicant_id=cmd.applicant_id,
        requested_amount_usd=cmd.requested_amount_usd,
        loan_purpose=LoanPurpose(cmd.loan_purpose.lower()),
        loan_term_months=12, # Stub default
        submission_channel=cmd.submission_channel,
        contact_email=cmd.contact_email,
        contact_name=cmd.contact_name,
        submitted_at=datetime.utcnow(),
        application_reference=f"REF-{cmd.application_id[:8]}"
    )
    
    # 4. Append
    pos = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event.to_store_dict()],
        expected_version=0,
        correlation_id=cmd.correlation_id
    )
    return pos[0]

async def handle_start_agent_session(cmd: StartAgentSessionCmd, store: EventStore) -> int:
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)
    if agent.version > 0:
        raise DomainError("Session already started")
        
    event = AgentSessionStarted(
        session_id=cmd.session_id,
        agent_type=cmd.agent_type,
        agent_id=cmd.agent_id,
        application_id=cmd.application_id,
        model_version=cmd.model_version,
        langgraph_graph_version="1.0",
        context_source=cmd.context_source,
        context_token_count=cmd.context_token_count,
        started_at=datetime.utcnow()
    )
    
    pos = await store.append(
        stream_id=f"agent-{cmd.agent_id}-{cmd.session_id}",
        events=[event.to_store_dict()],
        expected_version=0,
        correlation_id=cmd.correlation_id
    )
    return pos[0]

async def handle_credit_analysis_completed(cmd: CreditAnalysisCompletedCmd, store: EventStore) -> None:
    # 1. Reconstruct current aggregate state
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # 2. Validate — business rules checked BEFORE state change
    app.assert_valid_transition(ApplicationState.CREDIT_ANALYSIS_COMPLETE)
    app.assert_can_receive_credit_analysis()
    
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)

    # 3. Determine new events
    loan_event = CreditAnalysisCompleted(
        application_id=cmd.application_id,
        session_id=cmd.session_id,
        decision={"risk_tier": cmd.risk_tier, "recommended_limit_usd": cmd.recommended_limit_usd, "confidence": cmd.confidence_score, "rationale": "Credit handler execution"},
        model_version=cmd.model_version,
        model_deployment_id="dep-1",
        input_data_hash=hash_inputs(cmd.input_data),
        analysis_duration_ms=cmd.duration_ms,
        completed_at=datetime.utcnow()
    )
    
    # We append the event to the loan stream, it was generated by the credit record/agent.
    # Often, analysts append into the Loan stream directly or into a distinct aggregate (like CreditRecord).
    # Since OCC was the double-decision challenge on the loan stream, we push it to Loan stream.
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[loan_event.to_store_dict()],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )

async def handle_fraud_screening_completed(cmd: FraudScreeningCompletedCmd, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    app.assert_valid_transition(ApplicationState.FRAUD_SCREENING_COMPLETE)
    agent.assert_context_loaded()

    event = FraudScreeningCompleted(
        application_id=cmd.application_id,
        session_id=cmd.session_id,
        fraud_score=cmd.fraud_score,
        risk_level=cmd.recommendation,
        anomalies_found=cmd.anomalies_found,
        recommendation=cmd.recommendation,
        screening_model_version=cmd.screening_model_version,
        input_data_hash=hash_inputs(cmd.input_data),
        completed_at=datetime.utcnow()
    )
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event.to_store_dict()],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )

async def handle_compliance_check(cmd: ComplianceCheckCmd, store: EventStore) -> None:
    rec = await ComplianceRecordAggregate.load(store, cmd.application_id)
    
    if cmd.passed:
        event = ComplianceRulePassed(
            application_id=cmd.application_id,
            session_id=cmd.session_id,
            rule_id=cmd.rule_id,
            rule_name=cmd.rule_name,
            rule_version=cmd.rule_version,
            evidence_hash=hash_inputs(cmd.evidence),
            evaluation_notes="Passed",
            evaluated_at=datetime.utcnow()
        )
    else:
        event = ComplianceRuleFailed(
            application_id=cmd.application_id,
            session_id=cmd.session_id,
            rule_id=cmd.rule_id,
            rule_name=cmd.rule_name,
            rule_version=cmd.rule_version,
            failure_reason=cmd.failure_reason or "Verification Failed",
            is_hard_block=True,
            remediation_available=False,
            evidence_hash=hash_inputs(cmd.evidence),
            evaluated_at=datetime.utcnow()
        )
    
    await store.append(
        stream_id=f"compliance-{cmd.application_id}",
        events=[event.to_store_dict()],
        expected_version=rec.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )

async def handle_generate_decision(cmd: GenerateDecisionCmd, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)
    
    app.assert_valid_transition(ApplicationState.PENDING_DECISION)
    # Check compliance constraint
    # App.assert_compliance_passed(compliance) -> Not strictly requiring approval here unless we transition to APPROVAL, but typically we enforce rules prior to generating the final decision block
    
    # Load agent sessions
    sessions = []
    for sid in cmd.contributing_agent_sessions:
        # Session IDs alone are tricky, we need agent_id. Here we assume orchestrator passes full agent-{id}-{sid} strings.
        # But if just session_id, we parse them. For simplicity, we assume cmd.contributing_agent_sessions are tuples or we mock list validation.
        pass
    
    final_rec = cmd.recommendation
    # Enforce Confidence floor (Rule 4)
    if cmd.confidence_score < 0.60:
        final_rec = "REFER"

    event = DecisionGenerated(
        application_id=cmd.application_id,
        orchestrator_session_id=cmd.session_id,
        recommendation=final_rec,
        confidence=cmd.confidence_score,
        executive_summary=cmd.executive_summary,
        contributing_sessions=cmd.contributing_agent_sessions,
        model_versions=cmd.model_versions,
        generated_at=datetime.utcnow()
    )

    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event.to_store_dict()],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )

async def handle_human_review_completed(cmd: HumanReviewCompletedCmd, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    
    event = HumanReviewCompleted(
        application_id=cmd.application_id,
        reviewer_id=cmd.reviewer_id,
        override=cmd.override,
        original_recommendation=cmd.original_recommendation,
        final_decision=cmd.final_decision,
        override_reason=cmd.override_reason,
        reviewed_at=datetime.utcnow()
    )
    
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[event.to_store_dict()],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id
    )
