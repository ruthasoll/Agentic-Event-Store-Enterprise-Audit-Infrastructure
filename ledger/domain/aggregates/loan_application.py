from __future__ import annotations
from dataclasses import dataclass, field
from ledger.schema.events import ApplicationState, DomainError

VALID_TRANSITIONS = {
    ApplicationState.SUBMITTED: [ApplicationState.DOCUMENTS_PENDING, ApplicationState.CREDIT_ANALYSIS_REQUESTED],
    ApplicationState.DOCUMENTS_PENDING: [ApplicationState.DOCUMENTS_UPLOADED],
    ApplicationState.DOCUMENTS_UPLOADED: [ApplicationState.DOCUMENTS_PROCESSED],
    ApplicationState.DOCUMENTS_PROCESSED: [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: [ApplicationState.CREDIT_ANALYSIS_COMPLETE],
    ApplicationState.CREDIT_ANALYSIS_COMPLETE: [ApplicationState.FRAUD_SCREENING_REQUESTED, ApplicationState.COMPLIANCE_CHECK_REQUESTED, ApplicationState.PENDING_DECISION],
    ApplicationState.FRAUD_SCREENING_REQUESTED: [ApplicationState.FRAUD_SCREENING_COMPLETE],
    ApplicationState.FRAUD_SCREENING_COMPLETE: [ApplicationState.COMPLIANCE_CHECK_REQUESTED, ApplicationState.PENDING_DECISION],
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_COMPLETE],
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: [ApplicationState.PENDING_DECISION, ApplicationState.DECLINED_COMPLIANCE],
    ApplicationState.PENDING_DECISION: [ApplicationState.APPROVED, ApplicationState.DECLINED, ApplicationState.PENDING_HUMAN_REVIEW, ApplicationState.REFERRED],
    ApplicationState.PENDING_HUMAN_REVIEW: [ApplicationState.APPROVED, ApplicationState.DECLINED],
}

@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState | None = None
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    version: int = 0
    
    # Domain Rule State Trackers
    has_credit_analysis: bool = False
    has_human_override: bool = False
    compliance_checks_completed: bool = False

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        events = await store.load_stream(f"loan-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        agg.version = len(events)
        return agg

    def _apply(self, event: dict) -> None:
        """Apply one event to update aggregate state."""
        et = event.get("event_type")
        p = event.get("payload", {})
        
        handler = getattr(self, f"_on_{et}", None)
        if handler:
            handler(p)

    def _on_ApplicationSubmitted(self, p: dict) -> None:
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = p.get("applicant_id")
        self.requested_amount_usd = float(p.get("requested_amount_usd", 0))
        self.loan_purpose = p.get("loan_purpose")

    def _on_DocumentUploadRequested(self, p: dict) -> None:
        self.state = ApplicationState.DOCUMENTS_PENDING

    def _on_DocumentUploaded(self, p: dict) -> None:
        self.state = ApplicationState.DOCUMENTS_UPLOADED

    def _on_CreditAnalysisRequested(self, p: dict) -> None:
        self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED

    def _on_CreditAnalysisCompleted(self, p: dict) -> None:
        self.state = ApplicationState.CREDIT_ANALYSIS_COMPLETE
        self.has_credit_analysis = True
        self.has_human_override = False  # resets override if a new analysis comes AFTER an override

    def _on_FraudScreeningRequested(self, p: dict) -> None:
        self.state = ApplicationState.FRAUD_SCREENING_REQUESTED

    def _on_FraudScreeningCompleted(self, p: dict) -> None:
        self.state = ApplicationState.FRAUD_SCREENING_COMPLETE

    def _on_ComplianceCheckRequested(self, p: dict) -> None:
        self.state = ApplicationState.COMPLIANCE_CHECK_REQUESTED

    def _on_ComplianceCheckCompleted(self, p: dict) -> None:
        self.state = ApplicationState.COMPLIANCE_CHECK_COMPLETE

    def _on_DecisionRequested(self, p: dict) -> None:
        self.state = ApplicationState.PENDING_DECISION

    def _on_DecisionGenerated(self, p: dict) -> None:
        # DecisionGenerated does not immediately transition state to APPROVED or DECLINED without human
        # Or it might if automated. But we track it here.
        rec = p.get("recommendation")
        if rec == "REFER":
            self.state = ApplicationState.PENDING_HUMAN_REVIEW

    def _on_HumanReviewCompleted(self, p: dict) -> None:
        if p.get("override", False):
            self.has_human_override = True

    def _on_ApplicationApproved(self, p: dict) -> None:
        self.state = ApplicationState.APPROVED

    def _on_ApplicationDeclined(self, p: dict) -> None:
        self.state = ApplicationState.DECLINED

    def assert_valid_transition(self, target: ApplicationState) -> None:
        if self.state is None:
            # Allows initial transition
            return
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed and target != self.state:
            raise DomainError(f"Invalid transition {self.state} → {target}. Allowed: {allowed}")

    def assert_can_receive_credit_analysis(self) -> None:
        if self.has_credit_analysis and not self.has_human_override:
            raise DomainError("Model version locking: Cannot append another CreditAnalysisCompleted unless superseded by HumanReviewOverride.")

    def assert_confidence_floor(self, confidence_score: float, recommendation: str) -> None:
        if confidence_score < 0.6 and recommendation != "REFER":
            raise DomainError("Confidence floor: Recommendation must be REFER when confidence < 0.6")

    def assert_compliance_passed(self, compliance_record) -> None:
        # compliance_record is an instance of ComplianceRecordAggregate
        if not compliance_record.is_cleared():
            raise DomainError("Compliance dependency: Cannot approve application without mandatory compliance clearance.")

    def assert_causal_chains_valid(self, agent_sessions: list) -> None:
        # Check that the orchestrator references actual agent sessions that processed this specific application
        for session in agent_sessions:
            if session.application_id != self.application_id:
                raise DomainError(f"Causal chain enforcement: Referenced session {session.session_id} did not process this application {self.application_id}.")
            if not session.has_made_decision:
                raise DomainError(f"Causal chain enforcement: Referenced session {session.session_id} does not contain a decision event.")
