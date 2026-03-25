from ledger.schema.events import (
    ApplicationState, DomainError, ApplicationSubmitted, DocumentUploadRequested,
    CreditAnalysisCompleted, ApplicationApproved, ApplicationDeclined, BaseEvent
)

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
            # event is a StoredEvent, we want the dict for _apply or adapt it
            if hasattr(event, "model_dump"):
                event_dict = event.model_dump(mode='json')
            else:
                event_dict = event
            agg._apply(event_dict)
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

    # --- COMMANDS (Decisions) ---

    def submit(self, applicant_id: str, amount_usd: float, purpose: str, **kwargs) -> ApplicationSubmitted:
        """Command: Submit a new loan application."""
        if self.state is not None:
            raise DomainError(f"Application {self.application_id} already exists in state {self.state}")
        
        from datetime import datetime, timezone
        event = ApplicationSubmitted(
            application_id=self.application_id,
            applicant_id=applicant_id,
            requested_amount_usd=amount_usd,
            loan_purpose=purpose,
            submitted_at=datetime.now(timezone.utc),
            **kwargs
        )
        return event

    def request_documents(self, required_types: list, **kwargs) -> DocumentUploadRequested:
        """Command: Move to DOCUMENTS_PENDING."""
        self.assert_valid_transition(ApplicationState.DOCUMENTS_PENDING)
        from datetime import datetime, timezone
        return DocumentUploadRequested(
            application_id=self.application_id,
            required_document_types=required_types,
            requested_at=datetime.now(timezone.utc),
            **kwargs
        )

    def complete_analysis(self, decision_data: dict, **kwargs) -> CreditAnalysisCompleted:
        """Command: Complete credit analysis with version locking check."""
        self.assert_valid_transition(ApplicationState.CREDIT_ANALYSIS_COMPLETE)
        self.assert_can_receive_credit_analysis()
        
        from datetime import datetime, timezone
        return CreditAnalysisCompleted(
            application_id=self.application_id,
            decision=decision_data,
            completed_at=datetime.now(timezone.utc),
            **kwargs
        )

    def approve(self, approved_amount: float, interest_rate: float, term_months: int, approved_by: str, compliance_record=None) -> ApplicationApproved:
        """Command: Final approval with compliance check."""
        self.assert_valid_transition(ApplicationState.APPROVED)
        
        if compliance_record:
            self.assert_compliance_passed(compliance_record)
            
        from datetime import datetime, timezone
        return ApplicationApproved(
            application_id=self.application_id,
            approved_amount_usd=approved_amount,
            interest_rate_pct=interest_rate,
            term_months=term_months,
            approved_by=approved_by,
            approved_at=datetime.now(timezone.utc),
            effective_date=datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )

    def decline(self, reasons: list, declined_by: str) -> ApplicationDeclined:
        """Command: Decline application."""
        self.assert_valid_transition(ApplicationState.DECLINED)
        from datetime import datetime, timezone
        return ApplicationDeclined(
            application_id=self.application_id,
            decline_reasons=reasons,
            declined_by=declined_by,
            adverse_action_notice_required=True,
            declined_at=datetime.now(timezone.utc)
        )

    # --- VALIDATIONS ---

    def assert_valid_transition(self, target: ApplicationState) -> None:
        if self.state is None and target == ApplicationState.SUBMITTED:
            return
        if self.state is None:
            raise DomainError(f"Cannot transition to {target} from uninitialized state.")
            
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed and target != self.state:
            raise DomainError(f"Invalid transition {self.state} → {target}. Allowed: {allowed}")

    def assert_can_receive_credit_analysis(self) -> None:
        if self.has_credit_analysis and not self.has_human_override:
            raise DomainError("Model version locking: Cannot append another CreditAnalysisCompleted unless superseded by HumanReviewOverride.")

    def assert_compliance_passed(self, compliance_record) -> None:
        if not compliance_record.is_cleared():
            raise DomainError("Compliance dependency: Cannot approve application without mandatory compliance clearance.")
