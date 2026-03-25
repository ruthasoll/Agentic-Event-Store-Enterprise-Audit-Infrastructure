from ledger.schema.events import (
    DomainError, ComplianceVerdict, ComplianceCheckInitiated, 
    ComplianceRulePassed, ComplianceRuleFailed, ComplianceCheckCompleted
)

@dataclass
class ComplianceRecordAggregate:
    application_id: str
    mandatory_rules: list[str] = field(default_factory=list)
    passed_rules: set[str] = field(default_factory=set)
    failed_rules: set[str] = field(default_factory=set)
    regulation_version: str | None = None
    verdict: ComplianceVerdict | None = None
    version: int = 0

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        events = await store.load_stream(f"compliance-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            if hasattr(event, "model_dump"):
                event_dict = event.model_dump(mode='json')
            else:
                event_dict = event
            agg._apply(event_dict)
        agg.version = len(events)
        return agg

    def _apply(self, event: dict) -> None:
        et = event.get("event_type")
        p = event.get("payload", {})

        if et == "ComplianceCheckInitiated" or et == "ComplianceCheckRequested":
            req_rules = p.get("rules_to_evaluate")
            if req_rules:
                self.mandatory_rules.extend(req_rules)
            self.regulation_version = p.get("regulation_set_version")
            
        elif et == "ComplianceRulePassed":
            rule_id = p.get("rule_id")
            if rule_id:
                self.passed_rules.add(rule_id)
                self.failed_rules.discard(rule_id)
                
        elif et == "ComplianceRuleFailed":
            rule_id = p.get("rule_id")
            if rule_id:
                self.failed_rules.add(rule_id)
                self.passed_rules.discard(rule_id)
                
        elif et == "ComplianceCheckCompleted":
            verdict_str = p.get("overall_verdict")
            if verdict_str:
                self.verdict = ComplianceVerdict(verdict_str)

    def is_cleared(self) -> bool:
        if self.verdict == ComplianceVerdict.CLEAR:
            return True
        if self.verdict == ComplianceVerdict.BLOCKED:
            return False
            
        # Ensure all mandatory rules have passed
        if not self.mandatory_rules:
            # If no mandatory rules ever requested but checking cleared, typically we'd return False or True based on policy.
            # Assuming if none initiated, it's not cleared.
            return False
            
        for rule in self.mandatory_rules:
            if rule not in self.passed_rules:
                 return False
        return True

    # --- COMMANDS ---

    def initiate(self, rules: list[str], regulation_version: str, session_id: str) -> ComplianceCheckInitiated:
        """Command: Initiate compliance check."""
        from datetime import datetime, timezone
        return ComplianceCheckInitiated(
            application_id=self.application_id,
            session_id=session_id,
            rules_to_evaluate=rules,
            regulation_set_version=regulation_version,
            initiated_at=datetime.now(timezone.utc)
        )

    def pass_rule(self, rule_id: str, rule_name: str, session_id: str, evidence_hash: str) -> ComplianceRulePassed:
        """Command: Pass a specific rule."""
        from datetime import datetime, timezone
        return ComplianceRulePassed(
            application_id=self.application_id,
            session_id=session_id,
            rule_id=rule_id,
            rule_name=rule_name,
            rule_version="1.0", # Simplified
            evidence_hash=evidence_hash,
            evaluation_notes="Automated pass",
            evaluated_at=datetime.now(timezone.utc)
        )

    def fail_rule(self, rule_id: str, rule_name: str, session_id: str, reason: str, evidence_hash: str) -> ComplianceRuleFailed:
        """Command: Fail a specific rule."""
        from datetime import datetime, timezone
        return ComplianceRuleFailed(
            application_id=self.application_id,
            session_id=session_id,
            rule_id=rule_id,
            rule_name=rule_name,
            rule_version="1.0",
            failure_reason=reason,
            is_hard_block=True,
            remediation_available=False,
            evidence_hash=evidence_hash,
            evaluated_at=datetime.now(timezone.utc)
        )

    def complete(self, session_id: str) -> ComplianceCheckCompleted:
        """Command: Consolidate scores and issue final verdict."""
        passed = len(self.passed_rules)
        failed = len(self.failed_rules)
        
        # Simple logic: any failure = BLOCKED
        verdict = ComplianceVerdict.CLEAR if failed == 0 else ComplianceVerdict.BLOCKED
        
        from datetime import datetime, timezone
        return ComplianceCheckCompleted(
            application_id=self.application_id,
            session_id=session_id,
            rules_evaluated=passed + failed,
            rules_passed=passed,
            rules_failed=failed,
            rules_noted=0,
            has_hard_block=failed > 0,
            overall_verdict=verdict,
            completed_at=datetime.now(timezone.utc)
        )
