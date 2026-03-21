from __future__ import annotations
from dataclasses import dataclass, field
from ledger.schema.events import DomainError, ComplianceVerdict

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
            agg._apply(event)
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

    def assert_can_issue_clearance(self) -> None:
        for rule in self.mandatory_rules:
            if rule not in self.passed_rules:
                raise DomainError(f"Cannot issue compliance clearance: missing mandatory check for {rule}.")
