from __future__ import annotations
from dataclasses import dataclass, field
from ledger.schema.events import DomainError

@dataclass
class AgentSessionAggregate:
    agent_id: str
    session_id: str
    application_id: str | None = None
    has_loaded_context: bool = False
    model_version: str | None = None
    has_made_decision: bool = False
    version: int = 0

    @classmethod
    async def load(cls, store, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in events:
            agg._apply(event)
        agg.version = len(events)
        return agg

    def _apply(self, event: dict) -> None:
        et = event.get("event_type")
        p = event.get("payload", {})

        if et == "AgentSessionStarted" or et == "AgentContextLoaded":
            self.has_loaded_context = True
            self.model_version = p.get("model_version")
            self.application_id = p.get("application_id")
        
        # any decision actions count as making a decision for causal chain reference checks
        if et in ["CreditAnalysisCompleted", "FraudScreeningCompleted", "ComplianceCheckCompleted", "DecisionGenerated"]:
            self.has_made_decision = True

    def assert_context_loaded(self) -> None:
        if not self.has_loaded_context:
            raise DomainError("Gas Town pattern: Agent context must be loaded (AgentSessionStarted/AgentContextLoaded) before appending decisions.")

    def assert_model_version_current(self, target_version: str) -> None:
        if self.model_version != target_version:
            raise DomainError(f"Agent session model_version {self.model_version} does not match command version {target_version}")
