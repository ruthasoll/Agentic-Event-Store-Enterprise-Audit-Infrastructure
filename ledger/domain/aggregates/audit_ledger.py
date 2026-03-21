from __future__ import annotations
from dataclasses import dataclass, field
from ledger.schema.events import DomainError

@dataclass
class AuditLedgerAggregate:
    entity_type: str
    entity_id: str
    last_integrity_hash: str | None = None
    total_events_verified: int = 0
    version: int = 0

    @classmethod
    async def load(cls, store, entity_type: str, entity_id: str) -> "AuditLedgerAggregate":
        events = await store.load_stream(f"audit-{entity_type}-{entity_id}")
        agg = cls(entity_type=entity_type, entity_id=entity_id)
        for event in events:
            agg._apply(event)
        agg.version = len(events)
        return agg

    def _apply(self, event: dict) -> None:
        et = event.get("event_type")
        p = event.get("payload", {})
        
        if et == "AuditIntegrityCheckRun":
            self.last_integrity_hash = p.get("integrity_hash")
            self.total_events_verified = p.get("events_verified_count", 0)

    def assert_valid_chain(self, previous_hash: str | None) -> None:
        if self.last_integrity_hash != previous_hash:
            raise DomainError(f"Audit chain broken: expected previous hash {self.last_integrity_hash}, got {previous_hash}")
