import json
from datetime import datetime
from ledger.event_store import EventStore

class ComplianceAuditView:
    name = "compliance_audit"

    def __init__(self, store: EventStore, snapshot_frequency: int = 100):
        self.store = store
        self.snapshot_frequency = snapshot_frequency

    async def setup(self, conn):
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compliance_audit_snapshots (
                application_id TEXT,
                stream_position INT,
                recorded_at TIMESTAMPTZ,
                state_json JSONB,
                PRIMARY KEY (application_id, stream_position)
            )
        """)

    async def handle_event(self, event: dict, conn):
        et = event["event_type"]
        if not et.startswith("Compliance"):
            return
            
        p = event["payload"]
        app_id = p.get("application_id")
        if not app_id: return
        
        pos = event.get("stream_position", 0)
        
        # Take snapshot every N events
        if pos % self.snapshot_frequency == 0:
            # Reconstruct state at exactly this position
            state = await self._rebuild_state(app_id, to_position=pos)
            
            await conn.execute("""
                INSERT INTO compliance_audit_snapshots (application_id, stream_position, recorded_at, state_json)
                VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT DO NOTHING
            """, app_id, pos, event["recorded_at"], json.dumps(state))

    async def _rebuild_state(self, application_id: str, from_position: int = 0, to_position: int | None = None, base_state: dict | None = None) -> dict:
        state = base_state or {"passed": [], "failed": [], "mandatory": []}
        
        events = await self.store.load_stream(f"compliance-{application_id}", from_position=from_position, to_position=to_position)
        
        for e in events:
            et = e["event_type"]
            p = e["payload"]
            
            if et in ["ComplianceCheckInitiated", "ComplianceCheckRequested"]:
                rules = p.get("rules_to_evaluate", [])
                for r in rules:
                    if r not in state["mandatory"]:
                        state["mandatory"].append(r)
            elif et == "ComplianceRulePassed":
                rule = p.get("rule_id")
                if rule not in state["passed"]: state["passed"].append(rule)
                if rule in state["failed"]: state["failed"].remove(rule)
            elif et == "ComplianceRuleFailed":
                rule = p.get("rule_id")
                if rule not in state["failed"]: state["failed"].append(rule)
                if rule in state["passed"]: state["passed"].remove(rule)
                
        return state

    async def get_state_at(self, application_id: str, target_timestamp: datetime, conn) -> dict:
        """
        TEMPORAL QUERY logic:
        1. Find latest snapshot before target timestamp.
        2. Load events from the Event Store starting from snapshot position up to target_timestamp.
        3. Apply events to reconstruct accurate state.
        """
        row = await conn.fetchrow("""
            SELECT stream_position, state_json FROM compliance_audit_snapshots
            WHERE application_id = $1 AND recorded_at <= $2
            ORDER BY stream_position DESC LIMIT 1
        """, application_id, target_timestamp)
        
        start_pos = 0
        state = None
        if row:
            start_pos = row["stream_position"] + 1
            state = json.loads(row["state_json"])
            
        # load forward from snapshot until we hit the timestamp
        # In a real system we'd filter by timestamp in SQL, but for the event store API we load and filter in python
        # Or add a load_events_until(stream_id, timestamp) in EventStore
        
        events = await self.store.load_stream(f"compliance-{application_id}", from_position=start_pos)
        
        target_events = []
        for e in events:
            if e["recorded_at"].replace(tzinfo=None) <= target_timestamp.replace(tzinfo=None):
                target_events.append(e)
            else:
                break
                
        # To avoid duplicate stream loading logic, we abstract the rebuild block if needed, but we can do it inline:
        # Instead of calling `_rebuild_state` modifying `base_state`, we apply events manually.
        final_state = state or {"passed": [], "failed": [], "mandatory": []}
        for e in target_events:
            et = e["event_type"]
            p = e["payload"]
            if et in ["ComplianceCheckInitiated", "ComplianceCheckRequested"]:
                rules = p.get("rules_to_evaluate", [])
                for r in rules:
                    if r not in final_state["mandatory"]:
                        final_state["mandatory"].append(r)
            elif et == "ComplianceRulePassed":
                rule = p.get("rule_id")
                if rule not in final_state["passed"]: final_state["passed"].append(rule)
                if rule in final_state["failed"]: final_state["failed"].remove(rule)
            elif et == "ComplianceRuleFailed":
                rule = p.get("rule_id")
                if rule not in final_state["failed"]: final_state["failed"].append(rule)
                if rule in final_state["passed"]: final_state["passed"].remove(rule)

        return final_state
