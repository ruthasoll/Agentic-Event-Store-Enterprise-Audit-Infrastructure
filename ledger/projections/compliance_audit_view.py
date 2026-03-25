import json
from datetime import datetime
from ledger.schema.events import StoredEvent

class ComplianceAuditView:
    name = "compliance_audit"

    def __init__(self, store, snapshot_frequency: int = 100):
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

    async def handle_event(self, event: StoredEvent, conn):
        et = event.event_type
        
        if not et.startswith("Compliance"):
            return
            
        p = event.payload
        app_id = p.get("application_id")
        if not app_id: return
        
        pos = event.stream_position
        
        if pos % self.snapshot_frequency == 0:
            state = await self._rebuild_state(app_id, to_position=pos)
            await conn.execute("""
                INSERT INTO compliance_audit_snapshots (application_id, stream_position, recorded_at, state_json)
                VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT DO NOTHING
            """, app_id, pos, event.recorded_at, json.dumps(state))

    async def _rebuild_state(self, application_id: str, from_position: int = 0, to_position: int | None = None, base_state: dict | None = None) -> dict:
        state = base_state or {"passed": [], "failed": [], "mandatory": []}
        events = await self.store.load_stream(f"compliance-{application_id}", from_position=from_position, to_position=to_position)
        for e in events:
            self._apply_event_to_state(e, state)
        return state

    def _apply_event_to_state(self, e: StoredEvent, state: dict):
        et = e.event_type
        p = e.payload
        if et in ["ComplianceCheckInitiated", "ComplianceCheckRequested"]:
            for r in p.get("rules_to_evaluate", []):
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

    async def get_state_at(self, application_id: str, target_timestamp: datetime, conn) -> dict:
        row = await conn.fetchrow("""
            SELECT stream_position, state_json FROM compliance_audit_snapshots
            WHERE application_id = $1 AND recorded_at <= $2
            ORDER BY stream_position DESC LIMIT 1
        """, application_id, target_timestamp)
        
        start_pos = 0
        state = {"passed": [], "failed": [], "mandatory": []}
        if row:
            start_pos = row["stream_position"] + 1
            state = json.loads(row["state_json"])
            
        events = await self.store.load_stream(f"compliance-{application_id}", from_position=start_pos)
        
        target_ts_naive = target_timestamp.replace(tzinfo=None)
        
        for e in events:
            if e.recorded_at.replace(tzinfo=None) <= target_ts_naive:
                self._apply_event_to_state(e, state)
            else:
                break
                
        return state

    async def rebuild_from_scratch(self, conn):
        """
        Truncate snapshot table and replay all events to rebuild.
        Must complete without downtime to live reads -> we can use a temporary table and swap, 
        but since projections don't store final views here except snapshots, we just regenerate snapshots.
        """
        # Load all compliance events
        events_by_app = {}
        async for e in self.store.load_all():
            if e.event_type.startswith("Compliance"):
                app_id = e.payload.get("application_id")
                if app_id:
                    events_by_app.setdefault(app_id, []).append(e)
                    
        await conn.execute("TRUNCATE compliance_audit_snapshots")
        
        for app_id, events in events_by_app.items():
            state = {"passed": [], "failed": [], "mandatory": []}
            for e in events:
                self._apply_event_to_state(e, state)
                pos = e.stream_position
                if pos % self.snapshot_frequency == 0:
                    await conn.execute("""
                        INSERT INTO compliance_audit_snapshots (application_id, stream_position, recorded_at, state_json)
                        VALUES ($1, $2, $3, $4::jsonb)
                        ON CONFLICT DO NOTHING
                    """, app_id, pos, e.recorded_at, json.dumps(state))

