import os
import json
from datetime import datetime
from typing import List, Dict, Any
from fastmcp import FastMCP
from dotenv import load_dotenv
from ledger.event_store import EventStore, get_default_upcaster_registry

# Load environment variables (.env)
load_dotenv()

# Initialize FastMCP
mcp = FastMCP("The Ledger Audit Server")

# Global EventStore instance (lazy loaded)
_store = None

async def get_store():
    global _store
    if _store is None:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL not set in environment")
        _store = EventStore(db_url, upcaster_registry=get_default_upcaster_registry())
        await _store.connect()
    return _store

@mcp.tool()
async def list_auditable_entities() -> List[Dict[str, Any]]:
    """
    Returns a list of all auditable event streams (loans, agents, compliance, etc.) 
    with their metadata.
    """
    store = await get_store()
    streams = await store.list_streams()
    return [s.model_dump(mode='json') for s in streams]

@mcp.tool()
async def inspect_loan_history(application_id: str) -> List[Dict[str, Any]]:
    """
    Fetches the full event history for a specific loan application.
    Automatically applies upcasting to ensure all events are in the latest format.
    """
    store = await get_store()
    stream_id = f"loan-{application_id}"
    events = await store.load_stream(stream_id)
    return [e.model_dump(mode='json') for e in events]

@mcp.tool()
async def verify_audit_trail(stream_id: str) -> Dict[str, Any]:
    """
    Runs a cryptographic integrity check on a specific event stream.
    Re-calculates the SHA256 hash chain to verify no events have been tampered with.
    """
    store = await get_store()
    is_valid = await store.verify_stream_integrity(stream_id)
    
    # Get stream metadata for context
    meta = await store.get_stream_metadata(stream_id)
    
    return {
        "stream_id": stream_id,
        "integrity_check_passed": is_valid,
        "current_version": meta.current_version if meta else None,
        "verified_at": datetime.now().isoformat()
    }

@mcp.tool()
async def get_compliance_summary(application_id: str) -> Dict[str, Any]:
    """
    Retrieves the compliance status for a loan application.
    Aggregates events from the compliance record stream.
    """
    store = await get_store()
    stream_id = f"compliance-{application_id}"
    events = await store.load_stream(stream_id)
    
    passed = []
    failed = []
    verdict = "UNKNOWN"
    
    for e in events:
        ptype = e.event_type
        payload = e.payload
        if ptype == "ComplianceRulePassed":
            passed.append(payload.get("rule_name") or payload.get("rule_id"))
        elif ptype == "ComplianceRuleFailed":
            failed.append({
                "rule": payload.get("rule_name") or payload.get("rule_id"),
                "reason": payload.get("failure_reason")
            })
        elif ptype == "ComplianceCheckCompleted":
            verdict = payload.get("overall_verdict")
            
    return {
        "application_id": application_id,
        "verdict": verdict,
        "rules_passed_count": len(passed),
        "rules_failed_count": len(failed),
        "failing_rules": failed,
        "passed_rules": passed
    }

if __name__ == "__main__":
    mcp.run()
