import os
import json
from datetime import datetime
from typing import List, Dict, Any
from fastmcp import FastMCP
from dotenv import load_dotenv
from ledger.event_store import EventStore, get_default_upcaster_registry
from ledger.mcp.resources import register_resources

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
        # Register resources from the shared module
        register_resources(mcp, _store)
    return _store

def format_error(e: Exception, suggested_action: str) -> Dict[str, Any]:
    """Uniform structured error response for LLM consumability."""
    return {
        "error": str(e),
        "error_type": type(e).__name__,
        "suggested_action": suggested_action
    }

@mcp.tool()
async def list_auditable_entities() -> List[Dict[str, Any]]:
    """
    Returns a list of all auditable event streams (loans, agents, compliance, etc.) 
    with their metadata.
    
    Preconditions:
    - EventStore must be connected to a valid Postgres database.
    """
    try:
        store = await get_store()
        streams = await store.list_streams()
        return [s.model_dump(mode='json') for s in streams]
    except Exception as e:
        return format_error(e, "Check DATABASE_URL and ensure Postgres is reachable.")

@mcp.tool()
async def inspect_loan_history(application_id: str) -> List[Dict[str, Any]] | Dict[str, Any]:
    """
    Fetches the full event history for a specific loan application.
    Automatically applies upcasting to ensure all events are in the latest format.
    
    Preconditions:
    - application_id must be a valid string identifier (e.g. 'APP-123').
    """
    try:
        store = await get_store()
        stream_id = f"loan-{application_id}"
        events = await store.load_stream(stream_id)
        if not events:
            return format_error(ValueError(f"No stream found for {stream_id}"), 
                                "Verify the application_id is correct or check if it was archived.")
        return [e.model_dump(mode='json') for e in events]
    except Exception as e:
        return format_error(e, "Check if the stream ID format fits 'loan-{application_id}'.")

@mcp.tool()
async def run_integrity_check(stream_id: str) -> Dict[str, Any]:
    """
    Runs a cryptographic integrity check on a specific event stream.
    Re-calculates the SHA256 hash chain to verify no events have been tampered with.
    
    Preconditions:
    - stream_id must exist in the event_streams table.
    """
    try:
        store = await get_store()
        is_valid = await store.verify_stream_integrity(stream_id)
        
        # Get stream metadata for context
        meta = await store.get_stream_metadata(stream_id)
        if not meta:
            return format_error(ValueError(f"Stream {stream_id} not found"), 
                                "List entities first to find a valid stream_id.")
        
        return {
            "stream_id": stream_id,
            "integrity_check_passed": is_valid,
            "current_version": meta.current_version,
            "verified_at": datetime.now().isoformat()
        }
    except Exception as e:
        return format_error(e, "Ensure the stream_id is fully qualified (e.g. 'loan-123' not '123').")

@mcp.tool()
async def get_compliance_summary(application_id: str) -> Dict[str, Any]:
    """
    Retrieves the compliance status for a loan application.
    Aggregates events from the compliance record stream.
    
    Preconditions:
    - application_id must have a 'compliance-{application_id}' stream.
    """
    try:
        store = await get_store()
        stream_id = f"compliance-{application_id}"
        events = await store.load_stream(stream_id)
        if not events:
            return format_error(ValueError(f"No compliance stream found for {application_id}"), 
                                "Ensure compliance checks have been initiated for this application.")
        
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
    except Exception as e:
        return format_error(e, "Check application_id and stream existence.")

@mcp.tool()
async def get_global_log(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]] | Dict[str, Any]:
    """
    Retrieves the global event log across all streams, ordered by global_position.
    Useful for system-wide auditing and timeline reconstruction.
    
    Preconditions:
    - limit must be between 1 and 500.
    """
    try:
        if not (1 <= limit <= 500):
            return format_error(ValueError("Limit must be between 1 and 500"), "Adjust the limit parameter.")
        
        store = await get_store()
        # load_all is an async generator
        count = 0
        results = []
        async for event in store.load_all(from_position=offset):
            results.append(event.model_dump(mode='json'))
            count += 1
            if count >= limit:
                break
        return results
    except Exception as e:
        return format_error(e, "Check database status and connection pool.")

if __name__ == "__main__":
    mcp.run()
