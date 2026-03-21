import json
from fastmcp import FastMCP
from ledger.projections.application_summary import ApplicationSummaryProjector
from ledger.projections.daemon import ProjectionDaemon

def register_resources(mcp: FastMCP, store):

    @mcp.resource("ledger://applications/{application_id}")
    async def get_application_state(application_id: str) -> str:
        """Retrieve the latest application summary derived from the async projections."""
        # Note: In a true deployment, this would query the projection database (Postgres SQL table).
        # We simulate the query directly against the event store stream reconstruction as a stand-in,
        # OR fetch from the database via db_pool. We'll use the DB pool dynamically provided if store carries it.
        try:
            # We access the postgres pool from the event store instance
            async with store._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM application_summary_view WHERE application_id = $1", 
                    application_id
                )
                if row:
                    return json.dumps(dict(row), default=str)
                return json.dumps({"error": "Application not found or projection delayed."})
        except Exception as e:
            # Fallback to reconstructing inline if Projection DB is non-existent 
            # (e.g. if we are in testing running InMemoryEventStore)
            events = await store.load_stream(f"loan-{application_id}")
            if not events: return json.dumps({"error": "No stream found"})
            return json.dumps({"events_count": len(events), "status": "Stream exists, projection DB unavailable."})

    @mcp.resource("ledger://ledger/health")
    async def get_ledger_health() -> str:
        """Return system health details including Projection daemon lag."""
        try:
            # Daemon lag can be monitored via `projection_checkpoints` globally
            lag_info = {}
            if hasattr(store, '_pool'):
                async with store._pool.acquire() as conn:
                    max_global = await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events")
                    rows = await conn.fetch("SELECT projection_name, last_position FROM projection_checkpoints")
                    for r in rows:
                        lag_info[r["projection_name"]] = max_global - r["last_position"]
            return json.dumps({"status": "Healthy", "projection_lag": lag_info})
        except Exception as e:
            return json.dumps({"status": "Unhealthy", "error": str(e)})
