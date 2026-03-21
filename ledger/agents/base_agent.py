import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.schema.events import AgentSessionStarted, AgentSessionCompleted, AgentSessionFailed, AgentType

class BaseApexAgent:
    """
    Base class for all Agents interacting with The Ledger.
    Enforces Gas Town patterns and strict event stream logging.
    """
    def __init__(self, agent_id: str, agent_type: AgentType, store: EventStore, model_version: str):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.store = store
        self.model_version = model_version
        self.session_id = str(uuid4())
        self.application_id: Optional[str] = None

    def _parse_json(self, text: str) -> dict:
        """Robustly parse JSON from LLM output, handling markdown blocks."""
        if not text: return {}
        clean = text.strip()
        if clean.startswith("```json"):
            clean = clean[7:]
        if clean.endswith("```"):
            clean = clean[:-3]
        return json.loads(clean.strip())

    async def start_session(self, application_id: str, context: dict = None) -> str:
        self.application_id = application_id
        stream_id = f"agent-{self.agent_id}-{application_id}"
        events = await self.store.load_stream(stream_id)
        
        ctx_source = "new_invocation"
        if events:
            starts = [e for e in events if e["event_type"] == "AgentSessionStarted"]
            ends = [e for e in events if e["event_type"] == "AgentSessionCompleted"]
            if len(starts) > len(ends):
                prior_id = starts[-1]["payload"]["session_id"]
                ctx_source = f"prior_session_replay: {prior_id}"
                
        payload = {
            "session_id": self.session_id,
            "agent_type": self.agent_type,
            "agent_id": self.agent_id,
            "application_id": application_id,
            "model_version": self.model_version,
            "langgraph_graph_version": "1.0.0", # Simplified
            "context_source": ctx_source,
            "context_token_count": 0 # Simplified
        }
        if context: payload.update(context)
        
        ev = AgentSessionStarted(**payload, started_at=datetime.now(timezone.utc))
        await self._append_to_agent_stream(application_id, [ev.to_store_dict()])
        return self.session_id
        
    async def _append_to_agent_stream(self, application_id: str, events: List[Dict[str, Any]], correlation_id: str = None):
        stream_id = f"agent-{self.agent_id}-{application_id}"
        
        # Agents write to their isolated stream. We use ExpectedVersion = -1 if new, but for simplicity agent streams usually don't have overlapping writes.
        # However, to be purely OCC safe, we should track version.
        
        for e in events:
            # We enforce structure here if not already done
            if "event_version" not in e:
                e["event_version"] = 1
                
        # Retry loop for OCC
        max_retries = 3
        for attempt in range(max_retries):
            try:
                version = await self.store.stream_version(stream_id)
                await self.store.append(stream_id, events, expected_version=version, metadata={"correlation_id": correlation_id})
                return
            except OptimisticConcurrencyError:
                if attempt == max_retries - 1:
                    raise
                pass

    async def _record_node_execution(self, node_name: str, input_keys: list, output_keys: list, duration_ms: int, ti: int = 0, to: int = 0, cost: float = 0.0, correlation_id: str = None):
        event = {
            "event_type": "AgentNodeExecuted",
            "payload": {
                "node_name": node_name,
                "input_keys": input_keys,
                "output_keys": output_keys,
                "llm_tokens": ti + to,
                "duration_ms": duration_ms
            }
        }
        await self._append_to_agent_stream(self.application_id, [event], correlation_id or self.session_id)

    async def _record_tool_call(self, tool_name: str, arguments: dict|str, result_summary: str, duration_ms: int, correlation_id: str = None):
        event = {
            "event_type": "AgentToolCalled",
            "payload": {
                "tool_name": tool_name,
                "arguments": arguments,
                "result_summary": result_summary,
                "duration_ms": duration_ms
            }
        }
        await self._append_to_agent_stream(self.application_id, [event], correlation_id or self.session_id)
        
    async def complete_session(self, status: str, application_id: str, error_message: str = None):
        """Mark session as AgentSessionCompleted, AgentSessionFailed, or AgentSessionRecovered"""
        if status == "Completed":
            ev = AgentSessionCompleted(
                session_id=self.session_id,
                agent_type=self.agent_type,
                agent_id=self.agent_id,
                application_id=application_id,
                total_nodes_executed=0,
                total_llm_calls=0,
                total_tokens_used=0,
                total_cost_usd=0.0,
                total_duration_ms=0,
                completed_at=datetime.now(timezone.utc)
            )
        else:
            ev = AgentSessionFailed(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=application_id,
                error_type="ExecutionError",
                error_message=error_message or "Unknown error",
                recoverable=True,
                failed_at=datetime.now(timezone.utc)
            )
        await self._append_to_agent_stream(application_id, [ev.to_store_dict()])

    async def process_application(self, application_id: str):
        """Standard entry point for running an agent session."""
        await self.start_session(application_id)
        graph = self.build_graph()
        initial = self._initial_state(application_id)
        try:
            await graph.ainvoke(initial)
            await self.complete_session("Completed", application_id)
        except Exception as e:
            await self.complete_session("Failed", application_id, error_message=str(e))
            raise
