from datetime import datetime
from typing import Any, Dict, List
from uuid import uuid4
from ledger.event_store import EventStore, OptimisticConcurrencyError

class BaseApexAgent:
    """
    Base class for all Agents interacting with The Ledger.
    Enforces Gas Town patterns and strict event stream logging.
    """
    def __init__(self, agent_id: str, store: EventStore, model_version: str):
        self.agent_id = agent_id
        self.store = store
        self.model_version = model_version
        self.session_id = str(uuid4())

    async def start_session(self, application_id: str, context_source: str, context_token_count: int, correlation_id: str):
        """Gas Town: Precedes any action with AgentSessionStarted"""
        event = {
            "event_type": "AgentSessionStarted",
            "payload": {
                "session_id": self.session_id,
                "agent_id": self.agent_id,
                "application_id": application_id,
                "model_version": self.model_version,
                "context_source": context_source,
                "context_token_count": context_token_count,
            }
        }
        await self._append_to_agent_stream([event], correlation_id)
        
    async def _append_to_agent_stream(self, events: List[Dict[str, Any]], correlation_id: str):
        stream_id = f"agent-{self.agent_id}-{self.session_id}"
        
        # Agents write to their isolated stream. We use ExpectedVersion = -1 if new, but for simplicity agent streams usually don't have overlapping writes.
        # However, to be purely OCC safe, we should track version.
        version = await self.store.stream_version(stream_id)
        
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
        await self._append_to_agent_stream([event], correlation_id or self.session_id)

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
        await self._append_to_agent_stream([event], correlation_id or self.session_id)
        
    async def complete_session(self, status: str, correlation_id: str, error_message: str = None):
        """Mark session as AgentSessionCompleted, AgentSessionFailed, or AgentSessionRecovered"""
        event = {
            "event_type": f"AgentSession{status}",
            "payload": {
                "session_id": self.session_id,
                "completed_at": datetime.utcnow().isoformat(),
                "error_message": error_message
            }
        }
        await self._append_to_agent_stream([event], correlation_id)
