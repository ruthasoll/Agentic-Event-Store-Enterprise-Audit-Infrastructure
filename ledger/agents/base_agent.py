import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4
from pydantic import BaseModel, Field
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.schema.events import (
    AgentSessionStarted, AgentSessionCompleted, AgentSessionFailed, 
    AgentSessionRecovered, AgentType, StoredEvent
)

class AgentContext(BaseModel):
    """Structured memory for the Gas Town pattern."""
    session_id: str
    agent_id: str
    application_id: str
    agent_type: AgentType
    last_event_position: int = -1
    nodes_executed: List[str] = Field(default_factory=list)
    tools_called: List[Dict[str, Any]] = Field(default_factory=list)
    pending_work: Dict[str, Any] = Field(default_factory=dict)
    is_recovered: bool = False

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

    async def reconstruct_agent_context(self, application_id: str) -> AgentContext:
        """
        Replays the agent's dedicated stream to rebuild its context.
        Detects if a session was interrupted (Gas Town pattern).
        """
        stream_id = f"agent-{self.agent_id}-{application_id}"
        events = await self.store.load_stream(stream_id)
        
        if not events:
            return AgentContext(
                session_id=self.session_id,
                agent_id=self.agent_id,
                application_id=application_id,
                agent_type=self.agent_type
            )
        
        # We look for the MOST RECENT session
        # Find the last 'AgentSessionStarted' event
        start_indices = [i for i, e in enumerate(events) if e.event_type == "AgentSessionStarted"]
        if not start_indices:
            return AgentContext(session_id=self.session_id, agent_id=self.agent_id, application_id=application_id, agent_type=self.agent_type)
        
        last_start_idx = start_indices[-1]
        session_events = events[last_start_idx:]
        start_event = session_events[0]
        
        # Check if session completed
        is_completed = any(e.event_type in ("AgentSessionCompleted", "AgentSessionFailed") for e in session_events)
        
        ctx = AgentContext(
            session_id=start_event.payload["session_id"],
            agent_id=self.agent_id,
            application_id=application_id,
            agent_type=self.agent_type,
            last_event_position=events[-1].stream_position,
            is_recovered=not is_completed
        )
        
        for e in session_events:
            if e.event_type == "AgentNodeExecuted":
                ctx.nodes_executed.append(e.payload["node_name"])
            elif e.event_type == "AgentToolCalled":
                ctx.tools_called.append(e.payload)
                
        # Detect Partial Decision:
        # If the last node was the decision node but no completion, it's pending output.
        if ctx.is_recovered and ctx.nodes_executed:
            last_node = ctx.nodes_executed[-1]
            if "decision" in last_node.lower() or "orchestrate" in last_node.lower():
                ctx.pending_work["partial_decision_detected"] = True
                
        return ctx

    async def start_session(self, application_id: str, context: dict = None) -> str:
        self.application_id = application_id
        
        # RECONSTRUCT CONTEXT (Gas Town Recovery)
        recovered_ctx = await self.reconstruct_agent_context(application_id)
        
        if recovered_ctx.is_recovered:
            # We are resuming!
            self.session_id = recovered_ctx.session_id
            ev = AgentSessionRecovered(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=application_id,
                recovered_from_session_id=recovered_ctx.session_id,
                recovery_point=recovered_ctx.nodes_executed[-1] if recovered_ctx.nodes_executed else "start",
                recovered_at=datetime.now(timezone.utc)
            )
            await self._append_to_agent_stream(application_id, [ev.to_store_dict()])
            return self.session_id

        # Else start new session
        payload = {
            "session_id": self.session_id,
            "agent_type": self.agent_type,
            "agent_id": self.agent_id,
            "application_id": application_id,
            "model_version": self.model_version,
            "langgraph_graph_version": "1.0.0",
            "context_source": "new_invocation",
            "context_token_count": 0
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
