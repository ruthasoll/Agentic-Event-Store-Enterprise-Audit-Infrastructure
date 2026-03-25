import pytest
import asyncio
from ledger.event_store import InMemoryEventStore
from ledger.agents.base_agent import BaseApexAgent, AgentType

class MockAgent(BaseApexAgent):
    def build_graph(self):
        return None
    def _initial_state(self, application_id):
        return {}

@pytest.mark.asyncio
async def test_agent_crash_recovery_logic():
    store = InMemoryEventStore()
    agent_id = "agent-1"
    app_id = "APP-100"
    
    agent = MockAgent(agent_id, AgentType.CREDIT_ANALYSIS, store, "gpt-4o")
    
    # 1. Start a session and execute some nodes, then "crash"
    await agent.start_session(app_id)
    await agent._record_node_execution("load_data", ["app_id"], ["data"], 100)
    await agent._record_node_execution("analyze_credit", ["data"], ["decision"], 200)
    
    # Simulate partial decision by ending on a decision node without completing the session
    # (Actually analyze_credit is the decision node in this mock)
    
    # 2. Create a NEW agent instance (simulating a restart/new process)
    agent2 = MockAgent(agent_id, AgentType.CREDIT_ANALYSIS, store, "gpt-4o")
    
    # 3. Reconstruct context BEFORE starting
    ctx = await agent2.reconstruct_agent_context(app_id)
    
    assert ctx.is_recovered is True
    assert "load_data" in ctx.nodes_executed
    assert "analyze_credit" in ctx.nodes_executed
    assert ctx.pending_work.get("partial_decision_detected") is True
    
    # 4. Starting the session should log AgentSessionRecovered
    await agent2.start_session(app_id)
    
    history = await store.load_stream(f"agent-{agent_id}-{app_id}")
    event_types = [e.event_type for e in history]
    assert "AgentSessionRecovered" in event_types
    assert event_types.index("AgentSessionRecovered") > event_types.index("AgentSessionStarted")

@pytest.mark.asyncio
async def test_no_recovery_on_clean_completion():
    store = InMemoryEventStore()
    agent_id = "agent-1"
    app_id = "APP-200"
    
    agent = MockAgent(agent_id, AgentType.CREDIT_ANALYSIS, store, "gpt-4o")
    
    await agent.start_session(app_id)
    await agent._record_node_execution("node1", [], [], 10)
    await agent.complete_session("Completed", app_id)
    
    # New agent
    agent2 = MockAgent(agent_id, AgentType.CREDIT_ANALYSIS, store, "gpt-4o")
    ctx = await agent2.reconstruct_agent_context(app_id)
    
    assert ctx.is_recovered is False
    assert ctx.last_event_position >= 0
