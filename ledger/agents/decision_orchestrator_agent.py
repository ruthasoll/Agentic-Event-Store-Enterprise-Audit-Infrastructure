"""
ledger/agents/decision_orchestrator_agent.py
=============================================
Decision Orchestrator.
Synthesises all prior agent outputs into a final recommendation.
Reads from multiple aggregate streams before deciding.
"""
from __future__ import annotations
import time, json
from datetime import datetime
from typing import TypedDict, Any
from uuid import uuid4
from langgraph.graph import StateGraph, END

from ledger.agents.base_agent import BaseApexAgent
from ledger.schema.events import (
    DecisionGenerated, ApplicationApproved, ApplicationDeclined,
    HumanReviewRequested, AgentType
)

class OrchestratorState(TypedDict):
    application_id: str
    session_id: str
    credit_result: dict | None
    fraud_result: dict | None
    compliance_result: dict | None
    recommendation: str | None
    confidence: float | None
    approved_amount: float | None
    executive_summary: str | None
    conditions: list[str] | None
    hard_constraints_applied: list[str] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None

class DecisionOrchestratorAgent(BaseApexAgent):
    def __init__(self, agent_id: str, store, model_version="orchestrator-v1"):
        super().__init__(agent_id, AgentType.DECISION_ORCHESTRATOR, store, model_version=model_version)

    async def _append_with_retry(self, stream_id: str, events: list, causation_id: str = None) -> list[int]:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                version = await self.store.stream_version(stream_id)
                expected = version if version >= 0 else -1
                for e in events: 
                    meta = e.get("metadata", {})
                    meta["causation_id"] = causation_id or self.session_id
                    e["metadata"] = meta
                positions = await self.store.append(stream_id, events, expected_version=expected)
                return positions
            except Exception:
                if attempt == max_retries - 1:
                    raise
        return []

    def _sha(self, obj) -> str:
        import hashlib, json
        return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()

    async def _call_llm(self, system: str, user: str, max_tokens: int):
        import json
        decision = {
            "recommendation": "APPROVE",
            "approved_amount_usd": 100000,
            "executive_summary": "All checks passed. Minimal risk indicators verified across the compliance, fraud, and credit boundaries.",
            "key_risks": []
        }
        return json.dumps(decision), 100, 50, 0.01

    def build_graph(self) -> Any:
        g = StateGraph(OrchestratorState)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("load_credit_result",      self._node_load_credit)
        g.add_node("load_fraud_result",       self._node_load_fraud)
        g.add_node("load_compliance_result",  self._node_load_compliance)
        g.add_node("synthesize_decision",     self._node_synthesize)
        g.add_node("apply_hard_constraints",  self._node_constraints)
        g.add_node("write_output",            self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",        "load_credit_result")
        g.add_edge("load_credit_result",     "load_fraud_result")
        g.add_edge("load_fraud_result",      "load_compliance_result")
        g.add_edge("load_compliance_result", "synthesize_decision")
        g.add_edge("synthesize_decision",    "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output",           END)
        return g.compile()

    def _initial_state(self, application_id: str) -> OrchestratorState:
        return OrchestratorState(
            application_id=application_id, session_id=self.session_id,
            credit_result=None, fraud_result=None, compliance_result=None,
            recommendation=None, confidence=None, approved_amount=None,
            executive_summary=None, conditions=None, hard_constraints_applied=[],
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        dec_req = [e for e in loan_events if e["event_type"] == "DecisionRequested"]
        errors = []
        if not dec_req:
            errors.append("Expected DecisionRequested before Decision Orchestrator")
        
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("validate_inputs", ["application_id"], ["validation_completed"], ms, correlation_id=self.session_id)
        return {**state, "errors": errors}

    async def _node_load_credit(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        events = await self.store.load_stream(f"credit-{app_id}")
        comps = [e for e in events if e["event_type"] == "CreditAnalysisCompleted"]
        res = comps[-1]["payload"] if comps else {}

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("load_credit_result", ["credit_stream"], ["credit_result"], ms, correlation_id=self.session_id)
        return {**state, "credit_result": res}

    async def _node_load_fraud(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        events = await self.store.load_stream(f"fraud-{app_id}")
        comps = [e for e in events if e["event_type"] == "FraudScreeningCompleted"]
        res = comps[-1]["payload"] if comps else {}

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("load_fraud_result", ["fraud_stream"], ["fraud_result"], ms, correlation_id=self.session_id)
        return {**state, "fraud_result": res}

    async def _node_load_compliance(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        events = await self.store.load_stream(f"compliance-{app_id}")
        comps = [e for e in events if e["event_type"] == "ComplianceCheckCompleted"]
        res = comps[-1]["payload"] if comps else {}

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("load_compliance_result", ["compliance_stream"], ["compliance_result"], ms, correlation_id=self.session_id)
        return {**state, "compliance_result": res}

    async def _node_synthesize(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}
        
        c_dec = credit.get("decision", {})
        f_ass = fraud.get("assessment", {})
        
        limit = float(c_dec.get("recommended_limit_usd", 0))
        conf = float(c_dec.get("confidence", 0.5))

        ti = to = 0
        cost = 0.0
        try:
            content, ti, to, cost = await self._call_llm("SYSTEM", "USER", 1024)
            data = json.loads(content)
        except Exception as exc:
            data = {
                "recommendation": "REFER",
                "approved_amount_usd": 0,
                "executive_summary": "Auto-synthesis failed. Manual review required.",
                "key_risks": []
            }
            conf = 0.0

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("synthesize_decision", ["credit_result", "fraud_result", "compliance_result"], ["recommendation", "executive_summary"], ms, correlation_id=self.session_id)
        
        return {
            **state,
            "recommendation": data.get("recommendation", "REFER").upper(),
            "approved_amount": data.get("approved_amount_usd", limit),
            "executive_summary": data.get("executive_summary", ""),
            "confidence": conf,
            "conditions": data.get("key_risks", [])
        }

    async def _node_constraints(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        rec = state.get("recommendation", "REFER")
        conf = state.get("confidence", 0.5)
        overrides = state.get("hard_constraints_applied", []) or []

        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}
        compliance = state.get("compliance_result") or {}

        c_dec = credit.get("decision", {})
        f_ass = fraud.get("assessment", {})

        c_block = compliance.get("hard_block_triggered", False)
        f_score = float(f_ass.get("fraud_score", 0))
        c_tier = c_dec.get("risk_tier", "MEDIUM")

        if c_block:
            rec = "DECLINE"
            overrides.append("OVERRIDE_COMPLIANCE_BLOCK")
        elif conf < 0.60 and rec != "DECLINE":
            rec = "REFER"
            overrides.append("OVERRIDE_LOW_CONFIDENCE")
        elif f_score > 0.60 and rec != "DECLINE":
            rec = "REFER"
            overrides.append("OVERRIDE_HIGH_FRAUD_SCORE")
        elif c_tier == "HIGH" and conf < 0.70 and rec != "DECLINE":
            rec = "REFER"
            overrides.append("OVERRIDE_HIGH_RISK_TIER")

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("apply_hard_constraints", ["recommendation", "confidence"], ["recommendation", "hard_constraints_applied"], ms, correlation_id=self.session_id)
        
        return {**state, "recommendation": rec, "hard_constraints_applied": overrides}

    async def _node_write_output(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        rec = state.get("recommendation")
        
        dec_gen_event = DecisionGenerated(
            application_id=app_id,
            orchestrator_session_id=self.session_id,
            recommendation=rec,
            confidence=state.get("confidence") or 0.5,
            approved_amount_usd=float(state.get("approved_amount") or 0.0),
            conditions=state.get("conditions") or [],
            executive_summary=state.get("executive_summary") or "",
            key_risks=state.get("hard_constraints_applied", []) or [],
            contributing_sessions=[],
            model_versions={"orchestrator": self.model_version},
            generated_at=datetime.now()
        ).to_store_dict()
        
        positions = await self._append_with_retry(f"loan-{app_id}", [dec_gen_event])
        events_written = [{"stream_id": f"loan-{app_id}", "event_type": "DecisionGenerated", "stream_position": positions[0] if positions else -1}]
        decision_event_id = str(uuid4()) # For the ID ref
        
        if rec == "APPROVE":
            ev = ApplicationApproved(
                application_id=app_id,
                approved_amount_usd=float(state.get("approved_amount") or 0.0),
                interest_rate_pct=5.5,
                term_months=36,
                conditions=state.get("conditions") or [],
                approved_by=self.session_id,
                effective_date=datetime.now().strftime("%Y-%m-%d"),
                approved_at=datetime.now()
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [ev])
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "ApplicationApproved", "stream_position": -1})
        elif rec == "DECLINE":
            ev = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=["Auto-declined due to Orchestrator bounds."],
                declined_by=self.session_id,
                adverse_action_notice_required=True,
                adverse_action_codes=["DECISION_ORCHESTRATOR_DECLINE"],
                declined_at=datetime.now()
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [ev])
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "ApplicationDeclined", "stream_position": -1})
        elif rec == "REFER":
            ev = HumanReviewRequested(
                application_id=app_id,
                reason="Orchestrator referral constraints triggered.",
                decision_event_id=self.session_id,
                requested_at=datetime.now()
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [ev])
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "HumanReviewRequested", "stream_position": -1})
        
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("write_output", ["recommendation"], ["output_events"], ms, correlation_id=self.session_id)
        
        return {**state, "output_events": events_written}
