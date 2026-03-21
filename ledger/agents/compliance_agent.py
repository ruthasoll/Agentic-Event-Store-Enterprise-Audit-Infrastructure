"""
ledger/agents/compliance_agent.py
=======================================
Compliance Agent.
Evaluates 6 deterministic regulatory rules in sequence.
Stops at first hard block (is_hard_block=True).
LLM not used in rule evaluation — only for human-readable evidence summaries.
"""
from __future__ import annotations
import time, json
from datetime import datetime
from typing import TypedDict, Any
from uuid import uuid4
from langgraph.graph import StateGraph, END

from ledger.agents.base_agent import BaseApexAgent
from ledger.schema.events import (
    ComplianceCheckInitiated, ComplianceRulePassed, ComplianceRuleFailed,
    ComplianceRuleNoted, ComplianceCheckCompleted,
    DecisionRequested, ApplicationDeclined, AgentType, ComplianceVerdict
)

class ComplianceState(TypedDict):
    application_id: str
    session_id: str
    company_profile: dict | None
    rule_results: list[dict] | None
    has_hard_block: bool
    block_rule_id: str | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None

# Regulation definitions — deterministic, no LLM in decision path
REGULATIONS = {
    "REG-001": {
        "name": "Bank Secrecy Act (BSA) Check",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not any(
            f.get("flag_type") == "AML_WATCH" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active AML Watch flag present. Remediation required.",
        "remediation": "Provide enhanced due diligence documentation within 10 business days.",
    },
    "REG-002": {
        "name": "OFAC Sanctions Screening",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: not any(
            f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active OFAC Sanctions Review. Application blocked.",
        "remediation": None,
    },
    "REG-003": {
        "name": "Jurisdiction Lending Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: co.get("jurisdiction") != "MT",
        "failure_reason": "Jurisdiction MT not approved for commercial lending at this time.",
        "remediation": None,
    },
    "REG-004": {
        "name": "Legal Entity Type Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not (
            co.get("legal_type") == "Sole Proprietor"
            and (co.get("requested_amount_usd", 0) or 0) > 250_000
        ),
        "failure_reason": "Sole Proprietor loans >$250K require additional documentation.",
        "remediation": "Submit SBA Form 912 and personal financial statement.",
    },
    "REG-005": {
        "name": "Minimum Operating History",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: (2024 - (co.get("founded_year") or 2024)) >= 2,
        "failure_reason": "Business must have at least 2 years of operating history.",
        "remediation": None,
    },
    "REG-006": {
        "name": "CRA Community Reinvestment",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: True,   # Always noted, never fails
        "note_type": "CRA_CONSIDERATION",
        "note_text": "Jurisdiction qualifies for Community Reinvestment Act consideration.",
    },
}

class ComplianceAgent(BaseApexAgent):
    def __init__(self, agent_id: str, store, registry, model_version="compliance-v1"):
        super().__init__(agent_id, AgentType.COMPLIANCE, store, model_version=model_version)
        self.registry = registry

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

    def build_graph(self) -> Any:
        g = StateGraph(ComplianceState)
        g.add_node("validate_inputs",     self._node_validate_inputs)
        g.add_node("load_company_profile",self._node_load_profile)
        
        async def node_reg001(s): return await self._evaluate_rule(s, "REG-001")
        async def node_reg002(s): return await self._evaluate_rule(s, "REG-002")
        async def node_reg003(s): return await self._evaluate_rule(s, "REG-003")
        async def node_reg004(s): return await self._evaluate_rule(s, "REG-004")
        async def node_reg005(s): return await self._evaluate_rule(s, "REG-005")
        async def node_reg006(s): return await self._evaluate_rule(s, "REG-006")
        
        g.add_node("evaluate_reg001",     node_reg001)
        g.add_node("evaluate_reg002",     node_reg002)
        g.add_node("evaluate_reg003",     node_reg003)
        g.add_node("evaluate_reg004",     node_reg004)
        g.add_node("evaluate_reg005",     node_reg005)
        g.add_node("evaluate_reg006",     node_reg006)
        g.add_node("write_output",        self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",      "load_company_profile")
        g.add_edge("load_company_profile", "evaluate_reg001")

        # Conditional edges: stop at hard block, proceed otherwise
        for src, nxt in [
            ("evaluate_reg001", "evaluate_reg002"),
            ("evaluate_reg002", "evaluate_reg003"),
            ("evaluate_reg003", "evaluate_reg004"),
            ("evaluate_reg004", "evaluate_reg005"),
            ("evaluate_reg005", "evaluate_reg006"),
            ("evaluate_reg006", "write_output"),
        ]:
            g.add_conditional_edges(
                src,
                lambda s, _nxt=nxt: "write_output" if s["has_hard_block"] else _nxt,
            )
        g.add_edge("write_output", END)
        return g.compile()

    def _initial_state(self, application_id: str) -> ComplianceState:
        return ComplianceState(
            application_id=application_id, session_id=self.session_id,
            company_profile=None, rule_results=[], has_hard_block=False,
            block_rule_id=None, errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        
        # Initiate Compliance Screening
        event = ComplianceCheckInitiated(
            application_id=app_id,
            session_id=self.session_id,
            regulation_set_version="1.0.0",
            rules_to_evaluate=["BSA_KNOW_YOUR_CUSTOMER", "OFAC_SANCTIONS", "AML_MONITORING", "CREDIT_HISTORY_CHECK", "IDENTITY_VERIFICATION", "FRAUD_REGISTRY_CHECK"],
            initiated_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"compliance-{app_id}", [event])

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("validate_inputs", ["application_id"], ["compliance_check_initiated"], ms, correlation_id=self.session_id)
        return state

    async def _node_load_profile(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        sub_events = [e for e in loan_events if e["event_type"] == "ApplicationSubmitted"]
        applicant_id = "COMP-001"
        requested_amount_usd = 0
        if sub_events:
            p = sub_events[0]["payload"]
            applicant_id = p.get("applicant_id", applicant_id)
            requested_amount_usd = p.get("requested_amount_usd", 0)

        profile = await self.registry.get_company(applicant_id) or {}
        compliance_flags = await self.registry.get_compliance_flags(applicant_id, active_only=True) or []
        
        co = {**profile, "compliance_flags": compliance_flags, "requested_amount_usd": requested_amount_usd}

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("load_company_profile", ["applicant_id"], ["company_profile"], ms, correlation_id=self.session_id)
        return {**state, "company_profile": co}

    async def _evaluate_rule(self, state: ComplianceState, rule_id: str) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        reg = REGULATIONS[rule_id]
        co = state.get("company_profile", {})
        passes = reg["check"](co)
        evidence_hash = self._sha(f"{rule_id}-{co.get('company_id')}-{passes}")

        results = state.get("rule_results", []) or []
        event = None

        if rule_id == "REG-006":
            event = ComplianceRuleNoted(
                application_id=app_id, session_id=self.session_id,
                rule_id=rule_id, rule_name=reg["name"], note_type=reg["note_type"], note_text=reg["note_text"],
                evaluated_at=datetime.now()
            )
            results.append({"rule_id": rule_id, "status": "NOTED"})
        elif passes:
            event = ComplianceRulePassed(
                application_id=app_id, session_id=self.session_id,
                rule_id=rule_id, rule_name=reg["name"], rule_version=reg["version"],
                evidence_hash=evidence_hash, evaluation_notes="Rule criteria met.",
                evaluated_at=datetime.now()
            )
            results.append({"rule_id": rule_id, "status": "PASSED"})
        else:
            event = ComplianceRuleFailed(
                application_id=app_id, session_id=self.session_id,
                rule_id=rule_id, rule_name=reg["name"], rule_version=reg["version"],
                failure_reason=reg["failure_reason"],
                is_hard_block=reg.get("is_hard_block", False),
                remediation_available=bool(reg.get("remediation")),
                remediation_description=reg.get("remediation"),
                evidence_hash=evidence_hash, evaluated_at=datetime.now()
            )
            results.append({"rule_id": rule_id, "status": "FAILED"})
            if reg.get("is_hard_block"):
                state["has_hard_block"] = True
                state["block_rule_id"] = rule_id

        if event:
            await self._append_with_retry(f"compliance-{app_id}", [event.to_store_dict()])

        ms = int((time.time() - t) * 1000)
        node_name = f"evaluate_{rule_id.lower().replace('-', '_')}"
        await self._record_node_execution(node_name, ["company_profile"], ["rule_result"], ms, correlation_id=self.session_id)
        
        return {**state, "rule_results": results}

    async def _node_write_output(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        has_block = state.get("has_hard_block", False)
        
        results = state.get("rule_results", [])
        rules_passed = len([r for r in results if r["status"] == "PASSED"])
        rules_failed = len([r for r in results if r["status"] == "FAILED"])
        rules_noted  = len([r for r in results if r["status"] == "NOTED"])

        completed_event = ComplianceCheckCompleted(
            application_id=app_id,
            session_id=self.session_id,
            rules_evaluated=len(results),
            rules_passed=rules_passed,
            rules_failed=rules_failed,
            rules_noted=rules_noted,
            has_hard_block=has_block,
            overall_verdict=ComplianceVerdict.BLOCKED if has_block else ComplianceVerdict.CLEAR,
            completed_at=datetime.now()
        ).to_store_dict()
        positions = await self._append_with_retry(f"compliance-{app_id}", [completed_event])

        output_events = [{"stream_id": f"compliance-{app_id}", "event_type": "ComplianceCheckCompleted", "stream_position": positions[0] if positions else -1}]

        if has_block:
            ev = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=[REGULATIONS.get(state.get("block_rule_id"), {}).get("failure_reason", "Compliance block")],
                declined_by=self.session_id,
                adverse_action_notice_required=True,
                adverse_action_codes=[state.get("block_rule_id", "REG-UNKNOWN")],
                declined_at=datetime.now()
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [ev])
            output_events.append({"stream_id": f"loan-{app_id}", "event_type": "ApplicationDeclined", "stream_position": -1})
            next_agent = None
        else:
            decision_event = DecisionRequested(
                application_id=app_id,
                requested_at=datetime.now(),
                all_analyses_complete=True,
                triggered_by_event_id=self.session_id
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [decision_event])
            output_events.append({"stream_id": f"loan-{app_id}", "event_type": "DecisionRequested", "stream_position": -1})
            next_agent = "decision_orchestrator"
            
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("write_output", ["rule_results", "has_hard_block"], ["output_events"], ms, correlation_id=self.session_id)
        return {**state, "output_events": output_events, "next_agent": next_agent}
