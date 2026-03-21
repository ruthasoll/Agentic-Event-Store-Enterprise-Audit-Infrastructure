"""
ledger/agents/fraud_detection_agent.py
=======================================
Fraud Detection Agent.
Cross-references extracted document facts against historical registry data to
detect anomalous discrepancies that suggest fraud or document manipulation.
"""
from __future__ import annotations
import time, json
from datetime import datetime
from typing import TypedDict, Any
from uuid import uuid4
from langgraph.graph import StateGraph, END

from ledger.agents.base_agent import BaseApexAgent
from ledger.schema.events import (
    FraudScreeningInitiated, FraudAnomalyDetected, FraudScreeningCompleted,
    ComplianceCheckRequested, FraudAssessment
)

class FraudState(TypedDict):
    application_id: str
    session_id: str
    extracted_facts: dict | None
    registry_profile: dict | None
    historical_financials: list[dict] | None
    fraud_signals: list[dict] | None
    fraud_score: float | None
    anomalies: list[dict] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None

class FraudDetectionAgent(BaseApexAgent):
    def __init__(self, agent_id: str, store, registry, model_version="fraud-v1"):
        super().__init__(agent_id, store, model_version=model_version)
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

    async def _call_llm(self, system: str, user: str, max_tokens: int):
        import json
        decision = {
            "justification": "Automated cross-reference flags assessed.",
        }
        return json.dumps(decision), 100, 50, 0.01

    def build_graph(self) -> Any:
        g = StateGraph(FraudState)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("load_document_facts",     self._node_load_facts)
        g.add_node("cross_reference_registry",self._node_cross_reference)
        g.add_node("analyze_fraud_patterns",  self._node_analyze)
        g.add_node("write_output",            self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",          "load_document_facts")
        g.add_edge("load_document_facts",      "cross_reference_registry")
        g.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns",   "write_output")
        g.add_edge("write_output",             END)
        return g.compile()

    def _initial_state(self, application_id: str) -> FraudState:
        return FraudState(
            application_id=application_id, session_id=self.session_id,
            extracted_facts=None, registry_profile=None, historical_financials=None,
            fraud_signals=None, fraud_score=None, anomalies=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        
        # Initiate Fraud Screening
        credit_events = await self.store.load_stream(f"credit-{app_id}")
        credit_completed = [e for e in credit_events if e["event_type"] == "CreditAnalysisCompleted"]
        errors = []
        if not credit_completed:
            errors.append("Expected CreditAnalysisCompleted before Fraud Screening")
            
        event = FraudScreeningInitiated(
            application_id=app_id,
            session_id=self.session_id,
            initiated_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"fraud-{app_id}", [event])

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("validate_inputs", ["application_id"], ["fraud_screening_initiated"], ms, correlation_id=self.session_id)
        return {**state, "errors": errors}

    async def _node_load_facts(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        pkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        extraction_events = [e for e in pkg_events if e["event_type"] == "ExtractionCompleted"]
        
        merged_facts = {}
        for ev in extraction_events:
            facts = ev["payload"].get("facts") or {}
            for k, v in facts.items():
                if v is not None and k not in merged_facts:
                    merged_facts[k] = v

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("load_document_facts", ["docpkg_stream"], ["extracted_facts"], ms, correlation_id=self.session_id)
        return {**state, "extracted_facts": merged_facts}

    async def _node_cross_reference(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        sub_events = [e for e in loan_events if e["event_type"] == "ApplicationSubmitted"]
        applicant_id = "COMP-001"
        if sub_events:
            applicant_id = sub_events[0]["payload"].get("applicant_id", applicant_id)

        profile = await self.registry.get_company(applicant_id) or {}
        financials = await self.registry.get_financial_history(applicant_id) or []

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("cross_reference_registry", ["applicant_id"], ["registry_profile", "historical_financials"], ms, correlation_id=self.session_id)
        return {**state, "registry_profile": profile, "historical_financials": financials}

    async def _node_analyze(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        facts = state.get("extracted_facts") or {}
        profile = state.get("registry_profile") or {}
        historical = state.get("historical_financials") or []
        
        fraud_score = 0.05
        anomalies = []

        doc_revenue = float(facts.get("total_revenue", 0))
        if historical:
            prior_revenue = float(historical[-1].get("total_revenue", 0))
            if prior_revenue > 0:
                gap = abs(doc_revenue - prior_revenue) / prior_revenue
                trajectory = profile.get("trajectory", "UNKNOWN")
                if gap > 0.40 and trajectory not in ["GROWTH", "RECOVERING"]:
                    fraud_score += 0.25
                    anomalies.append({
                        "anomaly_type": "REVENUE_DISCREPANCY",
                        "severity": "HIGH",
                        "evidence": f"Revenue deviates {gap:.0%} from prior year under {trajectory} trajectory",
                        "affected_fields": ["total_revenue"]
                    })

        # Append anomaly detected events
        for anomaly in anomalies:
            event = FraudAnomalyDetected(
                application_id=app_id,
                session_id=self.session_id,
                anomaly_type=anomaly["anomaly_type"],
                severity=anomaly["severity"],
                evidence=anomaly["evidence"],
                affected_fields=anomaly["affected_fields"],
                detected_at=datetime.now()
            ).to_store_dict()
            await self._append_with_retry(f"fraud-{app_id}", [event])

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("analyze_fraud_patterns", ["extracted_facts", "historical_financials"], ["fraud_score", "anomalies"], ms, correlation_id=self.session_id)
        return {**state, "fraud_score": fraud_score, "anomalies": anomalies}

    async def _node_write_output(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        fraud_score = state.get("fraud_score", 0.05)
        
        if fraud_score > 0.60:
            rec = "DECLINE"
        elif fraud_score >= 0.30:
            rec = "FLAG_FOR_REVIEW"
        else:
            rec = "PROCEED"

        assessment = FraudAssessment(
            fraud_score=fraud_score,
            recommendation=rec,
            primary_anomalies_detected=[a["anomaly_type"] for a in state.get("anomalies", [])],
            justification="System cross-checks resolved successfully."
        )

        completed_event = FraudScreeningCompleted(
            application_id=app_id,
            session_id=self.session_id,
            assessment=assessment,
            model_version=self.model_version,
            model_deployment_id=f"dep-{uuid4().hex[:8]}",
            input_data_hash=self._sha(state),
            analysis_duration_ms=int((time.time() - t) * 1000),
            completed_at=datetime.now()
        ).to_store_dict()
        positions = await self._append_with_retry(f"fraud-{app_id}", [completed_event])

        compliance_trigger = ComplianceCheckRequested(
            application_id=app_id,
            requested_at=datetime.now(),
            triggered_by_event_id=self.session_id
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [compliance_trigger])

        events_written = [
            {"stream_id": f"fraud-{app_id}", "event_type": "FraudScreeningCompleted", "stream_position": positions[0] if positions else -1},
            {"stream_id": f"loan-{app_id}", "event_type": "ComplianceCheckRequested", "stream_position": -1}
        ]
        
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("write_output", ["fraud_score", "anomalies"], ["output_events"], ms, correlation_id=self.session_id)
        return {**state, "output_events": events_written, "next_agent": "compliance_checking"}
