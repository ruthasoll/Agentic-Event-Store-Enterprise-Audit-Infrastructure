def reconstruct_agent_memory(events: list[dict]) -> str:
    """
    Gas Town pattern: Rebuilds an agent's working memory specifically for context
    re-injection after a crash/restart. Emits a plain English summary of events.
    """
    lines = []
    lines.append("=== Agent Memory Reconstruction ===")
    
    for e in events:
        et = e["event_type"]
        p = e.get("payload", {})
        
        if et == "AgentSessionStarted":
            app_id = p.get("application_id", "Unknown")
            lines.append(f"Started session for application {app_id}.")
        elif et == "AgentContextLoaded":
            lines.append(f"Loaded context from {p.get('context_source')}.")
        elif et == "AgentThoughtRecorded":
            lines.append(f"Observation: {p.get('observation')}")
        elif et == "CreditAnalysisCompleted":
            lines.append(f"Action: Completed credit analysis. Set recommendation based on score {p.get('confidence_score')}")
        elif et == "FraudScreeningCompleted":
            lines.append(f"Action: Completed fraud screening. Found {p.get('anomalies_found')} anomalies.")
        elif et == "ComplianceCheckCompleted":
            lines.append(f"Action: Completed compliance checks. Result: {p.get('overall_verdict')}")
        elif et == "DecisionGenerated":
            lines.append(f"Action: Generated decision to {p.get('recommendation')} with confidence {p.get('confidence_score')}.")
            
    return "\n".join(lines)
