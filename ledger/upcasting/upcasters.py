def upcast_credit_analysis_v1_to_v2(event: dict) -> dict:
    """
    Addresses rule: v1 lacked model_version and confidence_score.
    Map model_version to 'legacy-pre-2026' and confidence_score to None to prevent integrity hallucination.
    """
    p = event["payload"]
    p["model_version"] = "legacy-pre-2026"
    p["confidence_score"] = None
    return event


def upcast_decision_generated_v1_to_v2(event: dict) -> dict:
    """
    Addresses rule: v1 lacked regulatory_basis. Map to empty list [] to prevent false compliance assertions.
    """
    p = event["payload"]
    p["regulatory_basis"] = []
    return event


def setup_upcasters(registry):
    """Binds all upcasters to the registry."""
    registry.register("CreditAnalysisCompleted", 1, upcast_credit_analysis_v1_to_v2)
    registry.register("DecisionGenerated", 1, upcast_decision_generated_v1_to_v2)
