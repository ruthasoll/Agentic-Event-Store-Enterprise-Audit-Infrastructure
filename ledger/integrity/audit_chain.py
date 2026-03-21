import hashlib
import json

def calculate_rolling_hash(events: list[dict]) -> str:
    """Calculates a rolling SHA-256 hash over an entire sequence of events."""
    current_hash = "0" * 64
    for event in events:
        data = {
            "stream_position": event.get("stream_position"),
            "event_type": event.get("event_type"),
            "payload": event.get("payload", {}),
        }
        raw_str = current_hash + json.dumps(data, sort_keys=True)
        current_hash = hashlib.sha256(raw_str.encode()).hexdigest()
    return current_hash

def verify_chain(events: list[dict], expected_final_hash: str | None = None) -> bool:
    """
    Verifies the rolling SHA-256 integrity of an event stream.
    If expected_final_hash is provided, verifies that it matches.
    If events store an expected `integrity_hash` in metadata, verifies the chain up to that point.
    """
    if not events:
        return True
        
    current_hash = "0" * 64
    for event in events:
        data = {
            "stream_position": event.get("stream_position"),
            "event_type": event.get("event_type"),
            "payload": event.get("payload", {}),
        }
        raw_str = current_hash + json.dumps(data, sort_keys=True)
        current_hash = hashlib.sha256(raw_str.encode()).hexdigest()
        
        meta = event.get("metadata", {})
        if "integrity_hash" in meta and meta["integrity_hash"] != current_hash:
            return False
            
    if expected_final_hash and current_hash != expected_final_hash:
        return False
        
    return True
