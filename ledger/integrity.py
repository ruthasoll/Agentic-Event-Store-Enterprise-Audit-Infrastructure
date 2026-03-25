import hashlib
import json
from typing import Any

def calculate_event_hash(
    previous_hash: str | None,
    stream_id: str,
    stream_position: int,
    event_type: str,
    event_version: int,
    payload: dict[str, Any],
    metadata: dict[str, Any]
) -> str:
    """
    Calculates a SHA256 hash for an event, chaining it to the previous hash.
    Uses deterministic JSON serialization for payload and metadata.
    """
    # Deterministic JSON serialization
    p_json = json.dumps(payload, sort_keys=True)
    m_json = json.dumps(metadata, sort_keys=True)
    
    # Create the data string to hash
    # We use a separator to avoid ambiguity
    data = f"{previous_hash or '0'}|{stream_id}|{stream_position}|{event_type}|{event_version}|{p_json}|{m_json}"
    
    return hashlib.sha256(data.encode('utf-8')).hexdigest()

def verify_hash_chain(events: list[dict[str, Any]]) -> bool:
    """
    Verifies that the integrity_hash chain is valid for a sequence of events.
    Each event must have 'integrity_hash' and 'previous_hash' keys.
    """
    prev_hash = None
    for event in events:
        if event.get('previous_hash') != prev_hash:
            return False
        
        expected_hash = calculate_event_hash(
            prev_hash,
            event['stream_id'],
            event['stream_position'],
            event['event_type'],
            event['event_version'],
            event['payload'],
            event['metadata']
        )
        
        if event.get('integrity_hash') != expected_hash:
            return False
        
        prev_hash = expected_hash
        
    return True
