class UpcasterRegistry:
    def __init__(self):
        self._upcasters = {}

    def register(self, event_type: str, from_version: int, func):
        """Registers a function to upcast an event from `from_version` to `from_version + 1`"""
        self._upcasters[(event_type, from_version)] = func

    def upcast(self, event: dict) -> dict:
        """Applies all registered upcasters sequentially until the event reaches the latest version."""
        et = event["event_type"]
        ver = event.get("event_version", 1)
        
        # Sequentially apply transformations to evolve the schema in-memory
        while (et, ver) in self._upcasters:
            event = self._upcasters[(et, ver)](event)
            ver += 1
            event["event_version"] = ver
            
        return event
