# DOMAIN_NOTES.md

## 1. EDA vs. ES Distinction
Capturing event-like data via component callbacks (like LangChain traces) represents an Event-Driven Architecture (EDA). In EDA, the events act as notifications—"something happened"—and the system's primary state is still stored as mutable records in a traditional database. If those events are lost or the message bus crashes, the system state cannot be completely reconstructed from scratch because the logs are not the definitive source of truth.

**If redesigned using The Ledger:**
The architecture would shift to Event Sourcing. The event stream itself would *become* the definitive database. The primary write operation would no longer update a row in a Postgres table, but rather append an immutable record to an `events` table (e.g. `AgentContextLoaded`, `CreditAnalysisCompleted`). 
**What we gain:** We gain perfect auditability ("mathematically guaranteed"). We can rebuild the system's exact state at any point in history by replaying the stream up to a specific global position. Additionally, it natively prevents the lost-memory issue (the Gas Town pattern) since agents recover their contexts by replaying their event streams.

## 2. The Aggregate Question
**Scenario Aggregates:** `LoanApplication`, `AgentSession`, `ComplianceRecord`, `AuditLedger`.
**Alternative Considered and Rejected:** Merging `ComplianceRecord` into the `LoanApplication` aggregate.
**Coupling Problem Prevented by Separation:** If compliance checks and loan application state changes occurred in the same stream, every single compliance validation check would lock the `LoanApplication` stream. Under peak load, an orchestrator agent might be updating the loan status while a parallel compliance agent is verifying 10 different regulatory rules. If they share the same aggregate, they will repeatedly trigger `OptimisticConcurrencyError` collisions on the single `loan-{id}` stream version. Separating `ComplianceRecord` ensures compliance-related writes do not contend for the optimistic lock of the core application state, maintaining high-throughput parallel processing. They become independent consistency boundaries.

## 3. Concurrency in Practice
Two AI agents (Agent A and Agent B) independently compute a credit decision for the same `LoanApplication` and simultaneously attempt:
`append_events(stream_id, expected_version=3)`.

**Exact Sequence of Operations:**
1. Both operations start a database transaction and attempt to insert into the `events` table, constrained by a unique index on `(stream_id, stream_position)`. Both try to insert `stream_position=4`.
2. Agent A's transaction reaches the database insert first. The insert succeeds, and `event_streams.current_version` is updated to 4. Agent A commits and returns the new version (4).
3. Agent B's transaction attempts to insert at `stream_position=4`. The database raises a Unique Constraint Violation because `(stream_id='loan-...', stream_position=4)` already exists.
4. The `EventStore` catches this Postgres exception and translates it into an `OptimisticConcurrencyError`.

**What the Losing Agent Receives and Must Do:**
Agent B receives an `OptimisticConcurrencyError`. Agent B must reload the `LoanApplication` aggregate from the event store (which will now be at version 4), re-run its business logic to see if its decision is still valid given Agent A's new event, and then retry appending its event with the new `expected_version=4`.

## 4. Projection Lag and Its Consequences
When the projection lag is 200ms, queries against the `ApplicationSummary` view immediately post-write will return stale data (e.g., the old credit limit).

**What the system does:** The backend returns the state exactly as it exists in the projection at the time of the query without blocking or loading the aggregate, since CQRS read models strictly query projections. 
**How to communicate to the UI (Read-After-Write Consistency):** 
1. **Client-side Optimistic Update:** The UI applies the new limit immediately upon a successful response from the command endpoint. 
2. **Version check or Long Polling:** The UI can poll the projection endpoint until the returned projection version/timestamp matches or exceeds the known write version timestamp returned by the command. 
3. **Explicit Notification:** Via Redis Streams or SSE, the UI can listen for a projection-updated notification. Until then, the UI displays an indicator ("Processing Updates...") next to the credit limit field.

## 5. The Upcasting Scenario
**The Upcaster:**
```python
@registry.register("CreditDecisionMade", from_version=1)
def upcast_credit_decision_v1_to_v2(payload: dict) -> dict:
    return {
        "application_id": payload["application_id"],
        "decision": payload["decision"],
        "reason": payload["reason"],
        "model_version": "legacy-pre-2026", # Inference strategy
        "confidence_score": None, # Genuinely unknown, do not fabricate
        "regulatory_basis": [] # Could infer based on active rules at timestamp, but safest default is empty.
    }
```

**Inference Strategy:** We infer `model_version = "legacy-pre-2026"` because for compliance reasons we must know roughly what execution pattern created this, even if the exact string wasn't logged. We set `confidence_score = None` because synthesizing a probability where none was calculated introduces objectively false data into a regulatory audit trail, which is worse than missing data. `regulatory_basis` is kept empty (or resolved securely if historical DB mappings were kept at the exact `recorded_at` date), since guessing a regulatory reason is also technically hazardous.

## 6. The Marten Async Daemon Parallel
Marten 7.0 supports distributed background projection nodes. In Python, achieving resilient, multi-node distributed projection processing requires a distributed locking primitive and leader election.

**Python Implementation:**
We would use Redis or Postgres advisory locks (`pg_try_advisory_lock` in Postgres) to ensure that only one instance of the daemon handles a specific projection at a time. The daemon nodes would compete to acquire a named lock for each projection (e.g., `lock_projection_ApplicationSummary`). 
**Failure Mode Guarded Against:** This guards against the split-brain scenario where two running daemons try to read from the exact same global checkpoint and apply projection updates simultaneously. Dual processing would lead to duplicate records, duplicate ID errors, or race conditions updating the `projection_checkpoints` table, unless projection inserts are purely idempotent upserts (but even then, maintaining ordered checkpoint commits across distributed writers leads to severe lock contention and rollback rates). By utilizing a locking mechanism, only one daemon actively processes the global `events` table for a specific named projection.
