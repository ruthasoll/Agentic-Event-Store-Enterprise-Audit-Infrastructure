# DESIGN.md

## 1. Aggregate Boundary Justification
**Question:** Why is `ComplianceRecord` a separate aggregate from `LoanApplication`? What would couple if you merged them? Trace the coupling to a specific failure mode under concurrent write scenarios.
**Answer:** The `ComplianceRecord` tracks all regulatory checks and rule evaluations (e.g., passing KYC, AML rules), while the `LoanApplication` tracks the core application lifecycle (submitted -> analysis -> decision). 
If merged, every compliance check (which might happen via an asynchronous ComplianceAgent checking 10 distinct rules) would require appending a `ComplianceRulePassed` event to the `LoanApplication` stream. Concurrently, a `CreditAnalysisAgent` and `FraudDetectionAgent` might be appending to the exact same stream.
**Failure mode:** Under concurrent scenarios, if the ComplianceAgent evaluates rule A and tries to append `ComplianceRulePassed(rule_id=A)`, while the FraudAgent tries to append `FraudScreeningCompleted`, they both read stream `version 5`. Whichever commits first advances the stream to `version 6`. The other receives an `OptimisticConcurrencyError`, reloads the stream, and retries. With high rule density, this creates severe write contention on the `loan-{id}` stream, blowing through retry budgets and increasing latency dramatically. By separating them, the `ComplianceRecord` provides an independent consistency boundary; compliance rules can be appended without locking the core loan application state.

## 2. Projection Strategy
For each projection, justify:
- **ApplicationSummary:** *Async*. A 500ms SLO commitment. Real-time synchronous updates (Inline) are unnecessary here because humans reviewing the UI can tolerate sub-second delays, and inline projections would artificially inflate the write latency for high-throughput AI agents saving decisions.
- **AgentPerformanceLedger:** *Async*. A 2-second SLO commitment. Analytics and performance calculations run entirely offline relative to the loan decision path and only require eventual consistency for reporting workloads.
- **ComplianceAuditView:** *Async*. A 2-second SLO commitment. Regulatory queries (`get_state_at(timestamp)`) are typically post-hoc analytical tasks rather than high-throughput operational dependencies.

**Temporal query snapshot strategy for ComplianceAuditView:** 
We will implement an *event-count trigger* snapshot strategy (e.g., snapshot every 100 events) combined with a *time-based bounds* invalidation logic. 
*Snapshot Invalidation Logic:* If the core `ComplianceRecordAggregate._apply()` logic changes (e.g., adding a new computed compliance flag), we increment the `snapshot_schema_version`. When loading a snapshot for the temporal query, if `snapshot.version != CURRENT_VERSION`, the read logic silently discards the snapshot and falls back to a full stream replay to regenerate the exact state up to `target_timestamp`.

## 3. Concurrency Analysis
**Scenario:** 100 concurrent applications, 4 agents each. Peak load.
**Analysis:** If we maintain four separate aggregates per application (LoanApplication, AgentSession, ComplianceRecord, FraudScreening), the collision rate drops exponentially because agents primarily write to their *own* AgentSession or distinct domain streams. However, all 4 agents eventually converge on modifying the `LoanApplication` state (e.g., triggering `DecisionGenerated` or `CreditAnalysisCompleted`).
With 100 loans/min, that's 400 agents. Assuming they converge on their respective loan streams, we expect roughly 10-15 `OptimisticConcurrencyError` instances per minute globally across all `loan-{id}` streams whenever agents finish tasks with overlapping sub-second completion times.
**Retry Strategy:**
- **Wait Policy:** Exponential backoff with jitter (e.g., `delay = min(100ms * 2^attempt + jitter, 1000ms)`).
- **Retry Budget:** Maximum 5 retries. 
- **Failure:** After 5 failed retries, the operation fails and the command handler returns the failure to the caller (e.g., the AI agent). The LLM will receive a structured HTTP/MCP error advising it to fetch the latest state and rethink.

## 4. Upcasting Inference Decisions
**Scenario:** Inferring `model_version`, `confidence_score`, `regulatory_basis`.
- `model_version` -> Inferred based on `recorded_at`. Error rate: ~2% at boundary deployments. An incorrect inference here associates a decision with a slightly older/newer model, which could misattribute model drift metrics, but does not invalidate the business action. Since we must preserve it, predicting "legacy-pre-2026" or mapping by date is necessary.
- `confidence_score` -> Set to `null`. *Why not infer?* If we calculate an average confidence (e.g., 0.8) and insert it, an auditor will assume the model generated 0.8 confidence. This is data fabrication and constitutes a severe compliance violation. Null correctly represents "the model did not emit a confidence score at the time".
- `regulatory_basis` -> Set to `[]`. Reconstructing complex regulation rules retroactively poses a massive risk of hallucinating compliance where none existed.

## 5. EventStoreDB Comparison
Mapping PostgreSQL schema concepts to EventStoreDB:
- **Streams:** The same. EventStoreDB natively uses stream IDs (`loan-123`).
- **events table:** EventStoreDB handles this internally; events are appended natively without defining JSONB schemas.
- **event_version:** In EventStoreDB, metadata headers accompany events, often driving the upcaster logic in application space.
- **load_all():** EventStoreDB provides the `$all` stream. Reading `$all` is highly optimized via gRPC persistent subscriptions or catch-up subscriptions.
- **ProjectionDaemon (Async):** EventStoreDB gives us *Persistent Subscriptions* built explicitly to track checkpoints, handle consumer groups, and implement reliable retries without us needing to manage `projection_checkpoints` explicitly in Postgres.

*What EventStoreDB gives us:* We must work hard to poll PostgreSQL efficiently (e.g., using `LISTEN/NOTIFY` plus table scans and locking `projection_checkpoints`). EventStoreDB manages cursor tracking, catch-up subscriptions, and native fan-out completely out-of-the-box.

## 6. What You Would Do Differently
**The single most significant architectural decision to reconsider:** 
If I had another day, I would rewrite the *Outbox implementation and Projection Daemon pooling*. Currently, building an async polling loop directly inside the application ties the compute bounds of the application server to the projection delivery rate. Under extreme enterprise loads, `LISTEN/NOTIFY` plus application-level polling can choke the async event loops or lead to memory bottlenecks when processing large batches.
I would reconsider deploying a dedicated stream processor tool (like a Kafka-connector reading the Postgres WAL via Debezium or using logical replication slots). By removing the daemon out of the Python memory space entirely and relying on Debezium, we eliminate application-layer polling overhead, achieve true exactly-once potential (or very robust at-least-once), and guarantee that the projection state represents the exact transaction log of Postgres without our own daemon fault tolerance heuristics failing.
