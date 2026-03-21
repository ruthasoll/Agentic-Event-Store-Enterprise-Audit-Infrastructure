# The Ledger — Weeks 9-10 Starter Code

## Quick Start
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start PostgreSQL
docker run -d -e POSTGRES_PASSWORD=apex -e POSTGRES_DB=apex_ledger -p 5432:5432 postgres:16

# 3. Set environment
cp .env.example .env
# Edit .env — add your ANTHROPIC_API_KEY

# 4. Generate all data (companies + documents + seed events → DB)
python datagen/generate_all.py --db-url postgresql://postgres:apex@localhost/apex_ledger

# 5. Validate schema (no DB needed)
python datagen/generate_all.py --skip-db --skip-docs --validate-only

# 6. Run Phase 0 tests (must pass before starting Phase 1)
pytest tests/test_schema_and_generator.py -v

# 7. Begin Phase 1: implement EventStore
# Edit: ledger/event_store.py
# Test: pytest tests/test_event_store.py -v
```

## What Works Out of the Box
- Full event schema (45 event types) — `ledger/schema/events.py`
- Complete data generator (GAAP PDFs, Excel, CSV, 1,200+ seed events)
- Event simulator (all 5 agent pipelines, deterministic)
- Schema validator (validates all events against EVENT_REGISTRY)

## Phase 2: Agents, Projections & MCP
We have completed Phase 2, which focuses on the agentic infrastructure and real-time views.

| Component | File | Status |
|-----------|------|-------|
| DocumentProcessingAgent | `ledger/agents/document_processing_agent.py` | [x] |
| CreditAnalysisAgent | `ledger/agents/credit_analysis_agent.py` | [x] |
| FraudDetectionAgent | `ledger/agents/fraud_detection_agent.py` | [x] |
| ComplianceAgent | `ledger/agents/compliance_agent.py` | [x] |
| DecisionOrchestratorAgent | `ledger/agents/decision_orchestrator_agent.py` | [x] |
| ProjectionDaemon | `ledger/projections/daemon.py` | [x] |
| MCP Server | `ledger/mcp/server.py` | [x] |

## Gate Tests
```bash
# Phase 0 & 1: Schema & Event Store
pytest tests/test_schema_and_generator.py -v
pytest tests/test_event_store.py -v

# Phase 2: Narrative Scenarios
# These tests verify the full E2E flow across all 5 agents.
pytest tests/test_narratives.py -v
```

### Running Narrative Tests
The narrative tests cover real-world scenarios:
- **NARR-01**: Concurrent OCC collision handling.
- **NARR-02**: Document extraction failure (ebitda missing).
- **NARR-03**: Agent crash & recovery (replay from event stream).

Run them specifically with:
```bash
pytest tests/test_narratives.py -k test_narr01
pytest tests/test_narratives.py -k test_narr02
pytest tests/test_narratives.py -k test_narr03
```
