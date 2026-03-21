"""
scripts/run_pipeline.py — Process one application through all agents.
Usage: python scripts/run_pipeline.py --application APEX-0007 [--phase all|document|credit|fraud|compliance|decision]
"""
import argparse, asyncio, os, sys
import asyncpg
from pathlib import Path; sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv()

from ledger.event_store import EventStore
from ledger.registry.client import ApplicantRegistryClient
from ledger.agents.document_processing_agent import DocumentProcessingAgent
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--application", required=True)
    p.add_argument("--phase", default="all")
    p.add_argument("--db-url", default=os.environ.get("DATABASE_URL","postgresql://localhost/the_ledger"))
    args = p.parse_args()

    # 1. Initialize dependencies
    store = EventStore(args.db_url)
    await store.connect()
    registry = ApplicantRegistryClient(store._pool)

    print(f"Processing {args.application} through phase: {args.phase}")

    if args.phase in ["document", "all"]:
        print("--- Running DocumentProcessingAgent ---")
        doc_agent = DocumentProcessingAgent("doc-agent-1", store)
        doc_graph = doc_agent.build_graph()
        try:
            initial = doc_agent._initial_state(args.application)
            await doc_graph.ainvoke(initial)
            print("Document processing complete! Verified Output written to Events.")
        except Exception as e:
            print(f"Document processing failed: {e}")

    if args.phase in ["credit", "all"]:
        print("\n--- Running CreditAnalysisAgent ---")
        # Instantiate CreditAgent with registry attached
        credit_agent = CreditAnalysisAgent("credit-agent-1", store, registry)
        credit_graph = credit_agent.build_graph()
        
        # Test requires Credit Analysis execution so let's start the session properly
        await credit_agent.start_session(args.application, "System Queue", 0, "corr-1")
        
        try:
            initial = credit_agent._initial_state(args.application)
            res = await credit_graph.ainvoke(initial)
            print("Credit Analysis complete! Verifying Outputs.")
            for e in res.get("output_events", []):
                print(f" > Event Produced: {e}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Credit analysis failed: {e}")

    await store.close()

if __name__ == "__main__":
    asyncio.run(main())
