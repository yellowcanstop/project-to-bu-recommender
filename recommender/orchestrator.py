import azure.durable_functions as df
import json
import logging

logger = logging.getLogger(__name__)

'''
Orchestrates:
Activity 1: Download & Filter BCI (per BU)
Activity 2: Download Non-BCI
Activity 3: Deduplication (embeddings + AI Search)
Activity 4: Human Approval of Duplicates (wait for external event)
Activity 5: Extract Signals (LLM per project lead)
Activity 6: Domain Agents + Synthesizer (per project lead)
Activity 7: Store Results
'''

name = "recommender_orchestrator"
main = df.Blueprint()

@main.function_name(name)
@main.orchestration_trigger(context_name="context", orchestration=name)
def recommender_orchestrator(context: df.DurableOrchestrationContext):
    input_data = context.get_input()

    # ──────────────────────────────────────────────
    # PHASE 1: Download BCI + filter per BU
    # ──────────────────────────────────────────────
    # Returns: {"makna_setia": [id1, id2], "ppch": [id3, id4], ...}
    # Plus the full filtered dataframe as serialized JSON rows
    print(">>> Orchestrator started, calling filter_bci...")

    filter_result = yield context.call_activity("filter_bci", input_data)
    filtered_leads = filter_result["filtered_leads"]  # list of project lead dicts
    bu_assignments = filter_result["bu_assignments"]  # {bu_name: [project_ids]}

    print(f">>> Filtered BCI leads count: {len(filtered_leads)}")
    print(f">>> BU Assignments: {bu_assignments}")
    

    # ──────────────────────────────────────────────
    # PHASE 2+3: Download Non-BCI + Deduplication
    # ──────────────────────────────────────────────
    dedup_result = yield context.call_activity("deduplicate", {
        "filtered_bci_leads": filtered_leads,
    })
    duplicate_candidates = dedup_result["duplicates"]  # list of {bci_id, non_bci_id, similarity, details}

    print(f">>> Found {len(duplicate_candidates)} duplicate candidates between BCI and Non-BCI leads.")
    print(f">>> Duplicate candidates: {duplicate_candidates}")

    return {"status": "complete", "duplicate_candidates": duplicate_candidates}

    '''
    # ──────────────────────────────────────────────
    # PHASE 4: Human approval (wait for external event)
    # ──────────────────────────────────────────────
    if len(duplicate_candidates) > 0:
        # Send duplicates to UI — store in blob for the frontend to fetch
        yield context.call_activity("store_duplicates_for_review", {
            "instance_id": context.instance_id,
            "duplicates": duplicate_candidates,
        })

        # Wait for human response (with timeout)
        import datetime
        approval = yield context.wait_for_external_event("duplicate_approval")
        # approval = {"removed_ids": ["X006116", ...]}
        removed_ids = approval.get("removed_ids", [])
    else:
        removed_ids = []

    # Filter out removed non-BCI duplicates (they're confirmed duplicates of BCI)
    # The remaining non-BCI leads are set aside for now

    # ──────────────────────────────────────────────
    # PHASE 5+6: For each filtered BCI lead, extract signals + run agents + synthesize
    # ──────────────────────────────────────────────
    # Fan out: process each project lead in parallel
    parallel_tasks = []
    for lead in filtered_leads:
        task = context.call_activity("process_single_lead", {
            "lead": lead,
            "bu_assignments": bu_assignments,
        })
        parallel_tasks.append(task)

    all_results = yield context.task_all(parallel_tasks)

    # ──────────────────────────────────────────────
    # PHASE 7: Store final results
    # ──────────────────────────────────────────────
    yield context.call_activity("store_results", {
        "recommendations": all_results,
        "bu_assignments": bu_assignments,
    })

    return {"status": "complete", "leads_processed": len(filtered_leads)}
    '''
