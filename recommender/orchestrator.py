import azure.durable_functions as df
import json

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

def recommender_orchestrator_function(ctx: df.DurableOrchestrationContext):
    input_data = ctx.get_input()

    # ──────────────────────────────────────────────
    # PHASE 1: Download BCI + filter per BU
    # ──────────────────────────────────────────────
    # Returns: {"makna_setia": [id1, id2], "ajiya_metal": [id3, id4], ...}
    # Plus the full filtered dataframe as serialized JSON rows
    filter_result = yield ctx.call_activity("filter_bci", input_data)
    filtered_leads = filter_result["filtered_leads"]  # list of project lead dicts
    bu_assignments = filter_result["bu_assignments"]  # {bu_name: [project_ids]}

    return {"status": "complete", "filtered_leads": filtered_leads, "bu_assignments": bu_assignments}

    '''
    # ──────────────────────────────────────────────
    # PHASE 2+3: Download Non-BCI + Deduplication
    # ──────────────────────────────────────────────
    dedup_result = yield ctx.call_activity("deduplicate", {
        "filtered_bci_leads": filtered_leads,
    })
    duplicate_candidates = dedup_result["duplicates"]  # list of {bci_id, non_bci_id, similarity, details}

    # ──────────────────────────────────────────────
    # PHASE 4: Human approval (wait for external event)
    # ──────────────────────────────────────────────
    if len(duplicate_candidates) > 0:
        # Send duplicates to UI — store in blob for the frontend to fetch
        yield ctx.call_activity("store_duplicates_for_review", {
            "instance_id": ctx.instance_id,
            "duplicates": duplicate_candidates,
        })

        # Wait for human response (with timeout)
        import datetime
        approval = yield ctx.wait_for_external_event("duplicate_approval")
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
        task = ctx.call_activity("process_single_lead", {
            "lead": lead,
            "bu_assignments": bu_assignments,
        })
        parallel_tasks.append(task)

    all_results = yield ctx.task_all(parallel_tasks)

    # ──────────────────────────────────────────────
    # PHASE 7: Store final results
    # ──────────────────────────────────────────────
    yield ctx.call_activity("store_results", {
        "recommendations": all_results,
        "bu_assignments": bu_assignments,
    })

    return {"status": "complete", "leads_processed": len(filtered_leads)}
    '''


main = df.Blueprint()
main.orchestration_trigger("recommender_orchestrator")(recommender_orchestrator_function)