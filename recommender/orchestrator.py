import re

import azure.durable_functions as df
import json
import logging

from datetime import timedelta

logger = logging.getLogger(__name__)


name = "recommender_orchestrator"
main = df.Blueprint()

@main.function_name(name)
@main.orchestration_trigger(context_name="context", orchestration=name)
def recommender_orchestrator(context: df.DurableOrchestrationContext):
    batch_size = 5

    input_data = context.get_input()
    if isinstance(input_data, str):
        input_data = json.loads(input_data)

    # file names are session-specific
    bci_name = input_data.get("bci_blob_name")
    nbci_name = input_data.get("non_bci_blob_name")

    # ──────────────────────────────────────────────
    # PHASE 1: Download BCI + filter per BU
    # ──────────────────────────────────────────────
    # Returns: {"makna_setia": [id1, id2], "ppch": [id3, id4], ...}
    # Plus the full filtered dataframe as serialized JSON rows
    context.set_custom_status({
        "phase": "Filtering BCI leads based on BU parameters...", "progress": 10
    })
    filter_result = yield context.call_activity("filter_bci", input_data)
    if isinstance(filter_result, str):
        filter_result = json.loads(filter_result)
    rejection_map = filter_result.get("rejection_map", {})
    filtered_leads = filter_result["filtered_leads"]  # list of project lead dicts
    bu_assignments = filter_result["bu_assignments"]  # {bu_name: [project_ids]}

    if not context.is_replaying:
        logger.info(f">>> Filtered BCI leads count: {len(filtered_leads)}")
        logger.info(f">>> BU assignments: {bu_assignments}")
    
    # ──────────────────────────────────────────────
    # PHASE 2+3: Download Non-BCI + Deduplication
    # ──────────────────────────────────────────────
    total_leads = len(filtered_leads)
    total_batches = (total_leads + batch_size - 1) // batch_size
    context.set_custom_status({
        "phase": "Filtering completed! Deduplicating BCI leads against Non-BCI leads...",
        "progress": 20,
        "processed_count": 0,
        "total_count": total_leads,
        "batch_number": 0,
        "total_batches": total_batches
    })

    dedup_result = yield context.call_activity("deduplicate", {
        "filtered_bci_leads": filtered_leads,
        "non_bci_blob_name": nbci_name,
    })
    duplicate_candidates = dedup_result["duplicates"]  # list of {bci_id, non_bci_id, similarity, details}

    # ──────────────────────────────────────────────
    # PHASE 4: Human approval (wait for external event)
    # ──────────────────────────────────────────────
    if len(duplicate_candidates) > 0:
        yield context.call_activity("store_duplicates_for_review", {
            "instance_id": context.instance_id,
            "duplicates": duplicate_candidates,
        })
        context.set_custom_status({
            "phase": "Duplicates found! Choose duplicates to remove...", 
            "progress": 25, 
            "has_duplicates": True,
            "duplicate_count": len(duplicate_candidates),
            "processed_count": 0,
            "total_count": total_leads,
            "batch_number": 0,
            "total_batches": total_batches
        })

        approval = yield context.wait_for_external_event("duplicate_approval")
        if isinstance(approval, str):
            approval = json.loads(approval)
        # approval = {"removed_ids": ["X006116", ...]}
        removed_ids = approval.get("removed_ids", [])
    else:
        context.set_custom_status({
            "phase": "No duplicates found! Proceeding...",
            "progress": 28,
            "processed_count": 0,
            "total_count": total_leads,
            "batch_number": 0,
            "total_batches": total_batches
        })
        removed_ids = []

    yield context.call_activity("store_bci_and_nonbci", {
        "instance_id": context.instance_id,
        "filter_results": filter_result,
        "removed_ids": removed_ids,
        "non_bci_blob_name": nbci_name
    })

    # ──────────────────────────────────────────────
    # PHASE: Show filtered BCI leads and final non-BCI leads for user to select the leads to run through the recommender for BU suggestions. Wait for external event with selected lead IDs.
    # ──────────────────────────────────────────────
    context.set_custom_status({
        "phase": "Select Leads for AI...",
        "progress": 28,
        "selection_required": True,
        "selection_done": False
    })
    lead_selection = yield context.wait_for_external_event("lead_selection")
    if isinstance(lead_selection, str):
        lead_selection = json.loads(lead_selection)
    selected_lead_ids = lead_selection.get("selected_lead_ids", [])

    # ──────────────────────────────────────────────
    # PHASE: Get stored leads which user has selected to run through the recommender for BU suggestions. 
    # ──────────────────────────────────────────────
    leads_for_recommender = yield context.call_activity("get_selected_leads_for_recommender", {
        "instance_id": context.instance_id,
        "selected_lead_ids": selected_lead_ids
    })
    
    # ──────────────────────────────────────────────
    # PHASE 5+6: Batched fan-out/fan-in
    # ──────────────────────────────────────────────
    temp_paths = []
    total_leads = len(leads_for_recommender)
    total_batches = (total_leads + batch_size - 1) // batch_size

    for i in range(0, total_leads, batch_size):
        batch = leads_for_recommender[i : i + batch_size]
        batch_idx = (i // batch_size) + 1
        
        current_batch_progress = int((i / total_leads) * 60)
    
        context.set_custom_status({
            "phase": "AI Recommender Running...",
            "progress": 30 + current_batch_progress,
            "batch_number": batch_idx,
            "total_batches": total_batches,
            "processed_count": i + len(batch), 
            "processing_range": f"{i+1}-{min(i+batch_size, total_leads)}", 
            "total_count": total_leads
        })

        # Fan-out: Start sub-orchestrations for this batch
        parallel_tasks = [
            context.call_sub_orchestrator("process_single_lead", {
                "lead": lead, "bu_assignments": bu_assignments
            }) for lead in batch
        ]

        # Fan-in: Wait for this batch to complete
        batch_results = yield context.task_all(parallel_tasks)
        temp_paths.extend(batch_results)

        # 2-3 seconds of breathing room between batches
        fire_at = context.current_utc_datetime + timedelta(seconds=2)
        yield context.create_timer(fire_at)
        
    # ──────────────────────────────────────────────
    # PHASE 7: Aggregate stored final results
    # ──────────────────────────────────────────────
    if not context.is_replaying:
        logger.info(f">>> Phase 7: Aggregating {len(temp_paths)} leads...")
    
    context.set_custom_status({
        "phase": "Aggregating results and generating final report...",
        "progress": 95,
        "processed_count": total_leads,
        "total_count": total_leads
    })

    final_output = yield context.call_activity("aggregate_and_finalize_results", {
        "temp_paths": temp_paths,
        "explicit_assignments": bu_assignments, # phase 1 filters
        "rejection_map": rejection_map,
        "confidence_threshold": 0.85,           
        "instance_id": context.instance_id 
    })

    context.set_custom_status({
        "phase": "Successfully completed!",
        "progress": 100,
        "processed_count": total_leads,
        "total_count": total_leads
    })

    return {
        "status": "complete", 
        "leads_processed": len(temp_paths),
        "results": final_output.get("results"),
        "final_report_path": final_output.get("blob_path")
    }
    

def _clean_project_detail(detail: str) -> str:
    """
    Strip generic building elements only if they follow the 
    specific sequence: *Access & Parking followed by *Access Panels.
    """
    if not detail:
        return ""

    marker_pattern = re.compile(r"building elements include:", re.IGNORECASE)
    match = marker_pattern.search(detail)

    if match:
        marker_end_index = match.end()
        content_after = detail[marker_end_index:].strip().lower()

        fingerprint = r"^\*\s*access & parking\s+\*\s*access panels & hatches"

        if re.match(fingerprint, content_after):
            return detail[:match.start()].strip()

    return detail.strip()


def _build_lead_context(lead: dict) -> str:
    """Build a clean text representation of the project lead for agents."""
    detail = _clean_project_detail(lead.get("Project Detail", ""))
    return json.dumps({
        "project_id": lead.get("Project ID"),
        "project_type": lead.get("Project Type"),
        "project_name": lead.get("Project Name"),
        "project_narrative": detail,
        "value": lead.get("Local Value"),
        "category_1": lead.get("Category 1 Name"),
        "category_2": lead.get("Category 2 Name"),
        "category_3": lead.get("Category 3 Name"),
        "category_4": lead.get("Category 4 Name"),
        "category_5": lead.get("Category 5 Name"),
        "sub_category_1": lead.get("Sub-Category 1 Name"),
        "sub_category_2": lead.get("Sub-Category 2 Name"),
        "sub_category_3": lead.get("Sub-Category 3 Name"),
        "sub_category_4": lead.get("Sub-Category 4 Name"),
        "sub_category_5": lead.get("Sub-Category 5 Name"),
        "sub_category_6": lead.get("Sub-Category 6 Name"),
        "sub_category_7": lead.get("Sub-Category 7 Name"),
        "sub_category_8": lead.get("Sub-Category 8 Name"),
        "storeys": lead.get("Storeys"),
        "project_status": lead.get("Project Status"),
        "project_stage": lead.get("Project Stage"),
        "construction_start": lead.get("Construction Start Date (Original format)"),
        "construction_end": lead.get("Construction End Date (Original format)"),
        "region": lead.get("Project Region"),
        "province": lead.get("Project Province / State"),
        "owner_type": lead.get("Owner Type Level 1 Primary"),
        "development_type": lead.get("Development Type"),
    }, indent=2)

@main.orchestration_trigger(context_name="context", orchestration="process_single_lead")
def process_single_lead_sub_orchestrator(context: df.DurableOrchestrationContext):
    data = context.get_input()
    lead = data["lead"]
    bu_assignments = data["bu_assignments"]
    
    lead_context = _build_lead_context(lead)
    
    # Fan-out: Run 6 agents in parallel for THIS lead
    agent_tasks = []
    for i in range(1, 7):
        params = {"agent_key": f"agent_{i}", "lead_context": lead_context}
        agent_tasks.append(context.call_activity("run_domain_agent", params))
    
    agent_results = yield context.task_all(agent_tasks)

    # Fan-in: Synthesize
    path = yield context.call_activity("synthesize_lead", {
        "lead": lead,
        "lead_context": lead_context,
        "agent_results": agent_results,
        "bu_assignments": bu_assignments,
        "instance_id": context.parent_instance_id # Use parent ID for storage grouping
    })
    
    return path