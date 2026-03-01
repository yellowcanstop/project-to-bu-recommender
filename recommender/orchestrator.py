import re

import azure.durable_functions as df
import json
import logging

logger = logging.getLogger(__name__)

'''
Orchestrates:
Activity 1: Download & Filter BCI (per BU)
Activity 2: Download Non-BCI
Activity 3: Deduplication (embeddings)
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
    context.set_custom_status({"phase": "Filtering BCI leads based on BU parameters...", "progress": 10})
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
    context.set_custom_status({"phase": "Filtering completed! Deduplicating BCI leads against Non-BCI leads...", "progress": 20})
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
            "phase": "Duplicates identified! Awaiting user-approved duplicate removal...", 
            "progress": 30, 
            "has_duplicates": True,
            "duplicate_count": len(duplicate_candidates)
        })

        approval = yield context.wait_for_external_event("duplicate_approval")
        if isinstance(approval, str):
            approval = json.loads(approval)
        # approval = {"removed_ids": ["X006116", ...]}
        removed_ids = approval.get("removed_ids", [])
    else:
        context.set_custom_status({
            "phase": "No duplicates found! Proceeding to the recommender...",
            "progress": 30
        })
        removed_ids = []

    # Filter out removed non-BCI duplicates (they're confirmed duplicates of BCI)
    # remaining non-BCI leads: TODO lightweight llm classifier to infer category/sector then also go through phase 5+6
    if not context.is_replaying:
        logger.info(f">>> User approved removal of {len(removed_ids)} duplicates: {removed_ids}")

    # ──────────────────────────────────────────────
    # PHASE 5+6: For each filtered BCI lead, run agents + synthesize + store results in temp storage (per lead)
    # ──────────────────────────────────────────────
    temp_paths = []
    total_leads = len(filtered_leads)
    for i, lead in enumerate(filtered_leads):
        context.set_custom_status({
            "phase": "AI Recommender Running",
            "progress": 30 + int((i / total_leads) * 60), # Scaled 30% to 90%
            "current_lead": lead.get('Project Name'),
            "processed_count": i + 1,
            "total_count": total_leads
        })

        # TODO to remove
        if lead.get('Project ID') != '90897003' and lead.get('Project ID') != '129285003':
            continue
        
        lead_context = _build_lead_context(lead)
        agent_tasks = []
        for i in range(1,7):
            agent_key = f"agent_{i}"
            params = {
                "agent_key": agent_key,
                "lead_context": lead_context
            }
            task = context.call_activity("run_domain_agent", params)
            agent_tasks.append(task)

        agent_results = yield context.task_all(agent_tasks)

        path = yield context.call_activity(
            "synthesize_lead", 
            {
                "lead": lead,
                "lead_context": lead_context,
                "agent_results": agent_results,
                "bu_assignments": bu_assignments,
                "instance_id": context.instance_id 
            }
        )
        temp_paths.append(path)
        
    # ──────────────────────────────────────────────
    # PHASE 7: Aggregate stored final results
    # ──────────────────────────────────────────────
    if not context.is_replaying:
        logger.info(f">>> Phase 7: Aggregating {len(temp_paths)} leads...")
    
    context.set_custom_status({
        "phase": "Aggregating results and generating final report...",
        "progress": 95
    })

    final_output = yield context.call_activity("aggregate_and_finalize_results", {
        "temp_paths": temp_paths,
        "explicit_assignments": bu_assignments, # phase 1 filters
        "rejection_map": rejection_map,
        "confidence_threshold": 0.85,           
        "instance_id": context.instance_id 
    })

    return {
        "status": "complete", 
        "leads_processed": len(temp_paths),
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