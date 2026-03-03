from typing import Dict, Optional, Tuple, Any

import azure.durable_functions as df
import json
import asyncio
import shared.identity as identity
from pathlib import Path
from openai import AsyncAzureOpenAI
from azure.storage.blob.aio import BlobServiceClient
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

from shared.identity import default_credential
from shared import app_settings
from shared.confidence.openai_confidence import evaluate_confidence

blueprint = df.Blueprint()

CONFIG_DIR = Path(__file__).parent.parent / "config"
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

def _load_json(filename: str) -> dict | list:
    with open(CONFIG_DIR / filename, "r") as f:
        return json.load(f)

def _load_prompt(filename: str) -> str:
    with open(PROMPTS_DIR / filename, "r") as f:
        return f.read()

# --- GLOBAL REGISTRY (Loaded once per worker start) ---

AGENT_NAMES = {
    "agent_1": "Structural & Substructure",
    "agent_2": "Envelope & Insulation",
    "agent_3": "Interior Finishing & Appliances",
    "agent_4": "Walling & Partitioning",
    "agent_5": "MEP, ELV & Smart Systems",
    "agent_6": "Civil Infrastructure & Precast",
}

# Pre-load all taxonomies into a dictionary
TAXONOMY_CACHE = {
    f"agent_{i}": _load_json(f"taxonomy_agent{i}.json") 
    for i in range(1, 7)
}

# Pre-load static files for the synthesizer too
CROSS_REF_MATRIX = _load_json("cross_reference_matrix.json")
SUBSTITUTION_FLAGS = _load_json("substitution_flags.json")

# Pre-load the prompts
DOMAIN_PROMPT_TEMPLATE = _load_prompt("domain_agent.txt")
SYNTHESIZER_PROMPT = _load_prompt("synthesizer.txt")

token_provider = get_bearer_token_provider(
    DefaultAzureCredential(), 
    "https://cognitiveservices.azure.com/.default"
)

client = AsyncAzureOpenAI(
    azure_endpoint=app_settings.azure_openai_endpoint,
    api_version="2024-12-01-preview",
    api_key=app_settings.azure_openai_key
)

# Initialize Global Blob Service Client
blob_service = BlobServiceClient.from_connection_string(app_settings.blob_account_url)


deployment = app_settings.azure_openai_chat_deployment

@blueprint.activity_trigger(input_name="params")
async def run_domain_agent(params: dict) -> dict:
    # We only receive the key and the context
    agent_key = params["agent_key"]
    lead_context = params["lead_context"]

    # LOOKUP from Global Registry (O(1) speed, no disk I/O)
    agent_name = AGENT_NAMES.get(agent_key)
    taxonomy = TAXONOMY_CACHE.get(agent_key)

    system_prompt = DOMAIN_PROMPT_TEMPLATE.replace(
        "{{AGENT_NAME}}", agent_name
    ).replace(
        "{{TAXONOMY}}", json.dumps(taxonomy, indent=2)
    )

    result, choice = await _call_llm(client, deployment, system_prompt, f"Project Lead:\n{lead_context}\n\n")
    
    try:
        result_json = json.loads(result)
        result_with_confidence = evaluate_confidence(result_json, choice)
        return {"agent": agent_key, "recommendations": result_with_confidence}
    except json.JSONDecodeError:
        return {"agent": agent_key, "recommendations": [], "raw": result}

@blueprint.activity_trigger(input_name="params")
async def synthesize_lead(params: dict) -> str:
    # Pull everything from global memory except the lead-specific data
    lead = params["lead"]
    lead_context = params["lead_context"]
    agent_results = params["agent_results"]
    bu_assignments = params["bu_assignments"]

    print(f">>> LEAD CONTEXT: {lead_context}")

    synthesis_input = json.dumps({
        "project_lead": json.loads(lead_context),
        "agent_recommendations_with_confidence": {r["agent"]: r["recommendations"] for r in agent_results},
        "cross_reference_matrix": CROSS_REF_MATRIX,  # From Global
        "substitution_flags": SUBSTITUTION_FLAGS,    # From Global
        "bu_assignments": {
            bu: lead["Project ID"] in ids for bu, ids in bu_assignments.items()
        }
    }, indent=2)

    final_result, choice = await _call_llm(client, deployment, SYNTHESIZER_PROMPT, synthesis_input)
    
    try:
        parsed_result = json.loads(final_result)
        confidence_for_synthesizer = evaluate_confidence(parsed_result, choice)
        bu_assigned = list({item["BU"] for item in parsed_result.get("assignments", [])})
    except json.JSONDecodeError:
        parsed_result = {"raw_output": final_result}
        confidence_for_synthesizer = 0.0
        bu_assigned = []

    lead_id = lead.get("Project ID")
    instance_id = params.get("instance_id")
    final_analysis = {
        "lead_id": lead_id,
        "project_name": lead.get("Project Name"),
        "project_value": lead.get("Local Value"),
        "project_status": lead.get("Project Status"),
        "project_stage": lead.get("Project Stage"),
        "construction_start": lead.get("Construction Start Date (Original format)"),
        "construction_end": lead.get("Construction End Date (Original format)"),
        "region": lead.get("Project Region"),
        "development_type": lead.get("Development Type"),
        "assigned_bu": bu_assigned,
        "synthesizer_confidence": confidence_for_synthesizer,
        "detailed_results": parsed_result
    }
    
    # Path: temp/orchestration_id/lead_id.json
    temp_blob_name = f"temp/{instance_id}/{lead_id}.json"

    container_client = blob_service.get_container_client("temp-results")

    if not await container_client.exists():
        await container_client.create_container()
        
    blob_client = container_client.get_blob_client(temp_blob_name)
    await blob_client.upload_blob(json.dumps(final_analysis), overwrite=True)

    # return path string of temporary blob to orchestrator
    return temp_blob_name

async def _call_llm(client, deployment: str, system_prompt: str, user_message: str) -> Tuple[Optional[Dict], Optional[Any]]:
    """Call Azure OpenAI chat completion."""
    response = await client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        temperature=0.1,
        presence_penalty=0.2, # 0.2-0.5 to discourage repetition
        #max_tokens=4096,
        logprobs=True,
        response_format={"type": "json_object"},
    )
    choice = response.choices[0]
    return response.choices[0].message.content, choice
