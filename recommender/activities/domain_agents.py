import azure.durable_functions as df
import json
import asyncio
import shared.identity as identity
from pathlib import Path
from openai import AsyncAzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

from shared.identity import default_credential
from shared import app_settings

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
    azure_ad_token_provider=token_provider,
    max_retries=5 # The SDK will handle some 429s for you automatically
)

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

    result = await _call_llm(client, deployment, system_prompt, f"Project Lead:\n{lead_context}\n\n")
    print(f">>> Result from {agent_key} for project lead: {result}")
    
    try:
        return {"agent": agent_key, "recommendations": json.loads(result)}
    except json.JSONDecodeError:
        return {"agent": agent_key, "recommendations": [], "raw": result}

@blueprint.activity_trigger(input_name="params")
async def synthesize_lead(params: dict) -> dict:
    # Pull everything from global memory except the lead-specific data
    lead = params["lead"]
    lead_context = params["lead_context"]
    agent_results = params["agent_results"]
    bu_assignments = params["bu_assignments"]

    print(f">>> LEAD CONTEXT: {lead_context}")

    synthesis_input = json.dumps({
        "project_lead": json.loads(lead_context),
        "agent_recommendations": {r["agent"]: r["recommendations"] for r in agent_results},
        "cross_reference_matrix": CROSS_REF_MATRIX,  # From Global
        "substitution_flags": SUBSTITUTION_FLAGS,    # From Global
        "bu_assignments": {
            bu: lead["Project ID"] in ids for bu, ids in bu_assignments.items()
        }
    }, indent=2)

    final_result = await _call_llm(client, deployment, SYNTHESIZER_PROMPT, synthesis_input)
    
    try:
        parsed_result = json.loads(final_result)
    except json.JSONDecodeError:
        parsed_result = {"raw_output": final_result}

    print(f">>> Final synthesized result for project {lead.get('Project ID')}: {parsed_result}")

    return {
        "project_id": lead.get("Project ID"),
        "project_name": lead.get("Project Name"),
        "assignments": parsed_result,
    }

async def _call_llm(client, deployment: str, system_prompt: str, user_message: str) -> str:
    """Call Azure OpenAI chat completion."""
    response = await client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        temperature=0.1,
        presence_penalty=0.2, # 0.2-0.5 to discourage repetition
        max_tokens=4096,
        response_format={"type": "json_object"},
    )
    return response.choices[0].message.content
