import azure.durable_functions as df
import json
import asyncio
from pathlib import Path

from shared.identity import default_credential

blueprint = df.Blueprint()

CONFIG_DIR = Path(__file__).parent.parent / "config"
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


def _load_json(filename: str) -> dict | list:
    with open(CONFIG_DIR / filename, "r") as f:
        return json.load(f)


def _load_prompt(filename: str) -> str:
    with open(PROMPTS_DIR / filename, "r") as f:
        return f.read()


def _clean_project_detail(detail: str) -> str:
    """Strip the generic building elements dump."""
    if not detail:
        return ""
    marker = "Building elements include:"
    if marker in detail:
        return detail.split(marker)[0].strip()
    # Also try lowercase variant
    marker_lower = "building elements include:"
    if marker_lower in detail.lower():
        idx = detail.lower().index(marker_lower)
        return detail[:idx].strip()
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
        "sub_category_1": lead.get("Sub-Category 1 Name"),
        "sub_category_2": lead.get("Sub-Category 2 Name"),
        "sub_category_3": lead.get("Sub-Category 3 Name"),
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


async def _call_llm(client, deployment: str, system_prompt: str, user_message: str) -> str:
    """Call Azure OpenAI chat completion."""
    response = await client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        temperature=0.1,
        response_format={"type": "json_object"},
    )
    return response.choices[0].message.content


@blueprint.activity_trigger("process_single_lead")
async def process_single_lead_activity(input_data: dict) -> dict:
    from openai import AsyncAzureOpenAI
    import os

    lead = input_data["lead"]
    bu_assignments = input_data["bu_assignments"]

    # Azure OpenAI client
    client = AsyncAzureOpenAI(
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_version="2024-12-01-preview",
        azure_ad_token_provider=default_credential(),
    )
    deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

    lead_context = _build_lead_context(lead)

    # ─── STEP 1: Signal Extraction ───
    signal_prompt = _load_prompt("signal_extractor.txt")
    extracted_signals = await _call_llm(client, deployment, signal_prompt, lead_context)

    # ─── STEP 2: Run 6 Domain Agents in Parallel ───
    domain_agent_prompt_template = _load_prompt("domain_agent.txt")
    taxonomies = {
        f"agent_{i}": _load_json(f"taxonomy_agent{i}.json")
        for i in range(1, 7)
    }
    agent_names = {
        "agent_1": "Structural & Substructure",
        "agent_2": "Envelope & Insulation",
        "agent_3": "Interior Finishing & Appliances",
        "agent_4": "Walling & Partitioning",
        "agent_5": "MEP, ELV & Smart Systems",
        "agent_6": "Civil Infrastructure & Precast",
    }

    async def run_domain_agent(agent_key: str) -> dict:
        taxonomy_json = json.dumps(taxonomies[agent_key], indent=2)
        system_prompt = domain_agent_prompt_template.replace(
            "{{AGENT_NAME}}", agent_names[agent_key]
        ).replace(
            "{{TAXONOMY}}", taxonomy_json
        )
        user_msg = (
            f"Project Lead:\n{lead_context}\n\n"
            f"Extracted Signals:\n{extracted_signals}"
        )
        result = await _call_llm(client, deployment, system_prompt, user_msg)
        try:
            return {"agent": agent_key, "recommendations": json.loads(result)}
        except json.JSONDecodeError:
            return {"agent": agent_key, "recommendations": [], "raw": result}

    agent_tasks = [run_domain_agent(key) for key in taxonomies.keys()]
    agent_results = await asyncio.gather(*agent_tasks)

    # ─── STEP 3: Synthesis ───
    cross_ref = _load_json("cross_reference_matrix.json")
    substitutions = _load_json("substitution_flags.json")
    synthesizer_prompt = _load_prompt("synthesizer.txt")

    synthesis_input = json.dumps({
        "project_lead": json.loads(lead_context),
        "extracted_signals": json.loads(extracted_signals),
        "agent_recommendations": {r["agent"]: r["recommendations"] for r in agent_results},
        "cross_reference_matrix": cross_ref,
        "substitution_flags": substitutions,
        "bu_assignments": {
            bu: lead["Project ID"] in ids
            for bu, ids in bu_assignments.items()
        },
    }, indent=2)

    final_result = await _call_llm(client, deployment, synthesizer_prompt, synthesis_input)

    try:
        parsed_result = json.loads(final_result)
    except json.JSONDecodeError:
        parsed_result = {"raw_output": final_result}

    return {
        "project_id": lead.get("Project ID"),
        "project_name": lead.get("Project Name"),
        "assignments": parsed_result,
    }