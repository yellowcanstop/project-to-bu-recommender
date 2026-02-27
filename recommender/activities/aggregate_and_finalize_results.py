import asyncio
import json
import logging
import os
import azure.durable_functions as df
from azure.storage.blob.aio import BlobServiceClient

from shared import app_settings
from shared.identity import default_credential

logger = logging.getLogger(__name__)

blueprint = df.Blueprint()

@blueprint.activity_trigger(input_name="params")
async def aggregate_and_finalize_results(params: dict) -> dict:
    temp_paths = params.get("temp_paths", [])
    explicit_map = params.get("explicit_assignments", {})
    threshold = params.get("confidence_threshold", 0.85)
    instance_id = params.get("instance_id")

    print(f"Explicit Map: {explicit_map}")

    # 1. Connection Logic (Robust for Local & Cloud)
    blob_url = app_settings.blob_account_url
    if "UseDevelopmentStorage=true" in blob_url or "DefaultEndpointsProtocol" in blob_url:
        blob_service = BlobServiceClient.from_connection_string(blob_url)
    else:
        blob_service = BlobServiceClient(blob_url, credential=default_credential)

    async with blob_service:
        temp_container = blob_service.get_container_client("temp-results")
        
        # 2. PARALLEL DOWNLOAD
        async def fetch_lead(path):
            try:
                blob_client = temp_container.get_blob_client(path)
                stream = await blob_client.download_blob()
                data = await stream.readall()
                return json.loads(data)
            except Exception as e:
                logger.error(f"Failed to read {path}: {e}")
                return None

        all_results = await asyncio.gather(*[fetch_lead(p) for p in temp_paths])
        all_results = [r for r in all_results if r is not None]

        # 3. APPLY RESCUE LOGIC
        final_output = {
            "summary": {"total_processed": len(all_results), "bu_counts": {}},
            "business_units": {}
        }

        rejection_map = params.get("rejection_map", {})

        normalized_map = {
            str(k).lower().replace(" ", ""): v 
            for k, v in explicit_map.items()
        }

        for analysis in all_results:
            lead_id = str(analysis.get("lead_id"))
            conf_assignments = analysis.get("synthesizer_confidence", {}).get("assignments", [])

            bu_max_scores = {}
            for item in conf_assignments:
                bu_name = item["BU"]["value"]
                score = item["BU"]["confidence"]
                if bu_name not in bu_max_scores or score > bu_max_scores[bu_name]:
                    bu_max_scores[bu_name] = score

            for bu, max_conf in bu_max_scores.items():
                # TODO to remove
                print(f"Lead {lead_id}: Final max confidence for BU {bu} is {max_conf}")

                lookup_key = bu.lower().replace(" ", "")

                is_explicit = lead_id in normalized_map.get(lookup_key, [])
                is_high_conf = max_conf >= threshold

                if bu not in final_output["business_units"]:
                    final_output["business_units"][bu] = {"verified": [], "discovery": []}
                    final_output["summary"]["bu_counts"][bu] = {"verified": 0, "discovery": 0}

                if is_explicit:
                    final_output["business_units"][bu]["verified"].append(analysis)
                    final_output["summary"]["bu_counts"][bu]["verified"] += 1
                elif is_high_conf:
                    # "why was this a discovery?" tooltip on lead card on frontend can show original rejection reason (e.g. AI suggested this lead but BU-given filtering parameters rejected it)
                    original_rejection = rejection_map.get(lead_id, {}).get(bu, "Criteria mismatch")
                    rescued_lead = analysis.copy()
                    rescued_lead["rescue_metadata"] = {
                        "ai_confidence": f"{round(max_conf * 100, 1)}%",
                        "original_rejection_reason": original_rejection
                    }
                    final_output["business_units"][bu]["discovery"].append(rescued_lead)
                    final_output["summary"]["bu_counts"][bu]["discovery"] += 1

        # 4. STORE FINAL: Only proceed to cleanup if this succeeds
        final_container = blob_service.get_container_client("recommender-outputs")
        if not await final_container.exists():
            await final_container.create_container()

        final_blob_name = f"results/recommendations_{instance_id}.json"
        await final_container.upload_blob(name=final_blob_name, data=json.dumps(final_output, indent=4), overwrite=True)
        logger.info(f"Successfully stored final results to {final_blob_name}")

        # 5. PARALLEL CLEANUP: Delete temp blobs now that final is safe
        async def safe_delete(path):
            try:
                await temp_container.delete_blob(path)
            except Exception as e:
                logger.warning(f"Cleanup failed for {path}: {e}")

        # Use gather again to delete hundreds of files in seconds
        await asyncio.gather(*[safe_delete(p) for p in temp_paths])

    return {
        "status": "complete",
        "final_path": f"recommender-outputs/{final_blob_name}",
        "count": len(all_results)
    }