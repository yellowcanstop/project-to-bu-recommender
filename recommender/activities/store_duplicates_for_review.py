import azure.durable_functions as df
import json
import logging
from azure.storage.blob.aio import BlobServiceClient
from shared.identity import default_credential
from shared import app_settings

logger = logging.getLogger(__name__)

blueprint = df.Blueprint()

@blueprint.activity_trigger(input_name="input_data")
async def store_duplicates_for_review(input_data: dict) -> dict:
    instance_id = input_data.get("instance_id")
    duplicates = input_data.get("duplicates", [])

    container_name = "duplicate-reviews"
    blob_name = f"pending/{instance_id}.json"

    blob_url = app_settings.blob_account_url

    if "UseDevelopmentStorage=true" in blob_url or "DefaultEndpointsProtocol" in blob_url:
        blob_service = BlobServiceClient.from_connection_string(blob_url)
    else:
        blob_service = BlobServiceClient(blob_url, credential=default_credential)

    async with blob_service:
        container_client = blob_service.get_container_client(container_name)
        if not await container_client.exists():
            await container_client.create_container()

        payload = {
            "instance_id": instance_id,
            "duplicates": duplicates,
            "duplicate_count": len(duplicates),
        }

        blob_client = container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(
            json.dumps(payload, indent=4),
            overwrite=True
        )

        logger.info(f"Stored {len(duplicates)} duplicate candidates for review at {container_name}/{blob_name}")

    return {
        "status": "pending_review",
        "blob_path": f"{container_name}/{blob_name}",
        "instance_id": instance_id,
        "duplicate_count": len(duplicates),
    }