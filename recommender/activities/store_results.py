import azure.durable_functions as df
import json
import io
import datetime
import logging
from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from shared.identity import default_credential
from shared import app_settings


blueprint = df.Blueprint()

@blueprint.activity_trigger(input_name="input_data")
async def store_results(input_data: dict) -> dict:
    temp_paths = input_data.get("temp_paths", [])
    instance_id = input_data.get("instance_id")
    temp_container_name = "temp-results"
    final_container_name = "recommender-outputs"
    
    combined_results = {}

    blob_url = app_settings.blob_account_url
    
    if "UseDevelopmentStorage=true" in blob_url or "DefaultEndpointsProtocol" in blob_url:
        blob_service = BlobServiceClient.from_connection_string(blob_url)
    else:
        blob_service = BlobServiceClient(blob_url, credential=default_credential)

    async with blob_service:
        # 1. AGGREGATE: Download all individual lead results
        for path in temp_paths:
            try:
                temp_client = blob_service.get_blob_client(temp_container_name, path)
                stream = await temp_client.download_blob()
                content = await stream.readall()
                
                # Use the filename as the key (Project ID)
                project_id = path.split('/')[-1].replace('.json', '')
                combined_results[project_id] = json.loads(content)
            except Exception as e:
                logging.error(f"Failed to read temp blob {path}: {e}")

        # 2. STORE FINAL: Save the combined JSON
        final_container = blob_service.get_container_client(final_container_name)
        if not await final_container.exists():
            await final_container.create_container()
            
        final_blob_name = f"results/recommendations_{instance_id}.json"
        final_blob_client = final_container.get_blob_client(final_blob_name)
        
        await final_blob_client.upload_blob(
            json.dumps(combined_results, indent=4), 
            overwrite=True
        )
        logging.info(f"Successfully stored final results to {final_blob_name}")

        # 3. CLEANUP: Delete the temp blobs now that final is safe
        # We only do this AFTER the final upload succeeds
        for path in temp_paths:
            try:
                temp_client = blob_service.get_container_client(temp_container_name)
                await temp_client.delete_blob(path)
            except Exception as e:
                logging.warning(f"Cleanup failed for {path}: {e}")

    return {
        "status": "complete",
        "final_path": f"{final_container_name}/{final_blob_name}",
        "count": len(combined_results)
    }