import azure.durable_functions as df
import json
import io
import datetime
from azure.storage.blob.aio import BlobServiceClient
from shared.identity import default_credential
from shared import app_settings


blueprint = df.Blueprint()

@blueprint.activity_trigger(input_name="input_data")
async def store_results(input_data: dict) -> dict:
    """
    Stores the final synthesis results into Azure Blob Storage as a JSON file.
    """
    recommendations = input_data.get("recommendations", {})
    # Use instance_id for the filename if provided, otherwise timestamp
    instance_id = input_data.get("instance_id", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    
    blob_url = app_settings.blob_account_url
    container_name = app_settings.results_container_name  
    blob_name = f"results/recommendations_{instance_id}.json"

    if "UseDevelopmentStorage=true" in blob_url or "DefaultEndpointsProtocol" in blob_url:
        blob_service = BlobServiceClient.from_connection_string(blob_url)
    else:
        blob_service = BlobServiceClient(blob_url, credential=default_credential)

    json_data = json.dumps(recommendations, indent=4)
    data_stream = io.BytesIO(json_data.encode('utf-8'))

    try:
        async with blob_service:
            container_client = blob_service.get_container_client(container_name)
            
            if not await container_client.exists():
              await container_client.create_container()

            blob_client = container_client.get_blob_client(blob_name)
            
            await blob_client.upload_blob(
                data_stream, 
                blob_type="BlockBlob", 
                overwrite=True
            )

        return {
            "status": "success",
            "blob_path": f"{container_name}/{blob_name}",
            "leads_stored": len(recommendations)
        }
    
    except Exception as e:
        print(f"Error storing results: {str(e)}")
        raise e