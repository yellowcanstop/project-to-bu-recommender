import azure.functions as func
import azure.durable_functions as df

import os
import json
import logging
from azure.storage.blob.aio import BlobServiceClient
from shared.identity import default_credential
from shared import app_settings

from recommender.orchestrator import main as orchestrator_bp
from recommender.activities.filter_bci import blueprint as filter_bci_bp
from recommender.activities.deduplicate import blueprint as deduplicate_bp
from recommender.activities.domain_agents import blueprint as domain_agents_bp
from recommender.activities.aggregate_and_finalize_results import blueprint as aggregate_and_finalize_results_bp
from recommender.activities.store_duplicates_for_review import blueprint as store_duplicates_for_review_bp


def register_recommender(app: df.DFApp):

    @app.route(route="recommender/upload", methods=["POST"])
    async def upload_leads(req: func.HttpRequest):
        logging.info("Processing file upload...")

        # 1. Parse the multipart form data
        # Streamlit/Curl will send files under specific keys
        file = req.files.get('file')
        if not file:
            return func.HttpResponse("No file provided in the 'file' field.", status_code=400)

        filename = file.filename
        container_name = "project-leads"
        
        # 2. Initialize Blob Client (using your existing logic pattern)
        blob_url = app_settings.blob_account_url
        if "UseDevelopmentStorage=true" in blob_url or "DefaultEndpointsProtocol" in blob_url:
            blob_service = BlobServiceClient.from_connection_string(blob_url)
        else:
            blob_service = BlobServiceClient(blob_url, credential=default_credential)

        try:
            async with blob_service:
                container_client = blob_service.get_container_client(container_name)
                
                # Ensure container exists
                if not await container_client.exists():
                    await container_client.create_container()

                # 3. Upload the file stream directly
                blob_client = container_client.get_blob_client(filename)
                file_body = file.read()
                
                await blob_client.upload_blob(file_body, overwrite=True)
                
                logging.info(f"Successfully uploaded {filename} to {container_name}")

            return func.HttpResponse(
                json.dumps({
                    "message": "File uploaded successfully",
                    "blob_name": filename,
                    "container": container_name,
                    "size_bytes": len(file_body)
                }),
                mimetype="application/json",
                status_code=200
            )

        except Exception as e:
            logging.error(f"Upload failed: {str(e)}")
            return func.HttpResponse(f"Internal Error: {str(e)}", status_code=500)
    
    @app.route(route="recommender/start", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def start_recommender(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        body = req.get_json()
        instance_id = await client.start_new("recommender_orchestrator", client_input=body)
        return client.create_check_status_response(req, instance_id)

    # callback for human-approved removal of BCI and non-BCI duplicates
    @app.route(route="recommender/approve/{instance_id}", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def approve_duplicates(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        instance_id = req.route_params["instance_id"]
        body = req.get_json()  # {"removed_ids": ["MEISID123", ...]}
        await client.raise_event(instance_id, "duplicate_approval", body)
        return func.HttpResponse(status_code=202)

    @app.route(route="recommender/status/{instance_id}", methods=["GET"])
    @app.durable_client_input(client_name="client")
    async def check_status(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        instance_id = req.route_params["instance_id"]
        status = await client.get_status(instance_id)
        return func.HttpResponse(status.to_json(), mimetype="application/json")

    # Register the blueprints
    app.register_functions(orchestrator_bp)
    app.register_functions(filter_bci_bp)
    app.register_functions(deduplicate_bp)
    app.register_functions(domain_agents_bp)
    app.register_functions(aggregate_and_finalize_results_bp)
    app.register_functions(store_duplicates_for_review_bp)