import uuid

import azure.functions as func
import azure.durable_functions as df

import os
import json
import logging
from azure.storage.blob.aio import BlobServiceClient
from shared.identity import default_credential
from shared import app_settings
from datetime import datetime

from recommender.orchestrator import main as orchestrator_bp
from recommender.activities.filter_bci import blueprint as filter_bci_bp
from recommender.activities.deduplicate import blueprint as deduplicate_bp
from recommender.activities.domain_agents import blueprint as domain_agents_bp
from recommender.activities.aggregate_and_finalize_results import blueprint as aggregate_and_finalize_results_bp
from recommender.activities.store_duplicates_for_review import blueprint as store_duplicates_for_review_bp
from recommender.activities.store_bci_and_nonbci import blueprint as store_bci_and_nonbci_bp

logger = logging.getLogger(__name__)



def register_recommender(app: df.DFApp):

    @app.route(route="recommender/upload", methods=["POST"])
    async def upload_leads(req: func.HttpRequest):
        logger.info("Processing file upload...")

        # 1. Parse the multipart form data
        # Streamlit/Curl will send files under specific keys
        file = req.files.get('file')
        lead_type = req.form.get('lead_type', 'unclassified')
        if not file:
            return func.HttpResponse("No file provided in the 'file' field.", status_code=400)

        # file name pattern: lead_type/YYYYMMDD_HHMM_UUID_original_name.xlsx
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        unique_id = str(uuid.uuid4())[:8]
        clean_filename = file.filename.replace(" ", "_")

        blob_name = f"{lead_type}/{timestamp}_{unique_id}_{clean_filename}"
        container_name = app_settings.blob_container or "project-leads"
        
        # 2. Initialize Blob Client (using your existing logic pattern)
        blob_service = BlobServiceClient.from_connection_string(app_settings.blob_account_url)

        try:
            async with blob_service:
                container_client = blob_service.get_container_client(container_name)
                
                # Ensure container exists
                if not await container_client.exists():
                    await container_client.create_container()

                # 3. Upload the file stream directly
                blob_client = container_client.get_blob_client(blob_name)
                file_body = file.read()
                
                await blob_client.upload_blob(file_body, overwrite=True)
                
                logger.info(f"Successfully uploaded {blob_name} to {container_name}")

            return func.HttpResponse(
                json.dumps({
                    "message": f"Successfully uploaded to {lead_type}",
                    "blob_path": blob_name,
                    "lead_type": lead_type
                }),
                mimetype="application/json",
                status_code=200
            )

        except Exception as e:
            logger.error(f"Upload failed: {str(e)}")
            return func.HttpResponse(f"Internal Error: {str(e)}", status_code=500)
    
    @app.route(route="recommender/start", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def start_recommender(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        body = req.get_json()
        instance_id = await client.start_new("recommender_orchestrator", client_input=body)
        return client.create_check_status_response(req, instance_id)

    @app.route(route="recommender/duplicates/{instance_id}", methods=["GET"])
    async def get_duplicates(req: func.HttpRequest) -> func.HttpResponse:
        instance_id = req.route_params.get("instance_id")
        container_name = "duplicate-reviews"
        blob_name = f"pending/{instance_id}.json"
        
        blob_service = BlobServiceClient.from_connection_string(app_settings.blob_account_url)
        blob_client = blob_service.get_blob_client(container_name, blob_name)
        
        if not await blob_client.exists():
            return func.HttpResponse("Not found", status_code=404)

        stream = await blob_client.download_blob()
        data = await stream.readall()
        return func.HttpResponse(data, mimetype="application/json")

    @app.route(route="recommender/leads/{instance_id}", methods=["GET"])
    async def get_leads(req: func.HttpRequest) -> func.HttpResponse:
        instance_id = req.route_params.get("instance_id")
        container_name = "processed-leads-combined"
        blob_name = f"final/{instance_id}.json"
        
        blob_service = BlobServiceClient.from_connection_string(app_settings.blob_account_url)
        blob_client = blob_service.get_blob_client(container_name, blob_name)
        
        if not await blob_client.exists():
            return func.HttpResponse("Not found", status_code=404)

        stream = await blob_client.download_blob()
        data = await stream.readall()
        return func.HttpResponse(data, mimetype="application/json")

    # callback for human-approved removal of BCI and non-BCI duplicates
    @app.route(route="recommender/approve/{instance_id}", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def approve_duplicates(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        instance_id = req.route_params["instance_id"]
        try:
            body = req.get_json() # {"removed_ids": ["MEISID123", ...]}
        except ValueError:
            return func.HttpResponse("Invalid JSON", status_code=400)
        removed_ids = body.get("removed_ids", [])
        await client.raise_event(instance_id, "duplicate_approval", {"removed_ids": removed_ids})
        return func.HttpResponse(status_code=200)
    
    # callback for human selection of lead IDs to run through recommender for BU suggestions
    @app.route(route="recommender/select/{instance_id}", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def select_leads(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        instance_id = req.route_params["instance_id"]
        try:
            body = req.get_json()
        except ValueError:
            return func.HttpResponse("Invalid JSON", status_code=400)
        selected_lead_ids = body.get("selected_lead_ids", [])
        await client.raise_event(instance_id, "lead_selection", {"selected_lead_ids": selected_lead_ids})
        return func.HttpResponse(status_code=200)

    @app.route(route="recommender/status/{instance_id}", methods=["GET"])
    @app.durable_client_input(client_name="client")
    async def check_status(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        instance_id = req.route_params["instance_id"]
        logger.info(f"Checking status for instance ID: {instance_id}")
        status = await client.get_status(instance_id)
        if not status:
            return func.HttpResponse(
                json.dumps({"error": "Instance ID not found or not yet initialized."}),
                mimetype="application/json",
                status_code=404
            )
        status_data = status.to_json()
        return func.HttpResponse(
            json.dumps(status_data), 
            mimetype="application/json",
            status_code=200
        )

    # Register the blueprints
    app.register_functions(orchestrator_bp)
    app.register_functions(filter_bci_bp)
    app.register_functions(deduplicate_bp)
    app.register_functions(domain_agents_bp)
    app.register_functions(aggregate_and_finalize_results_bp)
    app.register_functions(store_duplicates_for_review_bp)
    app.register_functions(store_bci_and_nonbci_bp)