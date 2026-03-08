import logging

import azure.durable_functions as df
from recommender.activities.deduplicate import find_and_normalize
import shared.identity as identity
import io

from shared.identity import default_credential
from shared import app_settings

from azure.storage.blob.aio import BlobServiceClient
from azure.identity import get_bearer_token_provider
from openai import AsyncAzureOpenAI
import polars as pl
import numpy as np
import fastexcel
import json

logger = logging.getLogger(__name__)
blueprint = df.Blueprint()


@blueprint.activity_trigger(input_name="input_data")
async def get_selected_leads(input_data: dict) -> list:
    """
    Downloads the combined leads from blob storage and filters them 
    for the recommender phase to keep orchestrator memory usage low.
    """
    instance_id = input_data.get("instance_id")
    # Using a set for O(1) lookup performance
    selected_ids = set(map(str, input_data.get("selected_lead_ids", [])))
    
    container_name = "processed-leads-combined"
    blob_name = f"final/{instance_id}.json"
    
    blob_service = BlobServiceClient.from_connection_string(app_settings.blob_account_url)

    async with blob_service:
        blob_client = blob_service.get_blob_client(container_name, blob_name)
        
        # Download and parse
        stream = await blob_client.download_blob()
        content = await stream.readall()
        payload = json.loads(content)
        
        combined_leads = payload.get("combined_leads", [])

        # TODO support for non-BCI leads in recommender, pending lightweight model to expand non-BCI lead context
        # We only return the specific BCI leads selected by the user
        leads_for_recommender = [
            lead for lead in combined_leads 
            if str(lead.get('id')) in selected_ids and lead.get('source') == 'bci'
        ]

    return leads_for_recommender