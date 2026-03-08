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
async def store_bci_and_nonbci(input_data: dict) -> dict:
    instance_id = input_data.get("instance_id")
    filter_results = input_data.get("filter_results", {})  # BCI Data
    removed_ids = input_data.get("removed_ids", [])        # IDs to exclude from Non-BCI
    nbci_blob_name = input_data.get("non_bci_blob_name")
    
    # Configuration
    source_container = input_data.get("container") or app_settings.blob_container
    output_container = "processed-leads-combined"
    output_blob_name = f"final/{instance_id}.json"
    
    blob_service = BlobServiceClient.from_connection_string(app_settings.blob_account_url)

    async with blob_service:
        # --- 1. PROCESS NON-BCI DATA (Excel) ---
        source_client = blob_service.get_blob_client(source_container, nbci_blob_name)
        download = await source_client.download_blob()
        content = await download.readall()

        f = fastexcel.read_excel(content)
        excel_data = io.BytesIO(content)
        dfs = []
        
        for sheet in f.sheet_names:
            raw = pl.read_excel(excel_data, sheet_name=sheet, has_header=False)
            normalized = find_and_normalize(raw, sheet)
            if normalized is not None:
                dfs.append(normalized)

        # Merge and Filter
        non_bci_df = pl.concat(dfs).filter(
            (pl.col("GSM Project ID").cast(pl.Utf8).str.strip_chars() != "Grand Total")
        )

        if removed_ids:
            non_bci_df = non_bci_df.filter(
                ~pl.col("GSM Project ID").is_in(removed_ids)
            )

        # Convert to JSON-safe list (handling NaNs)
        clean_nbci_rows = [
            {k: (None if isinstance(v, (float, np.float64)) and np.isnan(v) else v) 
             for k, v in row.items()} 
            for row in non_bci_df.to_dicts()
        ]

        final_nbci_leads = []
        for row in clean_nbci_rows:
            lead = row.copy()
            # Use GSM Project ID as the standard 'id'
            lead['id'] = str(row.get("GSM Project ID", ""))
            lead['source'] = "non-bci"
            final_nbci_leads.append(lead)

        bci_leads_raw = filter_results.get("filtered_leads", [])
        final_bci_leads = []
        for row in bci_leads_raw:
            lead = row.copy()
            # Ensure BCI leads also have the 'id' key
            lead['id'] = str(row.get("Project ID", ""))
            lead['source'] = "bci"
            final_bci_leads.append(lead)
        
        combined_leads = final_bci_leads + final_nbci_leads

        # --- 2. CONSOLIDATE PAYLOAD ---
        # This is the single object the frontend will receive
        combined_payload = {
            "instance_id": instance_id,
            "metadata": {
                "nbci_source_file": nbci_blob_name,
                "bci_count": filter_results.get("total_filtered", 0),
                "nbci_count": len(clean_nbci_rows)
            },
            "combined_leads": combined_leads
        }

        # --- 3. UPLOAD TO BLOB ---
        container_client = blob_service.get_container_client(output_container)
        if not await container_client.exists():
            await container_client.create_container()

        blob_client = container_client.get_blob_client(output_blob_name)
        await blob_client.upload_blob(
            json.dumps(combined_payload, indent=4), 
            overwrite=True
        )

    logger.info(f"Combined data stored successfully for {instance_id}")

    return {
        "status": "success",
        "blob_path": f"{output_container}/{output_blob_name}",
        "counts": {
            "bci": filter_results.get("total_filtered", 0),
            "non_bci": len(clean_nbci_rows)
        }
    }