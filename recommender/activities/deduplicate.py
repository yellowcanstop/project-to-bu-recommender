import azure.durable_functions as df
import shared.identity as identity
import io

from shared.identity import default_credential
from shared import app_settings

from azure.storage.blob.aio import BlobServiceClient
from azure.identity import get_bearer_token_provider
from openai import AsyncAzureOpenAI
import polars as pl
import numpy as np

blueprint = df.Blueprint()


@blueprint.activity_trigger(input_name="input_data")
async def deduplicate(input_data: dict) -> dict:
    filtered_bci_leads = input_data["filtered_bci_leads"]

    # Download non-BCI file
    blob_url = input_data.get("blob_account_url") or app_settings.blob_account_url
    container = input_data.get("container") or app_settings.blob_container
    non_bci_blob = input_data.get("non_bci_blob_name") or app_settings.non_bci_blob_name

    if "UseDevelopmentStorage=true" in blob_url or "DefaultEndpointsProtocol" in blob_url:
        blob_service = BlobServiceClient.from_connection_string(blob_url)
    else:
        blob_service = BlobServiceClient(blob_url, credential=default_credential)

    async with blob_service:
        blob_client = blob_service.get_blob_client(container, non_bci_blob)
        download = await blob_client.download_blob()
        content = await download.readall()

    non_bci_df = pl.read_excel(io.BytesIO(content))

    # If 'GSM Project ID' isn't in the headers, search all rows/columns to find the true header row
    if "GSM Project ID" not in non_bci_df.columns:
        header_idx = None
        
        # iter_rows() yields tuples of the row values
        for i, row_tuple in enumerate(non_bci_df.iter_rows()):
            # Check if 'GSM Project ID' is in any cell of this row
            if any(str(val).strip() == "GSM Project ID" for val in row_tuple if val is not None):
                header_idx = i
                break
                
        if header_idx is not None:
            # Extract that row to use as headers, stripping trailing spaces
            real_headers = [str(val).strip() if val is not None else f"unnamed_{j}" for j, val in enumerate(non_bci_df.row(header_idx))]
            
            # Polars requires unique column names. Ensure no duplicates just in case.
            seen = set()
            unique_headers = []
            for h in real_headers:
                new_h = h
                count = 1
                while new_h in seen:
                    new_h = f"{h}_{count}"
                    count += 1
                seen.add(new_h)
                unique_headers.append(new_h)

            non_bci_df = non_bci_df.rename(dict(zip(non_bci_df.columns, unique_headers)))
            
            # Slice the dataframe to keep only the data rows below the header
            non_bci_df = non_bci_df[header_idx + 1:]

    # the table in the excel file ends with a row that just states "Grand Total" in the "GSM Project ID" column, which is not a real project and doesn't have a valid ID
    # so we drop rows where the primary ID is completely null/empty or is "Grand Total"
    non_bci_df = non_bci_df.filter(
        pl.col("GSM Project ID").is_not_null() & 
        (pl.col("GSM Project ID").cast(pl.Utf8).str.strip_chars() != "") &
        (pl.col("GSM Project ID").cast(pl.Utf8).str.strip_chars() != "Grand Total")
    )

    non_bci_rows = non_bci_df.to_dicts()
    print(f"Non-BCI rows count: {len(non_bci_rows)}")
    print(f"Non-BCI: {non_bci_rows}")
    
    
    # Build embedding texts
    bci_texts = []
    for lead in filtered_bci_leads:
        text = " | ".join(filter(None, [
            str(lead.get("Project Address", "")),
            str(lead.get("Project Name", "")),
            str(lead.get("Project Type", "")),
        ]))
        bci_texts.append(text)

    print(f"BCI: {bci_texts}")

    # Non-BCI: project name + province (limited fields available)
    non_bci_texts = []
    for row in non_bci_rows:
        text = " | ".join(filter(None, [
            str(row.get("Project", "")),
            str(row.get("Province", "")),
        ]))
        non_bci_texts.append(text)

    token_provider = get_bearer_token_provider(
            identity.default_credential, "https://cognitiveservices.azure.com/.default")

    client = AsyncAzureOpenAI(
        api_version="2024-12-01-preview",
        azure_endpoint=app_settings.azure_openai_embedding_endpoint,
        azure_ad_token_provider=token_provider)

    embedding_model = app_settings.azure_openai_embedding_deployment

    # Batch embed (API limit ~2048 per call)
    async def get_embeddings(texts: list[str]) -> list[list[float]]:
        all_embeddings = []
        batch_size = 500
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            response = await client.embeddings.create(model=embedding_model, input=batch)
            all_embeddings.extend([item.embedding for item in response.data])
        return all_embeddings

    bci_embeddings = await get_embeddings(bci_texts)
    non_bci_embeddings = await get_embeddings(non_bci_texts)

    # Cosine similarity
    bci_matrix = np.array(bci_embeddings)
    non_bci_matrix = np.array(non_bci_embeddings)

    # Normalize
    bci_norm = bci_matrix / np.linalg.norm(bci_matrix, axis=1, keepdims=True)
    non_bci_norm = non_bci_matrix / np.linalg.norm(non_bci_matrix, axis=1, keepdims=True)

    # Similarity matrix: (num_non_bci x num_bci)
    similarity = non_bci_norm @ bci_norm.T

    # Find duplicates above threshold
    threshold = 0.8 # cross-lingual embeddings score lower (malay-english)
    duplicates = []
    for non_bci_idx in range(len(non_bci_rows)):
        for bci_idx in range(len(filtered_bci_leads)):
            score = float(similarity[non_bci_idx, bci_idx])

            #if filtered_bci_leads[bci_idx].get("Project ID") == "129285003" and non_bci_rows[non_bci_idx].get("GSM Project ID") == "129285003":
            #    print(f"DEBUG: Similarity is {score}")

            #if filtered_bci_leads[bci_idx].get("Project ID") == "90897003" and non_bci_rows[non_bci_idx].get("GSM Project ID") == "90897003":
            #    print(f"DEBUG: Similarity is {score}")

            if score >= threshold:
                duplicates.append({
                    "non_bci_id": str(non_bci_rows[non_bci_idx].get("GSM Project ID", "")),
                    "non_bci_project": str(non_bci_rows[non_bci_idx].get("Project", "")),
                    "non_bci_province": str(non_bci_rows[non_bci_idx].get("Province", "")),
                    "bci_id": str(filtered_bci_leads[bci_idx].get("Project ID", "")),
                    "bci_project": str(filtered_bci_leads[bci_idx].get("Project Name", "")),
                    "bci_address": str(filtered_bci_leads[bci_idx].get("Project Address", "")),
                    "bci_type": str(filtered_bci_leads[bci_idx].get("Project Type", "")),
                    "similarity": round(score, 4),
                })

    # Sort by similarity descending
    duplicates.sort(key=lambda x: x["similarity"], reverse=True)

    return {
        "duplicates": duplicates,
        "total_non_bci": len(non_bci_rows),
        "total_duplicates_found": len(duplicates),
    }
    