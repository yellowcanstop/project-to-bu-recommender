import azure.durable_functions as df
import json
import os
import io

from shared.identity import default_credential

blueprint = df.Blueprint()


@blueprint.activity_trigger("deduplicate")
async def deduplicate_activity(input_data: dict) -> dict:
    from azure.storage.blob.aio import BlobServiceClient
    from openai import AsyncAzureOpenAI
    import polars as pl
    import numpy as np

    filtered_bci_leads = input_data["filtered_bci_leads"]

    # Download non-BCI file
    blob_url = os.environ["BLOB_ACCOUNT_URL"]
    container = os.environ.get("BLOB_CONTAINER", "project-leads")
    non_bci_blob = os.environ.get("NON_BCI_BLOB_NAME", "non_bci_leads.xlsx")

    async with BlobServiceClient(blob_url, credential=default_credential()) as blob_service:
        blob_client = blob_service.get_blob_client(container, non_bci_blob)
        download = await blob_client.download_blob()
        content = await download.readall()

    non_bci_df = pl.read_excel(io.BytesIO(content))
    non_bci_rows = non_bci_df.to_dicts()

    # Build embedding texts
    # BCI: address + project type + province
    bci_texts = []
    for lead in filtered_bci_leads:
        text = " | ".join(filter(None, [
            str(lead.get("Project Address", "")),
            str(lead.get("Project Type", "")),
            str(lead.get("Project Province / State", "")),
        ]))
        bci_texts.append(text)

    # Non-BCI: project name + province (limited fields available)
    non_bci_texts = []
    for row in non_bci_rows:
        text = " | ".join(filter(None, [
            str(row.get("Project", "")),
            str(row.get("Province", "")),
        ]))
        non_bci_texts.append(text)

    # Get embeddings from Azure OpenAI
    client = AsyncAzureOpenAI(
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_version="2024-12-01-preview",
        azure_ad_token_provider=default_credential(),
    )
    embedding_model = os.environ.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-3-small")

    # Batch embed (API limit ~2048 per call)
    async def get_embeddings(texts: list[str]) -> list[list[float]]:
        all_embeddings = []
        batch_size = 100
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
    threshold = 0.85
    duplicates = []
    for non_bci_idx in range(len(non_bci_rows)):
        for bci_idx in range(len(filtered_bci_leads)):
            score = float(similarity[non_bci_idx, bci_idx])
            if score >= threshold:
                duplicates.append({
                    "non_bci_id": str(non_bci_rows[non_bci_idx].get("GSM Project ID", "")),
                    "non_bci_project": str(non_bci_rows[non_bci_idx].get("Project", "")),
                    "non_bci_province": str(non_bci_rows[non_bci_idx].get("Province", "")),
                    "bci_id": str(filtered_bci_leads[bci_idx].get("Project ID", "")),
                    "bci_project": str(filtered_bci_leads[bci_idx].get("Project Name", "")),
                    "bci_address": str(filtered_bci_leads[bci_idx].get("Project Address", "")),
                    "similarity": round(score, 4),
                })

    # Sort by similarity descending
    duplicates.sort(key=lambda x: x["similarity"], reverse=True)

    return {
        "duplicates": duplicates,
        "total_non_bci": len(non_bci_rows),
        "total_duplicates_found": len(duplicates),
    }