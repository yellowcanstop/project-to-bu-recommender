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
import fastexcel

blueprint = df.Blueprint()

chat_client = AsyncAzureOpenAI(
    azure_endpoint=app_settings.azure_openai_endpoint,
    api_version="2024-12-01-preview",
    azure_ad_token_provider=get_bearer_token_provider(default_credential, "https://cognitiveservices.azure.com/.default"),
)

@blueprint.activity_trigger(input_name="input_data")
async def deduplicate(input_data: dict) -> dict:
    filtered_bci_leads = input_data["filtered_bci_leads"]

    # Download non-BCI file
    blob_url = input_data.get("blob_account_url") or app_settings.blob_account_url
    container = input_data.get("container") or app_settings.blob_container
    non_bci_blob = input_data.get("non_bci_blob_name")

    if "UseDevelopmentStorage=true" in blob_url or "DefaultEndpointsProtocol" in blob_url:
        blob_service = BlobServiceClient.from_connection_string(blob_url)
    else:
        blob_service = BlobServiceClient(blob_url, credential=default_credential)

    async with blob_service:
        blob_client = blob_service.get_blob_client(container, non_bci_blob)
        download = await blob_client.download_blob()
        content = await download.readall()

    # non-bci file has multiple sheets with the same table so we need to get names of all sheets and merge them
    f = fastexcel.read_excel(content)
    sheet_names = f.sheet_names
    dfs = []
    excel_data = io.BytesIO(content)
    for sheet in sheet_names:
        raw = pl.read_excel(
            excel_data,
            sheet_name=sheet,
            has_header=False
        )
        normalized = find_and_normalize(raw, sheet)
        if normalized is not None:
            dfs.append(normalized)

    non_bci_df = pl.concat(dfs)

    # the table in the excel file ends with a row that just states "Grand Total" in the "GSM Project ID" column, which is not a real project and doesn't have a valid ID
    # we cannot drop rows where the primary ID is null/empty because some rows have missing GSM Project ID but still contain valid project name and province
    # so we drop rows where the primary ID is "Grand Total"
    non_bci_df = non_bci_df.filter(
        (pl.col("GSM Project ID").cast(pl.Utf8).str.strip_chars() != "Grand Total")
    )

    non_bci_rows = non_bci_df.to_dicts()    

    print(f"Filtered BCI leads: {len(filtered_bci_leads)}, Non-BCI rows: {len(non_bci_rows)}")
    
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
            str(row.get("source_sheet", "")),
        ]))
        non_bci_texts.append(text)

    # Embedding Client (for deduplication)
    embedding_client = AsyncAzureOpenAI(
        azure_endpoint=app_settings.azure_openai_embedding_endpoint,
        api_version="2024-12-01-preview",
        azure_ad_token_provider=get_bearer_token_provider(default_credential, "https://cognitiveservices.azure.com/.default"),
    )

    embedding_model = app_settings.azure_openai_embedding_deployment

    # Batch embed (API limit ~2048 per call)
    async def get_embeddings(texts: list[str]) -> list[list[float]]:
        all_embeddings = []
        batch_size = 500
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            response = await embedding_client.embeddings.create(model=embedding_model, input=batch)
            all_embeddings.extend([item.embedding for item in response.data])
        return all_embeddings

    try:
        bci_embeddings = await get_embeddings(bci_texts)
        non_bci_embeddings = await get_embeddings(non_bci_texts)
    finally:
        await embedding_client.close()

    # Cosine similarity
    bci_matrix = np.array(bci_embeddings)
    non_bci_matrix = np.array(non_bci_embeddings)

    # Normalize
    bci_norm = bci_matrix / np.linalg.norm(bci_matrix, axis=1, keepdims=True)
    non_bci_norm = non_bci_matrix / np.linalg.norm(non_bci_matrix, axis=1, keepdims=True)

    # Similarity matrix: (num_non_bci x num_bci)
    similarity = non_bci_norm @ bci_norm.T

    # Find duplicates above threshold
    threshold = 0.5 # cross-lingual embeddings score lower (malay-english)
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

def find_and_normalize(df: pl.DataFrame, source_sheet: str) -> pl.DataFrame:
    """
    Locates the header row, standardizes column names, and ensures a 
    consistent schema across different Excel sheets.
    """
    header_idx = None
    for i, row_tuple in enumerate(df.iter_rows()):
        if any(str(val).strip() == "GSM Project ID" for val in row_tuple if val is not None):
            header_idx = i
            break

    if header_idx is None:
        return None

    raw_headers = [
        str(val).strip() if val is not None else f"unnamed_{j}" 
        for j, val in enumerate(df.row(header_idx))
    ]

    df = df.rename(dict(zip(df.columns, raw_headers)))
    df = df.slice(header_idx + 1)

    target_cols = ["GSM Project ID", "Project", "Province"]
    available_cols = [c for c in target_cols if c in df.columns]
    df = df.select(available_cols)

    # if a sheet is missing some of the target columns, add them with null values to maintain a consistent schema
    for col in target_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    return df.with_columns(pl.lit(source_sheet).alias("source_sheet"))