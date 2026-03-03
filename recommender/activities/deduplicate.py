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
    api_key=app_settings.azure_openai_key
)

@blueprint.activity_trigger(input_name="input_data")
async def deduplicate(input_data: dict) -> dict:
    filtered_bci_leads = input_data["filtered_bci_leads"]

    bci_df = pl.DataFrame(filtered_bci_leads)

    # Download non-BCI file
    blob_url = input_data.get("blob_account_url") or app_settings.blob_account_url
    container = input_data.get("container") or app_settings.blob_container
    non_bci_blob = input_data.get("non_bci_blob_name")

    blob_service = BlobServiceClient.from_connection_string(app_settings.blob_account_url)

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

    # the table in the excel file ends with a row that just states "Grand Total" in the "GSM Project ID" column, which is not a real project and doesn't have a valid ID
    # we cannot drop rows where the primary ID is null/empty because some rows have missing GSM Project ID but still contain valid project name and province
    # so we drop rows where the primary ID is "Grand Total"
    non_bci_df = pl.concat(dfs).filter(
        (pl.col("GSM Project ID").cast(pl.Utf8).str.strip_chars() != "Grand Total")
    )

    bci_texts = (
        bci_df.select([
            pl.format("{} | {} | {}", 
                      pl.col("Project Address").fill_null(""),
                      pl.col("Project Name").fill_null(""),
                      pl.col("Project Type").fill_null(""))
        ]).to_series().to_list()
    )

    non_bci_texts = (
        non_bci_df.select([
            pl.format("{} | {} | {}", 
                      pl.col("Project").fill_null(""),
                      pl.col("Province").fill_null(""),
                      pl.col("source_sheet").fill_null(""))
        ]).to_series().to_list()
    )

    # Embedding Client (for deduplication)
    embedding_client = AsyncAzureOpenAI(
        azure_endpoint=app_settings.azure_openai_embedding_endpoint,
        api_version="2024-12-01-preview",
        api_key=app_settings.azure_openai_key
    )

    embedding_model = app_settings.azure_openai_embedding_deployment

    # Batch embed (API limit ~2048 per call)
    async def get_embeddings(texts: list[str]) -> np.ndarray:
        all_embeddings = []
        batch_size = 500
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            response = await embedding_client.embeddings.create(model=embedding_model, input=batch)
            all_embeddings.extend([item.embedding for item in response.data])
        return np.array(all_embeddings)

    try:
        bci_matrix = await get_embeddings(bci_texts)
        non_bci_matrix = await get_embeddings(non_bci_texts)
    finally:
        await embedding_client.close()

    # 5. Vectorized Similarity Calculation
    # Normalize
    bci_norm = bci_matrix / np.linalg.norm(bci_matrix, axis=1, keepdims=True)
    non_bci_norm = non_bci_matrix / np.linalg.norm(non_bci_matrix, axis=1, keepdims=True)

    # Similarity matrix: (M_non_bci x N_bci)
    similarity = non_bci_norm @ bci_norm.T

    # 6. VECTORIZED INDEXING (The Loop Replacement)
    threshold = 0.6
    # Find coordinates where similarity >= threshold
    non_bci_indices, bci_indices = np.where(similarity >= threshold)
    
    # Get the actual scores for those coordinates
    relevant_scores = similarity[non_bci_indices, bci_indices]

    # Create the result by gathering rows from the dataframes
    # This avoids iterating over the full original dataframes
    if len(non_bci_indices) > 0:
        # Pull matching rows from Non-BCI
        dupe_non_bci = non_bci_df[non_bci_indices].select([
            pl.col("GSM Project ID").alias("non_bci_id"),
            pl.col("Project").alias("non_bci_project"),
            pl.col("Province").alias("non_bci_province")
        ])

        # Pull matching rows from BCI
        dupe_bci = bci_df[bci_indices].select([
            pl.col("Project ID").alias("bci_id"),
            pl.col("Project Name").alias("bci_project"),
            pl.col("Project Address").alias("bci_address"),
            pl.col("Project Type").alias("bci_type")
        ])

        # Combine them horizontally and add the similarity score
        duplicates_df = pl.concat([dupe_non_bci, dupe_bci], how="horizontal")
        duplicates_df = duplicates_df.with_columns(
            pl.lit(relevant_scores).round(4).alias("similarity")
        ).sort("similarity", descending=True)

        duplicates = duplicates_df.to_dicts()
    else:
        duplicates = []

    return {
        "duplicates": duplicates,
        "total_non_bci": len(non_bci_df),
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