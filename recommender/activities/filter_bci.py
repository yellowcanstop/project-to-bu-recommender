import azure.durable_functions as df
import polars as pl
import io
import re

from shared.identity import default_credential
from recommender.config.bu_filters import BU_FILTERS
from recommender.models.bu_filter import BUFilter

blueprint = df.Blueprint()

# polars read_excel does not support selective column loading (unlike read_csv)
# so load all initially and then select for needed columns (drop rest before converting to dict)
COLUMNS_TO_LOAD = [
    "Project ID",
    "Project Type",
    "Project Name",
    "Project Detail",
    "Local Value",
    "Category 1 Name",
    "Category 2 Name",
    "Category 3 Name",
    "Category 4 Name",
    "Category 5 Name",
    "Sub-Category 1 Name",
    "Sub-Category 2 Name",
    "Sub-Category 3 Name",
    "Sub-Category 4 Name",
    "Sub-Category 5 Name",
    "Sub-Category 6 Name",
    "Sub-Category 7 Name",
    "Sub-Category 8 Name",
    "Storeys",
    "Project Status",
    "Project Stage",
    "Construction Start Date (Original format)",
    "Construction End Date (Original format)",
    "Project Province / State",
    "Project Town / Suburb",
    "Project Region",
    "Project Address",
    "Owner Type Level 1 Primary",
    "Development Type",
]

def _build_bu_filter(name: str, config: dict) -> BUFilter:
    """Build a BUFilter dataclass from a config dictionary."""
    return BUFilter(
        name=name,
        subcategory=config.get("subcategory", []),
        project_status=config.get("project_status", []),
        project_state=config.get("project_state", []),
        development_type=config.get("development_type", []),
        start_date_min=config.get("start_date_min"),
        start_date_max=config.get("start_date_max"),
        end_date_min=config.get("end_date_min"),
        min_value=config.get("min_value", 0.0),
        subcategory_min_units=config.get("subcategory_min_units", {}),
        development_type_min_units=config.get("development_type_min_units", {}),
    )


@blueprint.activity_trigger(input_name="input_data")
async def filter_bci(input_data: dict) -> dict:
    from azure.storage.blob.aio import BlobServiceClient

    import os
    print(">>> Start filter bci...")

    # Fallback to local storage emulator if not provided
    blob_url_or_conn_str = input_data.get("blob_account_url", os.environ.get("AzureWebJobsStorage", "UseDevelopmentStorage=true"))
    container = input_data.get("container", "project-leads")
    bci_blob_name = input_data.get("bci_blob_name", "bci_leads.xlsx")

    # If it contains '=', it's a connection string (e.g., UseDevelopmentStorage=true)
    if "=" in blob_url_or_conn_str:
        blob_service = BlobServiceClient.from_connection_string(blob_url_or_conn_str)
    else:
        blob_service = BlobServiceClient(blob_url_or_conn_str, credential=default_credential())


    # Download BCI Excel from blob
    async with blob_service:
        blob_client = blob_service.get_blob_client(container, bci_blob_name)
        download = await blob_client.download_blob()
        content = await download.readall()

    df = pl.read_excel(io.BytesIO(content))
    available_columns = [c for c in COLUMNS_TO_LOAD if c in df.columns]
    df = df.select(available_columns)
    rows = df.to_dicts()

    bu_filters = {
        name: _build_bu_filter(name, config)
        for name, config in BU_FILTERS.items()
    }

    # Filter per BU
    bu_assignments: dict[str, list[str]] = {}
    all_matched_ids: set[str] = set()

    for bu_name, bu_filter in bu_filters.items():
        matched_ids = []
        for row in rows:
            if bu_filter.matches(row):
                project_id = str(row.get("Project ID", ""))
                matched_ids.append(project_id)
                all_matched_ids.add(project_id)
        bu_assignments[bu_name] = matched_ids

    # Build filtered leads list (union of all BU matches)
    filtered_leads = [
        row for row in rows 
        if str(row.get("Project ID", "")) in all_matched_ids
    ]

    return {
        "filtered_leads": filtered_leads,
        "bu_assignments": bu_assignments,
        "total_bci_rows": len(rows),
        "total_filtered": len(filtered_leads),
    }