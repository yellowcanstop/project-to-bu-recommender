import azure.durable_functions as df
import polars as pl
import io

from shared.identity import default_credential
from shared import app_settings
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
   
    # Get connection string or URL from input, fallback to app_settings
    blob_url = input_data.get("blob_account_url") or app_settings.blob_account_url
    container = input_data.get("container") or app_settings.blob_container
    bci_blob_name = input_data.get("bci_blob_name") or app_settings.bci_blob_name

    if "UseDevelopmentStorage=true" in blob_url or "DefaultEndpointsProtocol" in blob_url:
        blob_service = BlobServiceClient.from_connection_string(blob_url)
    else:
        blob_service = BlobServiceClient(blob_url, credential=default_credential)


    # Download BCI Excel from blob
    async with blob_service:
        blob_client = blob_service.get_blob_client(container, bci_blob_name)
        download = await blob_client.download_blob()
        content = await download.readall()

    df = pl.read_excel(io.BytesIO(content))

    # The excel file may have multiple header rows or leading metadata before actual header we want (which contains "Project ID").
    # Strip whitespace from existing column names just in case it read correctly
    df = df.rename({c: str(c).strip() for c in df.columns})

    # If 'Project ID' isn't in the headers, search all rows/columns to find the true header row
    if "Project ID" not in df.columns:
        header_idx = None
        
        # iter_rows() yields tuples of the row values
        for i, row_tuple in enumerate(df.iter_rows()):
            # Check if 'Project ID' is in any cell of this row
            if any(str(val).strip() == "Project ID" for val in row_tuple if val is not None):
                header_idx = i
                break
        
        if header_idx is not None:
            # Extract that row to use as headers, stripping trailing spaces
            real_headers = [str(val).strip() if val is not None else f"unnamed_{j}" for j, val in enumerate(df.row(header_idx))]
            
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

            df = df.rename(dict(zip(df.columns, unique_headers)))
            
            # Slice the dataframe to keep only the data rows below the header
            df = df[header_idx + 1:]

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