import azure.durable_functions as df
import polars as pl
import io
import numpy as np

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
    bci_blob_name = input_data.get("bci_blob_name")

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
    rejection_map = {} # New: {project_id: {bu_name: "Reason"}}
    all_matched_ids: set[str] = set()

    for bu_name, bu_filter in bu_filters.items():
        matched_ids = []
        for row in rows:
            project_id = str(row.get("Project ID", ""))
            if bu_filter.matches(row):
                matched_ids.append(project_id)
                all_matched_ids.add(project_id)
            else:
                # If the lead passed AT LEAST ONE other BU, we want to know why THIS BU rejected it
                reason = _get_rejection_reason(bu_filter, row)
                if project_id not in rejection_map:
                    rejection_map[project_id] = {}
                rejection_map[project_id][bu_name] = reason
        bu_assignments[bu_name] = matched_ids

    # Build filtered leads list (union of all BU matches)
    filtered_leads = [
        row for row in rows 
        if str(row.get("Project ID", "")) in all_matched_ids
    ]

    # Final cleanup: only keep rejection reasons for leads that actually made it into the pipeline
    filtered_rejection_map = {
        pid: rejections for pid, rejections in rejection_map.items() 
        if pid in all_matched_ids
    }

    def clean_for_json(obj):
        if isinstance(obj, (np.float64, np.float32)):
            return float(obj) if not np.isnan(obj) else None
        if isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        return obj

    # Sanitize leads before returning
    sanitized_leads = [
        {k: clean_for_json(v) for k, v in row.items()} 
        for row in filtered_leads
    ]

    return {
        "filtered_leads": sanitized_leads,
        "bu_assignments": bu_assignments,
        "rejection_map": filtered_rejection_map,
        "total_bci_rows": len(rows),
        "total_filtered": len(sanitized_leads),
    }


def _get_rejection_reason(bu_filter: BUFilter, row: dict) -> str:
    """Helper to identify the first failing criteria for diagnostics."""
    
    # 1. Check Project Value
    if not bu_filter._matches_value(row):
        val_str = str(row.get("Local Value") or "0")
        return f"Value too low: {val_str} (Target: >={bu_filter.min_value})"

    # 2. Check Subcategory (Fuzzy Match)
    if not bu_filter._matches_subcategory(row):
        row_subcats = [str(row.get(f"Sub-Category {i} Name") or "").strip() for i in range(1, 9)]
        row_subcats = [s for s in row_subcats if s]
        return f"Sub-Category mismatch. Found: {row_subcats}. Expected one of: {bu_filter.subcategory}"

    # 3. Check Province / State
    if not bu_filter._matches_state(row):
        province = str(row.get("Project Province / State") or "N/A").strip()
        return f"Region mismatch: '{province}' not in {bu_filter.project_state}"

    # 4. Check Project Status (e.g., Tenders, Design)
    if not bu_filter._matches_status(row):
        status = str(row.get("Project Status") or "N/A").strip()
        return f"Status mismatch: '{status}' not accepted for this BU."

    # 5. Check Dates (Year parsing)
    if not bu_filter._matches_dates(row):
        start = row.get("Construction Start Date (Original format)")
        end = row.get("Construction End Date (Original format)")
        return f"Date outside range (Start: {start}, End: {end})"

    # 6. Check Development Type
    if not bu_filter._matches_development_type(row):
        dev_type = str(row.get("Development Type") or "N/A").strip()
        return f"Dev Type mismatch: '{dev_type}' vs required {bu_filter.development_type}"

    # 7. Check Unit Minimums (Complex Regex Checks)
    if not bu_filter._matches_unit_minimums(row):
        project_type = str(row.get("Project Type") or "N/A")
        # Determine if it was a subcat-specific unit fail or a total unit fail
        if bu_filter.subcategory_min_units:
             return f"Insufficient units for specific category in Project Type: '{project_type}'"
        return f"Total units below minimum requirement. Project Type: '{project_type}'"

    return "Unknown filter rejection"