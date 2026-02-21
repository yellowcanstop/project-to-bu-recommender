import azure.durable_functions as df
import polars as pl
import io
import re
from datetime import datetime

from shared.identity import default_credential
from recommender.config.bu_filters import BU_FILTERS

blueprint = df.Blueprint()


def _parse_value(val_str: str) -> float:
    """Parse '2,000,000.00' -> 2000000.0"""
    if val_str is None:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", str(val_str))
    return float(cleaned) if cleaned else 0.0


def _parse_quarter_date(date_str: str) -> datetime | None:
    """Parse 'Quarter 4,2025' or 'January 2025' into a datetime."""
    if not date_str or str(date_str).strip() == "":
        return None
    date_str = str(date_str).strip()

    # Quarter format: "Quarter 4,2025"
    q_match = re.match(r"Quarter\s*(\d),?\s*(\d{4})", date_str)
    if q_match:
        quarter = int(q_match.group(1))
        year = int(q_match.group(2))
        month = (quarter - 1) * 3 + 1
        return datetime(year, month, 1)

    # Month Year format: "January 2025"
    try:
        return datetime.strptime(date_str, "%B %Y")
    except ValueError:
        return None


def _matches_bu_filter(row: dict, bu_filter: dict) -> bool:
    """Check if a project lead row matches a BU's filter criteria."""

    # Value filter
    value = _parse_value(row.get("Local Value"))
    if value < bu_filter.get("min_value", 0):
        return False

    # Category keyword filter
    cat_keywords = bu_filter.get("category_keywords", {})
    if cat_keywords:
        # Collect all category and sub-category values from the row
        cat_fields = []
        for i in range(1, 6):
            cat_fields.append(str(row.get(f"Category {i} Name", "")).lower())
            cat_fields.append(str(row.get(f"Sub-Category {i} Name", "")).lower())
        for i in range(6, 9):
            cat_fields.append(str(row.get(f"Sub-Category {i} Name", "")).lower())

        project_type_lower = str(row.get("Project Type", "")).lower()
        cat_text = " ".join(cat_fields) + " " + project_type_lower

        matched_any_category = False
        for category, keywords in cat_keywords.items():
            if any(kw in cat_text for kw in keywords):
                matched_any_category = True
                break
        if not matched_any_category:
            return False

    # Region filter
    allowed_regions = bu_filter.get("allowed_regions", [])
    if allowed_regions:
        region_map = bu_filter.get("regions", {})
        province = str(row.get("Project Province / State", "")).lower()
        region = str(row.get("Project Region", "")).lower()

        in_allowed_region = False
        for region_name in allowed_regions:
            region_states = region_map.get(region_name, [])
            if any(state in province or state in region for state in region_states):
                in_allowed_region = True
                break
            if region_name.replace("_", " ") in region:
                in_allowed_region = True
                break
        if not in_allowed_region:
            return False

    # Project stage filter
    allowed_stages = bu_filter.get("project_stage", [])
    if allowed_stages:
        project_status = str(row.get("Project Status", "")).lower()
        project_stage = str(row.get("Project Stage", "")).lower()
        combined_stage = project_status + " " + project_stage
        if not any(stage in combined_stage for stage in allowed_stages):
            return False

    # Date filter
    start_from = bu_filter.get("start_date_from")
    if start_from:
        start_date = _parse_quarter_date(row.get("Construction Start Date (Original format)"))
        if start_date and start_date < datetime.strptime(start_from, "%Y-%m-%d"):
            return False

    end_to = bu_filter.get("end_date_to")
    if end_to:
        end_date = _parse_quarter_date(row.get("Construction End Date (Original format)"))
        if end_date and end_date > datetime.strptime(end_to, "%Y-%m-%d"):
            return False

    return True


@blueprint.activity_trigger("filter_bci")
async def filter_bci_activity(input_data: dict) -> dict:
    from azure.storage.blob.aio import BlobServiceClient

    blob_url = input_data.get("blob_account_url")
    container = input_data.get("container", "project-leads")
    bci_blob_name = input_data.get("bci_blob_name", "bci_leads.xlsx")

    # Download BCI Excel from blob
    async with BlobServiceClient(blob_url, credential=default_credential()) as blob_service:
        blob_client = blob_service.get_blob_client(container, bci_blob_name)
        download = await blob_client.download_blob()
        content = await download.readall()

    # Load into Polars
    df = pl.read_excel(io.BytesIO(content))
    rows = df.to_dicts()

    # Filter per BU
    bu_assignments: dict[str, list[str]] = {}
    all_matched_ids: set[str] = set()

    for bu_name, bu_filter in BU_FILTERS.items():
        matched_ids = []
        for row in rows:
            if _matches_bu_filter(row, bu_filter):
                project_id = str(row.get("Project ID", ""))
                matched_ids.append(project_id)
                all_matched_ids.add(project_id)
        bu_assignments[bu_name] = matched_ids

    # Build filtered leads list (union of all BU matches)
    filtered_leads = [row for row in rows if str(row.get("Project ID", "")) in all_matched_ids]

    # Strip heavy columns not needed downstream (contractor columns etc.)
    keep_columns = [
        "Project ID", "Project Type", "Project Name", "Project Detail",
        "Local Value", "Category 1 Name", "Category 2 Name", "Category 3 Name",
        "Sub-Category 1 Name", "Sub-Category 2 Name", "Sub-Category 3 Name",
        "Storeys", "Project Status", "Project Stage",
        "Construction Start Date (Original format)",
        "Construction End Date (Original format)",
        "Project Province / State", "Project Town / Suburb",
        "Project Region", "Project Address",
        "Owner Type Level 1 Primary", "Development Type",
    ]
    filtered_leads = [
        {k: v for k, v in row.items() if k in keep_columns}
        for row in filtered_leads
    ]

    return {
        "filtered_leads": filtered_leads,
        "bu_assignments": bu_assignments,
        "total_bci_rows": len(rows),
        "total_filtered": len(filtered_leads),
    }