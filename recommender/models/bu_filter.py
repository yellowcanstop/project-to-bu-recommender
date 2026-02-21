from dataclasses import dataclass, field
from datetime import datetime
import re


@dataclass
class BUFilter:
    name: str
    category_keywords: dict[str, list[str]] = field(default_factory=dict)
    project_stage: list[str] = field(default_factory=list)
    regions: dict[str, list[str]] = field(default_factory=dict)
    allowed_regions: list[str] = field(default_factory=list)
    start_date_from: str | None = None
    end_date_to: str | None = None
    min_value: float = 0.0

    def matches(self, row: dict) -> bool:
        if not self._matches_value(row):
            return False
        if not self._matches_category(row):
            return False
        if not self._matches_region(row):
            return False
        if not self._matches_stage(row):
            return False
        if not self._matches_dates(row):
            return False
        return True

    def _matches_value(self, row: dict) -> bool:
        val_str = str(row.get("Local Value", "0"))
        cleaned = re.sub(r"[^\d.]", "", val_str)
        value = float(cleaned) if cleaned else 0.0
        return value >= self.min_value

    def _matches_category(self, row: dict) -> bool:
        if not self.category_keywords:
            return True

        cat_fields = []
        for i in range(1, 6):
            cat_fields.append(str(row.get(f"Category {i} Name", "")).lower())
        for i in range(1, 9):
            cat_fields.append(str(row.get(f"Sub-Category {i} Name", "")).lower())

        project_type_lower = str(row.get("Project Type", "")).lower()
        cat_text = " ".join(cat_fields) + " " + project_type_lower

        for keywords in self.category_keywords.values():
            if any(kw in cat_text for kw in keywords):
                return True
        return False

    def _matches_region(self, row: dict) -> bool:
        if not self.allowed_regions:
            return True

        province = str(row.get("Project Province / State", "")).lower()
        region = str(row.get("Project Region", "")).lower()

        for region_name in self.allowed_regions:
            region_states = self.regions.get(region_name, [])
            if any(state in province or state in region for state in region_states):
                return True
            if region_name.replace("_", " ") in region:
                return True
        return False

    def _matches_stage(self, row: dict) -> bool:
        if not self.project_stage:
            return True

        project_status = str(row.get("Project Status", "")).lower()
        project_stage = str(row.get("Project Stage", "")).lower()
        combined = project_status + " " + project_stage

        return any(stage in combined for stage in self.project_stage)

    def _matches_dates(self, row: dict) -> bool:
        if self.start_date_from:
            start = _parse_quarter_date(
                row.get("Construction Start Date (Original format)")
            )
            if start and start < datetime.strptime(self.start_date_from, "%Y-%m-%d"):
                return False

        if self.end_date_to:
            end = _parse_quarter_date(
                row.get("Construction End Date (Original format)")
            )
            if end and end > datetime.strptime(self.end_date_to, "%Y-%m-%d"):
                return False

        return True


def _parse_quarter_date(date_str: str | None) -> datetime | None:
    if not date_str or str(date_str).strip() == "":
        return None
    date_str = str(date_str).strip()

    q_match = re.match(r"Quarter\s*(\d),?\s*(\d{4})", date_str)
    if q_match:
        quarter = int(q_match.group(1))
        year = int(q_match.group(2))
        month = (quarter - 1) * 3 + 1
        return datetime(year, month, 1)

    try:
        return datetime.strptime(date_str, "%B %Y")
    except ValueError:
        return None