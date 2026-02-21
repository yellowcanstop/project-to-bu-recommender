from dataclasses import dataclass, field
import re


@dataclass
class BUFilter:
    name: str
    subcategory: list[str] = field(default_factory=list)
    project_status: list[str] = field(default_factory=list)
    project_state: list[str] = field(default_factory=list)
    development_type: list[str] = field(default_factory=list)
    start_date_min: str | None = None  # only year matters
    start_date_max: str | None = None
    end_date_min: str | None = None
    min_value: float = 0.0
    subcategory_min_units: dict[str, int] = field(default_factory=dict)
    development_type_min_units: dict[str, int] = field(default_factory=dict)

    def matches(self, row: dict) -> bool:
        if not self._matches_value(row):
            return False
        if not self._matches_subcategory(row):
            return False
        if not self._matches_state(row):
            return False
        if not self._matches_status(row):
            return False
        if not self._matches_dates(row):
            return False
        if not self._matches_development_type(row):
            return False
        if not self._matches_unit_minimums(row):
            return False
        return True

    def _matches_value(self, row: dict) -> bool:
        if self.min_value <= 0:
            return True
        val_str = str(row.get("Local Value") or "0")
        cleaned = re.sub(r"[^\d.]", "", val_str)
        value = float(cleaned) if cleaned else 0.0
        return value >= self.min_value

    def _matches_subcategory(self, row: dict) -> bool:
        if not self.subcategory:
            return True

        row_subcats: list[str] = []
        for i in range(1, 9):
            val = str(row.get(f"Sub-Category {i} Name") or "").strip().lower()
            if val:
                row_subcats.append(val)

        if not row_subcats:
            return False

        return any(
            sub.lower() in row_subcats
            for sub in self.subcategory
        )

    def _matches_state(self, row: dict) -> bool:
        if not self.project_state:
            return True

        province = str(row.get("Project Province / State") or "").strip().lower()
        if not province:
            return False

        return any(
            state.lower() == province
            for state in self.project_state
        )

    def _matches_status(self, row: dict) -> bool:
        if not self.project_status:
            return True

        status = str(row.get("Project Status") or "").strip().lower()
        if not status:
            return False

        return any(
            s.lower() == status
            for s in self.project_status
        )

    def _matches_development_type(self, row: dict) -> bool:
        if not self.development_type:
            return True

        dev_type = str(row.get("Development Type") or "").strip().lower()
        if not dev_type:
            return False

        return any(
            dt.lower() == dev_type
            for dt in self.development_type
        )

    def _matches_dates(self, row: dict) -> bool:
        if self.start_date_min or self.start_date_max:
            start_year = _parse_year(
                row.get("Construction Start Date (Original format)")
            )
            if start_year is not None:
                if self.start_date_min:
                    min_year = int(self.start_date_min[:4])
                    if start_year < min_year:
                        return False
                if self.start_date_max:
                    max_year = int(self.start_date_max[:4])
                    if start_year > max_year:
                        return False

        if self.end_date_min:
            end_year = _parse_year(
                row.get("Construction End Date (Original format)")
            )
            if end_year is not None:
                min_year = int(self.end_date_min[:4])
                if end_year < min_year:
                    return False

        return True

    def _matches_unit_minimums(self, row: dict) -> bool:
        """
        Check subcategory_min_units and development_type_min_units.
        E.g. if subcategory_min_units = {"hotel": 100}, then for rows whose
        sub-category includes "hotel", the Project Type must contain a hotel
        entry with >= 100 units like 'HOTEL (329 rooms)'.
        Similarly for development_type_min_units.
        """
        project_type = str(row.get("Project Type") or "").strip()

        # Check subcategory_min_units
        if self.subcategory_min_units:
            row_subcats = set()
            for i in range(1, 9):
                val = str(row.get(f"Sub-Category {i} Name") or "").strip().lower()
                if val:
                    row_subcats.add(val)

            for subcat, min_units in self.subcategory_min_units.items():
                if subcat.lower() in row_subcats:
                    units = _extract_units(project_type, subcat)
                    if units is not None and units < min_units:
                        return False

        # Check development_type_min_units
        if self.development_type_min_units:
            dev_type = str(row.get("Development Type") or "").strip().lower()
            for dt, min_units in self.development_type_min_units.items():
                if dt.lower() == dev_type:
                    # For development type, extract total units from Project Type
                    total = _extract_total_units(project_type)
                    if total is not None and total < min_units:
                        return False

        return True


def _parse_year(date_str: str | None) -> int | None:
    """Extract year from 'Quarter 4,2025' or 'January 2025'."""
    if not date_str or str(date_str).strip() == "":
        return None
    date_str = str(date_str).strip()

    # Quarter format: "Quarter 4,2025"
    q_match = re.match(r"Quarter\s*(\d),?\s*(\d{4})", date_str)
    if q_match:
        return int(q_match.group(2))

    # Month Year format: "January 2025"
    m_match = re.match(r"[A-Za-z]+\s+(\d{4})", date_str)
    if m_match:
        return int(m_match.group(1))

    return None


def _extract_units(project_type: str, keyword: str) -> int | None:
    """
    Extract unit count for a specific keyword from Project Type to check minimum unit requirement for subcategory.
    E.g. 'HOTEL (329 rooms)' with keyword 'hotel' -> 329
         'HOTEL (34 rooms) - extension - 7 storey' with keyword 'hotel' -> 34
         'APARTMENT, CONDOMINIUM, TOWNHOUSE (500)' with keyword
            'apartment, condominium, townhouse' -> 500
    """
    # Pattern: KEYWORD (NUMBER ...) — keyword can be multi-word
    # Build a flexible pattern: keyword followed by (number ...)
    escaped = re.escape(keyword)
    pattern = rf"(?i){escaped}\s*\(\s*(\d[\d,]*)"
    match = re.search(pattern, project_type)
    if match:
        return int(match.group(1).replace(",", ""))
    return None


def _extract_total_units(project_type: str) -> int | None:
    """
    Extract total units from all entries in Project Type to check minimum unit requirement for development type.
    E.g. 'HOTEL (329 rooms) | SOHO (245) | OFFICES (223) - new - 55 storey'
         -> 329 + 245 + 223 = 797
    """
    matches = re.findall(r"\(\s*(\d[\d,]*)\s*(?:rooms|units)?\s*\)", project_type, re.IGNORECASE)
    if not matches:
        return None
    total = sum(int(m.replace(",", "")) for m in matches)
    return total if total > 0 else None