from dataclasses import dataclass, field


@dataclass
class ProjectLead:
    project_id: str
    project_type: str = ""
    project_name: str = ""
    project_detail: str = ""
    local_value: float = 0.0
    category_1: str = ""
    category_2: str = ""
    category_3: str = ""
    sub_category_1: str = ""
    sub_category_2: str = ""
    sub_category_3: str = ""
    storeys: str = ""
    project_status: str = ""
    project_stage: str = ""
    construction_start: str = ""
    construction_end: str = ""
    province: str = ""
    town: str = ""
    region: str = ""
    address: str = ""
    owner_type: str = ""
    development_type: str = ""

    @staticmethod
    def from_row(row: dict) -> "ProjectLead":
        import re
        val_str = str(row.get("Local Value", "0"))
        cleaned = re.sub(r"[^\d.]", "", val_str)
        value = float(cleaned) if cleaned else 0.0

        return ProjectLead(
            project_id=str(row.get("Project ID", "")),
            project_type=str(row.get("Project Type", "")),
            project_name=str(row.get("Project Name", "")),
            project_detail=str(row.get("Project Detail", "")),
            local_value=value,
            category_1=str(row.get("Category 1 Name", "")),
            category_2=str(row.get("Category 2 Name", "")),
            category_3=str(row.get("Category 3 Name", "")),
            sub_category_1=str(row.get("Sub-Category 1 Name", "")),
            sub_category_2=str(row.get("Sub-Category 2 Name", "")),
            sub_category_3=str(row.get("Sub-Category 3 Name", "")),
            storeys=str(row.get("Storeys", "")),
            project_status=str(row.get("Project Status", "")),
            project_stage=str(row.get("Project Stage", "")),
            construction_start=str(row.get("Construction Start Date (Original format)", "")),
            construction_end=str(row.get("Construction End Date (Original format)", "")),
            province=str(row.get("Project Province / State", "")),
            town=str(row.get("Project Town / Suburb", "")),
            region=str(row.get("Project Region", "")),
            address=str(row.get("Project Address", "")),
            owner_type=str(row.get("Owner Type Level 1 Primary", "")),
            development_type=str(row.get("Development Type", "")),
        )

    @property
    def clean_narrative(self) -> str:
        detail = self.project_detail
        marker = "Building elements include:"
        if marker in detail:
            return detail.split(marker)[0].strip()
        marker_lower = "building elements include:"
        if marker_lower in detail.lower():
            idx = detail.lower().index(marker_lower)
            return detail[:idx].strip()
        return detail.strip()

    def to_context_dict(self) -> dict:
        return {
            "project_id": self.project_id,
            "project_type": self.project_type,
            "project_name": self.project_name,
            "project_narrative": self.clean_narrative,
            "value": self.local_value,
            "category_1": self.category_1,
            "category_2": self.category_2,
            "category_3": self.category_3,
            "sub_category_1": self.sub_category_1,
            "sub_category_2": self.sub_category_2,
            "sub_category_3": self.sub_category_3,
            "storeys": self.storeys,
            "project_status": self.project_status,
            "project_stage": self.project_stage,
            "construction_start": self.construction_start,
            "construction_end": self.construction_end,
            "region": self.region,
            "province": self.province,
            "owner_type": self.owner_type,
            "development_type": self.development_type,
        }