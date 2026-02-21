from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Assignment:
    bu: str
    product: str
    reason: str
    opportunity: Literal["Immediate", "Future Cross-sell"]
    confidence: float = 0.0


@dataclass
class CrossBUBundle:
    bundle: str
    bus: list[str] = field(default_factory=list)
    products: list[str] = field(default_factory=list)


@dataclass
class ConflictResolution:
    conflict: str
    winner: str
    loser: str
    reason: str


@dataclass
class ProjectRecommendation:
    project_id: str
    project_name: str
    assignments: list[Assignment] = field(default_factory=list)
    cross_bu_bundles: list[CrossBUBundle] = field(default_factory=list)
    conflicts_resolved: list[ConflictResolution] = field(default_factory=list)