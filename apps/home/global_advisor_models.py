from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RecommendationCategory(str, Enum):
    DESIGN = "DESIGN"
    QUERY = "QUERY"
    INDEX = "INDEX"
    CONFIGURATION = "CONFIGURATION"
    STATISTICS = "STATISTICS"
    MAINTENANCE = "MAINTENANCE"
    SECURITY = "SECURITY"
    OTHER = "OTHER"


class ObjectType(str, Enum):
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"
    INDEX = "INDEX"
    COLUMN = "COLUMN"
    QUERY = "QUERY"
    CONFIG = "CONFIG"
    EXTENSION = "EXTENSION"
    ROLE = "ROLE"
    OTHER = "OTHER"


def build_default_title(rec: "GlobalRecommendation") -> str:
    label = rec.label or rec.source or rec.category_id.value

    object_name = (
        rec.object_name
        or rec.index_name
        or rec.table_name
        or rec.column_name
    )

    if object_name:
        return f"{label}: {object_name}"

    return label


def build_default_description(rec: "GlobalRecommendation") -> str:
    parts = []

    if rec.schema_name and rec.table_name:
        parts.append(f"Object: {rec.schema_name}.{rec.table_name}")
    elif rec.schema_name:
        parts.append(f"Schema: {rec.schema_name}")

    if rec.index_name:
        parts.append(f"Index: {rec.index_name}")

    if rec.column_name:
        parts.append(f"Column: {rec.column_name}")

    if rec.query_id:
        parts.append(f"Query ID: {rec.query_id}")

    return " | ".join(parts)


@dataclass
class GlobalRecommendation:
    # Ranking
    rank: int

    # Classification
    category_id: RecommendationCategory
    source: str

    # Main targeted object
    object_type: ObjectType
    object_id: Optional[int] = None
    object_name: Optional[str] = None

    label: Optional[str] = None

    # Readable context
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    index_name: Optional[str] = None
    column_name: Optional[str] = None

    # Internal identifiers
    schema_id: Optional[int] = None
    table_id: Optional[int] = None
    query_id: Optional[int] = None

    # Recommendation content
    title: str = ""
    description: str = ""
    recommendation_note: Optional[str] = None

    # Optional SQL action
    improvement_sql: Optional[str] = None

    # Scoring details
    confidence: Optional[int] = None
    impact: Optional[int] = None
    effort: Optional[int] = None

    # UX / safety
    manual_review_required: bool = True

    # Table size estimation
    estimated_rows: Optional[int] = None    

    def __post_init__(self) -> None:
        self.rank = max(0, min(100, int(self.rank)))

        if isinstance(self.category_id, str):
            self.category_id = RecommendationCategory(self.category_id)

        if isinstance(self.object_type, str):
            self.object_type = ObjectType(self.object_type)

        if not self.title:
            self.title = build_default_title(self)

        if not self.description:
            self.description = build_default_description(self)