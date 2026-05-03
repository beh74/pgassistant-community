from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any


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


class AdvisorOutcome(str, Enum):
    PERFORMANCE = "PERFORMANCE"
    RELIABILITY = "RELIABILITY"
    MAINTAINABILITY = "MAINTAINABILITY"
    OBSERVABILITY = "OBSERVABILITY"
    OPERABILITY = "OPERABILITY"
    STORAGE = "STORAGE"
    SECURITY = "SECURITY"
    OTHER = "OTHER"


class AdvisorGroup(str, Enum):
    TOP_PRIORITIES = "TOP_PRIORITIES"
    QUICK_WINS = "QUICK_WINS"
    DATA_MODEL_ISSUES = "DATA_MODEL_ISSUES"
    INDEX_CLEANUP = "INDEX_CLEANUP"
    INDEXING_OPPORTUNITIES = "INDEX_OPPORTUNITIES"
    MAINTENANCE_RISKS = "MAINTENANCE_RISKS"
    CONFIGURATION_ISSUES = "CONFIGURATION_ISSUES"
    OBSERVABILITY_GAPS = "OBSERVABILITY_GAPS"
    STORAGE_RISKS = "STORAGE_RISKS"
    OTHER = "OTHER"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    UNKNOWN = "UNKNOWN"


class ActionType(str, Enum):
    CREATE_INDEX = "CREATE_INDEX"
    DROP_INDEX = "DROP_INDEX"
    ALTER_TABLE = "ALTER_TABLE"
    VACUUM = "VACUUM"
    ANALYZE = "ANALYZE"
    CONFIG_CHANGE = "CONFIG_CHANGE"
    REVIEW_ONLY = "REVIEW_ONLY"
    OTHER = "OTHER"


class ActionSafety(str, Enum):
    SAFE_TO_APPLY = "SAFE_TO_APPLY"
    SAFE_TO_REVIEW = "SAFE_TO_REVIEW"
    MANUAL_ONLY = "MANUAL_ONLY"
    UNSAFE = "UNSAFE"
    UNKNOWN = "UNKNOWN"


class PriorityLevel(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


def enum_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    return value


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


def compute_priority(rank: int) -> PriorityLevel:
    if rank >= 80:
        return PriorityLevel.HIGH
    if rank >= 50:
        return PriorityLevel.MEDIUM
    return PriorityLevel.LOW


@dataclass
class GlobalRecommendation:
    # Ranking
    rank: int

    # Classification
    category_id: RecommendationCategory
    source: str

    # Stable recommendation identifier from YAML catalog
    recommendation_id: Optional[str] = None

    # Advisor classification metadata
    outcome_id: AdvisorOutcome = AdvisorOutcome.OTHER
    advisor_group: AdvisorGroup = AdvisorGroup.OTHER

    # Main targeted object
    object_type: ObjectType = ObjectType.OTHER
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
    why_it_matters: Optional[str] = None
    fix_strategy: Optional[str] = None
    expected_benefit: Optional[str] = None

    # Optional SQL action
    improvement_sql: Optional[str] = None

    # Scoring details
    confidence: Optional[int] = None
    impact: Optional[int] = None
    effort: Optional[int] = None
    priority: PriorityLevel = PriorityLevel.LOW

    # UX / safety
    manual_review_required: bool = True
    risk_level: RiskLevel = RiskLevel.UNKNOWN
    action_type: ActionType = ActionType.OTHER
    action_safety: ActionSafety = ActionSafety.UNKNOWN
    requires_lock: bool = False
    requires_maintenance_window: bool = False
    can_generate_sql: bool = True
    can_auto_apply: bool = False
    tags: List[str] = field(default_factory=list)

    # Table size estimation
    estimated_rows: Optional[int] = None

    def __post_init__(self) -> None:
        self.rank = max(0, min(100, int(self.rank)))

        if isinstance(self.category_id, str):
            self.category_id = RecommendationCategory(self.category_id)

        if isinstance(self.object_type, str):
            self.object_type = ObjectType(self.object_type)

        if isinstance(self.outcome_id, str):
            self.outcome_id = AdvisorOutcome(self.outcome_id)

        if isinstance(self.advisor_group, str):
            self.advisor_group = AdvisorGroup(self.advisor_group)

        if isinstance(self.risk_level, str):
            self.risk_level = RiskLevel(self.risk_level)

        if isinstance(self.action_type, str):
            self.action_type = ActionType(self.action_type)

        if isinstance(self.action_safety, str):
            self.action_safety = ActionSafety(self.action_safety)

        self.priority = compute_priority(self.rank)

        if not self.title:
            self.title = build_default_title(self)

        if not self.description:
            self.description = build_default_description(self)

    def to_dict(self) -> Dict[str, Any]:
        """Useful when returning JSON from Flask."""
        return {
            "rank": self.rank,
            "priority": enum_value(self.priority),
            "category_id": enum_value(self.category_id),
            "source": self.source,
            "recommendation_id": self.recommendation_id,
            "outcome_id": enum_value(self.outcome_id),
            "advisor_group": enum_value(self.advisor_group),
            "object_type": enum_value(self.object_type),
            "object_id": self.object_id,
            "object_name": self.object_name,
            "label": self.label,
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "index_name": self.index_name,
            "column_name": self.column_name,
            "schema_id": self.schema_id,
            "table_id": self.table_id,
            "query_id": self.query_id,
            "title": self.title,
            "description": self.description,
            "recommendation_note": self.recommendation_note,
            "why_it_matters": self.why_it_matters,
            "fix_strategy": self.fix_strategy,
            "expected_benefit": self.expected_benefit,
            "improvement_sql": self.improvement_sql,
            "confidence": self.confidence,
            "impact": self.impact,
            "effort": self.effort,
            "manual_review_required": self.manual_review_required,
            "risk_level": enum_value(self.risk_level),
            "action_type": enum_value(self.action_type),
            "action_safety": enum_value(self.action_safety),
            "requires_lock": self.requires_lock,
            "requires_maintenance_window": self.requires_maintenance_window,
            "can_generate_sql": self.can_generate_sql,
            "can_auto_apply": self.can_auto_apply,
            "tags": self.tags,
            "estimated_rows": self.estimated_rows,
        }
