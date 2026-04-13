from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any, cast

from pydantic import BaseModel, Field, model_validator


class ValueKind(str, Enum):
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    STRING_LIST = "string_list"
    GEOMETRY_WKT = "geometry_wkt"
    GEOMETRY_GEOJSON = "geometry_geojson"


class Operator(str, Enum):
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    IN_SET = "in_set"
    NOT_IN_SET = "not_in_set"
    STARTS_WITH = "starts_with"
    NOT_STARTS_WITH = "not_starts_with"
    CONTAINS_TEXT = "contains_text"
    NOT_CONTAINS_TEXT = "not_contains_text"
    OVERLAPS_ANY = "overlaps_any"
    NOT_OVERLAPS_ANY = "not_overlaps_any"
    RANGE_INCLUSIVE = "range_inclusive"
    GREATER_OR_EQUAL = "greater_or_equal"
    LESS_OR_EQUAL = "less_or_equal"
    WITHIN_LAST = "within_last"
    OLDER_THAN = "older_than"
    INTERSECTS_GEOMETRY = "intersects_geometry"


class RelativeTimeUnit(str, Enum):
    MINUTES = "minutes"
    HOURS = "hours"
    DAYS = "days"


class MessageFieldDefinition(BaseModel):
    id: str
    display_name: str
    description: str | None = None
    source_path: str
    output_path: str | None = None
    value_kind: ValueKind
    required: bool = False


class EqualsConditionConfig(BaseModel):
    value: str | float | int | bool


class NotEqualsConditionConfig(BaseModel):
    value: str | float | int | bool


class InSetConditionConfig(BaseModel):
    values: list[str]


class NotInSetConditionConfig(BaseModel):
    values: list[str]


class StartsWithConditionConfig(BaseModel):
    value: str


class NotStartsWithConditionConfig(BaseModel):
    value: str


class ContainsTextConditionConfig(BaseModel):
    value: str


class NotContainsTextConditionConfig(BaseModel):
    value: str


class OverlapsAnyConditionConfig(BaseModel):
    values: list[str]


class NotOverlapsAnyConditionConfig(BaseModel):
    values: list[str]


class RangeInclusiveConditionConfig(BaseModel):
    minimum: float | None = None
    maximum: float | None = None

    @model_validator(mode="after")
    def _validate_bounds(self) -> RangeInclusiveConditionConfig:
        if self.minimum is None and self.maximum is None:
            raise ValueError("range_inclusive requires minimum, maximum, or both")
        return self


class GreaterOrEqualConditionConfig(BaseModel):
    value: float


class LessOrEqualConditionConfig(BaseModel):
    value: float


class RelativeTimeConditionConfig(BaseModel):
    value: float = Field(gt=0)
    unit: RelativeTimeUnit

    def to_timedelta(self) -> timedelta:
        if self.unit == RelativeTimeUnit.MINUTES:
            return timedelta(minutes=self.value)
        if self.unit == RelativeTimeUnit.HOURS:
            return timedelta(hours=self.value)
        return timedelta(days=self.value)


class IntersectsGeometryConditionConfig(BaseModel):
    geometry_wkt: str | None = None
    geometry_geojson: dict[str, Any] | None = None
    geometry_wkts: list[str] = Field(default_factory=list)
    geometry_geojsons: list[dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_geometry_source(self) -> IntersectsGeometryConditionConfig:
        populated_fields = [
            value
            for value in (
                self.geometry_wkt,
                self.geometry_geojson,
                self.geometry_wkts or None,
                self.geometry_geojsons or None,
            )
            if value is not None
        ]
        if len(populated_fields) != 1:
            raise ValueError("intersects_geometry requires exactly one geometry input")
        return self


ConditionConfig = (
    EqualsConditionConfig
    | NotEqualsConditionConfig
    | InSetConditionConfig
    | NotInSetConditionConfig
    | StartsWithConditionConfig
    | NotStartsWithConditionConfig
    | ContainsTextConditionConfig
    | NotContainsTextConditionConfig
    | OverlapsAnyConditionConfig
    | NotOverlapsAnyConditionConfig
    | RangeInclusiveConditionConfig
    | GreaterOrEqualConditionConfig
    | LessOrEqualConditionConfig
    | RelativeTimeConditionConfig
    | IntersectsGeometryConditionConfig
)


class RouteCondition(BaseModel):
    field_id: str
    operator: Operator
    config: ConditionConfig

    @model_validator(mode="before")
    @classmethod
    def _coerce_config(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        operator = data["operator"] if "operator" in data else None
        config = data["config"] if "config" in data else None
        if not isinstance(operator, str | Operator):
            return data
        updated = dict(data)
        updated["config"] = _config_model_for_operator(operator).model_validate(config)
        return updated

    def equals_config(self) -> EqualsConditionConfig:
        return cast(EqualsConditionConfig, self.config)

    def in_set_config(self) -> InSetConditionConfig:
        return cast(InSetConditionConfig, self.config)

    def not_in_set_config(self) -> NotInSetConditionConfig:
        return cast(NotInSetConditionConfig, self.config)

    def not_equals_config(self) -> NotEqualsConditionConfig:
        return cast(NotEqualsConditionConfig, self.config)

    def starts_with_config(self) -> StartsWithConditionConfig:
        return cast(StartsWithConditionConfig, self.config)

    def not_starts_with_config(self) -> NotStartsWithConditionConfig:
        return cast(NotStartsWithConditionConfig, self.config)

    def contains_text_config(self) -> ContainsTextConditionConfig:
        return cast(ContainsTextConditionConfig, self.config)

    def not_contains_text_config(self) -> NotContainsTextConditionConfig:
        return cast(NotContainsTextConditionConfig, self.config)

    def overlaps_any_config(self) -> OverlapsAnyConditionConfig:
        return cast(OverlapsAnyConditionConfig, self.config)

    def not_overlaps_any_config(self) -> NotOverlapsAnyConditionConfig:
        return cast(NotOverlapsAnyConditionConfig, self.config)

    def range_inclusive_config(self) -> RangeInclusiveConditionConfig:
        return cast(RangeInclusiveConditionConfig, self.config)

    def greater_or_equal_config(self) -> GreaterOrEqualConditionConfig:
        return cast(GreaterOrEqualConditionConfig, self.config)

    def less_or_equal_config(self) -> LessOrEqualConditionConfig:
        return cast(LessOrEqualConditionConfig, self.config)

    def relative_time_config(self) -> RelativeTimeConditionConfig:
        return cast(RelativeTimeConditionConfig, self.config)

    def intersects_geometry_config(self) -> IntersectsGeometryConditionConfig:
        return cast(IntersectsGeometryConditionConfig, self.config)


class RouteDefinition(BaseModel):
    id: str
    display_name: str
    namespace_id: str
    enabled: bool = True
    conditions: list[RouteCondition] = Field(default_factory=list)


class RulesDocument(BaseModel):
    fields: list[MessageFieldDefinition] = Field(default_factory=list)
    rules: list[RouteDefinition] = Field(default_factory=list)


@dataclass(frozen=True)
class RouteMatch:
    route_id: str
    namespace_id: str
    mutations: dict[str, Any]
    mutated_paths: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RouteMiss:
    route_id: str
    reason: str


@dataclass
class RouteEvaluation:
    matches: list[RouteMatch] = field(default_factory=list)
    misses: list[RouteMiss] = field(default_factory=list)


def _config_model_for_operator(operator: str | Operator) -> type[BaseModel]:
    operator_value = operator.value if isinstance(operator, Operator) else operator
    config_models: dict[str, type[BaseModel]] = {
        Operator.EQUALS.value: EqualsConditionConfig,
        Operator.NOT_EQUALS.value: NotEqualsConditionConfig,
        Operator.IN_SET.value: InSetConditionConfig,
        Operator.NOT_IN_SET.value: NotInSetConditionConfig,
        Operator.STARTS_WITH.value: StartsWithConditionConfig,
        Operator.NOT_STARTS_WITH.value: NotStartsWithConditionConfig,
        Operator.CONTAINS_TEXT.value: ContainsTextConditionConfig,
        Operator.NOT_CONTAINS_TEXT.value: NotContainsTextConditionConfig,
        Operator.OVERLAPS_ANY.value: OverlapsAnyConditionConfig,
        Operator.NOT_OVERLAPS_ANY.value: NotOverlapsAnyConditionConfig,
        Operator.RANGE_INCLUSIVE.value: RangeInclusiveConditionConfig,
        Operator.GREATER_OR_EQUAL.value: GreaterOrEqualConditionConfig,
        Operator.LESS_OR_EQUAL.value: LessOrEqualConditionConfig,
        Operator.WITHIN_LAST.value: RelativeTimeConditionConfig,
        Operator.OLDER_THAN.value: RelativeTimeConditionConfig,
        Operator.INTERSECTS_GEOMETRY.value: IntersectsGeometryConditionConfig,
    }
    try:
        return config_models[operator_value]
    except KeyError as exc:
        raise ValueError(f"unsupported operator '{operator_value}'") from exc
