from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any

from civec.rules.field_access import get_value
from civec.rules.models import (
    MessageFieldDefinition,
    Operator,
    RouteCondition,
    RouteDefinition,
    RouteEvaluation,
    RouteMatch,
    RouteMiss,
    ValueKind,
)


class RuleEvaluationError(ValueError):
    """Raised when a route or field configuration is invalid."""


def allowed_operators_for_value_kind(value_kind: ValueKind) -> tuple[Operator, ...]:
    allowed_by_kind: dict[ValueKind, tuple[Operator, ...]] = {
        ValueKind.STRING: (
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.IN_SET,
            Operator.STARTS_WITH,
            Operator.CONTAINS_TEXT,
        ),
        ValueKind.NUMBER: (
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.RANGE_INCLUSIVE,
            Operator.GREATER_OR_EQUAL,
            Operator.LESS_OR_EQUAL,
        ),
        ValueKind.INTEGER: (
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.RANGE_INCLUSIVE,
            Operator.GREATER_OR_EQUAL,
            Operator.LESS_OR_EQUAL,
        ),
        ValueKind.DATETIME: (
            Operator.WITHIN_LAST,
            Operator.OLDER_THAN,
        ),
        ValueKind.BOOLEAN: (Operator.EQUALS, Operator.NOT_EQUALS),
        ValueKind.STRING_LIST: (Operator.OVERLAPS_ANY, Operator.NOT_OVERLAPS_ANY),
        ValueKind.GEOMETRY_WKT: (Operator.INTERSECTS_GEOMETRY,),
        ValueKind.GEOMETRY_GEOJSON: (Operator.INTERSECTS_GEOMETRY,),
    }
    if value_kind == ValueKind.STRING:
        return (
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.IN_SET,
            Operator.NOT_IN_SET,
            Operator.STARTS_WITH,
            Operator.NOT_STARTS_WITH,
            Operator.CONTAINS_TEXT,
            Operator.NOT_CONTAINS_TEXT,
        )
    return allowed_by_kind[value_kind]


def validate_route_against_fields(
    route: RouteDefinition,
    fields: dict[str, MessageFieldDefinition],
) -> None:
    for condition in route.conditions:
        if condition.field_id not in fields:
            raise RuleEvaluationError(
                f"route '{route.id}' references unknown field '{condition.field_id}'"
            )
        field = fields[condition.field_id]
        allowed = allowed_operators_for_value_kind(field.value_kind)
        if condition.operator not in allowed:
            allowed_text = ", ".join(operator.value for operator in allowed)
            raise RuleEvaluationError(
                f"route '{route.id}' uses operator '{condition.operator.value}' for field "
                f"'{field.id}' ({field.value_kind.value}), allowed: {allowed_text}"
            )
        if condition.operator == Operator.INTERSECTS_GEOMETRY:
            geometry_config = condition.intersects_geometry_config()
            if (
                field.value_kind == ValueKind.GEOMETRY_WKT
                and geometry_config.geometry_wkt is None
                and not geometry_config.geometry_wkts
            ):
                raise RuleEvaluationError(
                    f"route '{route.id}' must use geometry_wkt config for field '{field.id}'"
                )
            if (
                field.value_kind == ValueKind.GEOMETRY_GEOJSON
                and geometry_config.geometry_geojson is None
                and not geometry_config.geometry_geojsons
            ):
                raise RuleEvaluationError(
                    f"route '{route.id}' must use geometry_geojson config for field '{field.id}'"
                )


def evaluate_routes(
    *,
    message: dict[str, Any],
    routes: list[RouteDefinition],
    fields: dict[str, MessageFieldDefinition],
    stop_after_first_match: bool,
) -> RouteEvaluation:
    evaluation = RouteEvaluation()
    for route in routes:
        if not route.enabled:
            continue
        validate_route_against_fields(route, fields)
        matched, reason, mutations = _evaluate_route(
            message=message,
            route=route,
            fields=fields,
        )
        if not matched:
            evaluation.misses.append(RouteMiss(route_id=route.id, reason=reason))
            continue

        evaluation.matches.append(
            RouteMatch(
                route_id=route.id,
                namespace_id=route.namespace_id,
                mutations=copy.deepcopy(mutations),
                mutated_paths=list(mutations.keys()),
            )
        )
        if stop_after_first_match:
            break
    return evaluation


def _evaluate_route(
    *,
    message: dict[str, Any],
    route: RouteDefinition,
    fields: dict[str, MessageFieldDefinition],
) -> tuple[bool, str, dict[str, Any]]:
    mutations: dict[str, Any] = {}
    for condition in route.conditions:
        field = fields[condition.field_id]
        raw_value = get_value(message, field.source_path)
        if raw_value is None:
            if field.required:
                return False, f"{field.id}: missing required value", {}
            return False, f"{field.id}: value missing", {}
        value = _coerce_message_value(field, raw_value)
        matched, reason, mutation = _evaluate_condition(
            condition=condition,
            field=field,
            value=value,
        )
        if not matched:
            return False, f"{field.id}: {reason}", {}
        if mutation is not None:
            mutations[field.output_path or field.source_path] = mutation
    return True, "matched", mutations


def _coerce_message_value(field: MessageFieldDefinition, raw_value: Any) -> Any:
    value_kind = field.value_kind
    if value_kind == ValueKind.STRING:
        if isinstance(raw_value, str):
            return raw_value
        raise RuleEvaluationError(f"field '{field.id}' expects a string")
    if value_kind == ValueKind.NUMBER:
        if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
            return float(raw_value)
        raise RuleEvaluationError(f"field '{field.id}' expects a number")
    if value_kind == ValueKind.INTEGER:
        if isinstance(raw_value, int) and not isinstance(raw_value, bool):
            return raw_value
        raise RuleEvaluationError(f"field '{field.id}' expects an integer")
    if value_kind == ValueKind.DATETIME:
        if not isinstance(raw_value, str):
            raise RuleEvaluationError(f"field '{field.id}' expects ISO datetime text")
        return _parse_iso_datetime(field.id, raw_value)
    if value_kind == ValueKind.BOOLEAN:
        if isinstance(raw_value, bool):
            return raw_value
        raise RuleEvaluationError(f"field '{field.id}' expects a boolean")
    if value_kind == ValueKind.STRING_LIST:
        if isinstance(raw_value, list) and all(isinstance(item, str) for item in raw_value):
            return raw_value
        raise RuleEvaluationError(f"field '{field.id}' expects a list of strings")
    if value_kind == ValueKind.GEOMETRY_WKT:
        if isinstance(raw_value, str):
            return raw_value
        raise RuleEvaluationError(f"field '{field.id}' expects WKT text")
    if value_kind == ValueKind.GEOMETRY_GEOJSON:
        if isinstance(raw_value, dict):
            return raw_value
        raise RuleEvaluationError(f"field '{field.id}' expects GeoJSON object")
    raise RuleEvaluationError(
        f"field '{field.id}' has unsupported value kind '{field.value_kind}'"
    )


def _evaluate_condition(
    *,
    condition: RouteCondition,
    field: MessageFieldDefinition,
    value: Any,
) -> tuple[bool, str, Any | None]:
    if condition.operator == Operator.EQUALS:
        expected = condition.equals_config().value
        if field.value_kind == ValueKind.STRING:
            return _evaluate_case_insensitive_equals(str(value), str(expected))
        if value == expected:
            return True, "matched", None
        return False, f"expected {expected!r}, got {value!r}", None

    if condition.operator == Operator.NOT_EQUALS:
        expected = condition.not_equals_config().value
        if field.value_kind == ValueKind.STRING:
            matched, reason, _ = _evaluate_case_insensitive_equals(str(value), str(expected))
            if matched:
                return False, f"value must not equal {expected!r}", None
            return True, "matched", None
        if value != expected:
            return True, "matched", None
        return False, f"value must not equal {expected!r}", None

    if condition.operator == Operator.IN_SET:
        options = condition.in_set_config().values
        if field.value_kind == ValueKind.STRING:
            normalized_value = value.casefold()
            normalized_options = {item.casefold() for item in options}
            if normalized_value in normalized_options:
                return True, "matched", None
            return False, f"value {value!r} not in configured set", None
        if value in options:
            return True, "matched", None
        return False, f"value {value!r} not in configured set", None

    if condition.operator == Operator.NOT_IN_SET:
        options = condition.not_in_set_config().values
        normalized_value = value.casefold()
        normalized_options = {item.casefold() for item in options}
        if normalized_value in normalized_options:
            return False, f"value {value!r} must not be in configured set", None
        return True, "matched", None

    if condition.operator == Operator.STARTS_WITH:
        prefix = condition.starts_with_config().value
        if value.casefold().startswith(prefix.casefold()):
            return True, "matched", None
        return False, f"value {value!r} does not start with {prefix!r}", None

    if condition.operator == Operator.NOT_STARTS_WITH:
        prefix = condition.not_starts_with_config().value
        if value.casefold().startswith(prefix.casefold()):
            return False, f"value {value!r} must not start with {prefix!r}", None
        return True, "matched", None

    if condition.operator == Operator.CONTAINS_TEXT:
        expected = condition.contains_text_config().value
        if expected.casefold() in value.casefold():
            return True, "matched", None
        return False, f"value {value!r} does not contain {expected!r}", None

    if condition.operator == Operator.NOT_CONTAINS_TEXT:
        expected = condition.not_contains_text_config().value
        if expected.casefold() in value.casefold():
            return False, f"value {value!r} must not contain {expected!r}", None
        return True, "matched", None

    if condition.operator == Operator.OVERLAPS_ANY:
        options = {item.casefold() for item in condition.overlaps_any_config().values}
        overlap = sorted({item.casefold() for item in value} & options)
        if overlap:
            return True, f"overlap={overlap!r}", None
        return False, "no overlap with configured values", None

    if condition.operator == Operator.NOT_OVERLAPS_ANY:
        options = {item.casefold() for item in condition.not_overlaps_any_config().values}
        overlap = sorted({item.casefold() for item in value} & options)
        if overlap:
            return False, f"overlap not allowed: {overlap!r}", None
        return True, "matched", None

    if condition.operator == Operator.RANGE_INCLUSIVE:
        config = condition.range_inclusive_config()
        if config.minimum is not None and value < config.minimum:
            return False, f"value {value} is below minimum {config.minimum}", None
        if config.maximum is not None and value > config.maximum:
            return False, f"value {value} is above maximum {config.maximum}", None
        return True, "matched", None

    if condition.operator == Operator.GREATER_OR_EQUAL:
        minimum = condition.greater_or_equal_config().value
        if value >= minimum:
            return True, "matched", None
        return False, f"value {value} is below minimum {minimum}", None

    if condition.operator == Operator.LESS_OR_EQUAL:
        maximum = condition.less_or_equal_config().value
        if value <= maximum:
            return True, "matched", None
        return False, f"value {value} is above maximum {maximum}", None

    if condition.operator == Operator.WITHIN_LAST:
        duration = condition.relative_time_config().to_timedelta()
        now = datetime.now(UTC)
        earliest = now - duration
        if earliest <= value <= now:
            return True, "matched", None
        return False, f"value {value.isoformat()} is not within last {duration}", None

    if condition.operator == Operator.OLDER_THAN:
        duration = condition.relative_time_config().to_timedelta()
        cutoff = datetime.now(UTC) - duration
        if value < cutoff:
            return True, "matched", None
        return False, f"value {value.isoformat()} is newer than cutoff {cutoff.isoformat()}", None

    if condition.operator == Operator.INTERSECTS_GEOMETRY:
        try:
            message_geometry, route_geometry = _load_geometry_pair(field, value, condition)
            intersection = message_geometry.intersection(route_geometry)
        except ValueError as exc:
            raise RuleEvaluationError(f"field '{field.id}' geometry evaluation failed") from exc
        except Exception as exc:
            # Geometry backend exceptions are implementation-specific (for example GEOSException).
            raise RuleEvaluationError(f"field '{field.id}' geometry evaluation failed") from exc
        if intersection.is_empty:
            return False, "geometry does not intersect route boundary", None
        if field.value_kind == ValueKind.GEOMETRY_GEOJSON:
            return True, "matched", _geometry_to_geojson(intersection)
        return True, "matched", intersection.wkt

    raise RuleEvaluationError(f"unsupported operator '{condition.operator}'")


def _load_geometry_pair(
    field: MessageFieldDefinition,
    value: Any,
    condition: RouteCondition,
) -> tuple[Any, Any]:
    wkt_module, shape_fn, unary_union_fn = _load_geometry_backend()
    config = condition.intersects_geometry_config()
    if field.value_kind == ValueKind.GEOMETRY_WKT:
        geometry_wkts = config.geometry_wkts or (
            [] if config.geometry_wkt is None else [config.geometry_wkt]
        )
        if not geometry_wkts:
            raise RuleEvaluationError(f"field '{field.id}' requires geometry_wkt config")
        return wkt_module.loads(value), unary_union_fn(
            [wkt_module.loads(item) for item in geometry_wkts]
        )
    if field.value_kind == ValueKind.GEOMETRY_GEOJSON:
        geometry_geojsons = config.geometry_geojsons or (
            [] if config.geometry_geojson is None else [config.geometry_geojson]
        )
        if not geometry_geojsons:
            raise RuleEvaluationError(f"field '{field.id}' requires geometry_geojson config")
        return shape_fn(value), unary_union_fn([shape_fn(item) for item in geometry_geojsons])
    raise RuleEvaluationError(
        f"field '{field.id}' does not support geometry intersection"
    )


def _geometry_to_geojson(geometry: Any) -> dict[str, Any]:
    try:
        from shapely.geometry import mapping
    except ImportError as exc:  # pragma: no cover - handled by geometry checks
        raise RuleEvaluationError(
            "geometry operators require optional dependency 'shapely'; "
            "install civec[rules-geometry]"
        ) from exc
    return mapping(geometry)


def _load_geometry_backend() -> tuple[Any, Any, Any]:
    try:
        from shapely import wkt
        from shapely.geometry import shape
        from shapely.ops import unary_union
    except ImportError as exc:
        raise RuleEvaluationError(
            "geometry operators require optional dependency 'shapely'; "
            "install civec[rules-geometry]"
        ) from exc
    return wkt, shape, unary_union


def _parse_iso_datetime(field_id: str, raw_value: str) -> datetime:
    normalized = raw_value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise RuleEvaluationError(
            f"field '{field_id}' expects ISO datetime text"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _evaluate_case_insensitive_equals(
    value: str,
    expected: str,
) -> tuple[bool, str, None]:
    if value.casefold() == expected.casefold():
        return True, "matched", None
    return False, f"expected {expected!r}, got {value!r}", None
