from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from civec.rules.engine import evaluate_routes, validate_route_against_fields
from civec.rules.field_access import get_value, set_value
from civec.rules.models import MessageFieldDefinition, RouteDefinition, RouteMiss


class GatewayNamespaceRecord(Protocol):
    namespace_id: str
    archived: bool


class GatewayNamespaceRegistry(Protocol):
    def get_namespace(self, namespace_id: str) -> GatewayNamespaceRecord | None: ...


@dataclass(frozen=True)
class GatewayRouteConfig:
    namespace_id_path: str = "namespace_id"
    stop_after_first_match: bool = True


@dataclass(frozen=True)
class GatewayOutput:
    namespace_id: str
    namespace: GatewayNamespaceRecord
    message: dict[str, Any]
    route_id: str | None
    mutated_paths: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RoutingResult:
    outputs: list[GatewayOutput] = field(default_factory=list)
    misses: list[RouteMiss] = field(default_factory=list)


class GatewayInputError(ValueError):
    """Message content is invalid for routing and should not be retried."""


class RoutingGatewayService:
    def __init__(
        self,
        *,
        config: GatewayRouteConfig,
        fields: list[MessageFieldDefinition],
        routes: list[RouteDefinition],
        namespace_registry: GatewayNamespaceRegistry,
    ) -> None:
        self._config = config
        self._fields = {item.id: item for item in fields}
        self._routes = list(routes)
        self._namespace_registry = namespace_registry
        for route in self._routes:
            validate_route_against_fields(route, self._fields)

    def route_message(self, message: dict[str, Any]) -> RoutingResult:
        explicit_namespace_id = self._read_optional_string(
            message, self._config.namespace_id_path
        )
        if explicit_namespace_id is not None:
            output = self._build_explicit_namespace_output(
                message=message,
                namespace_id=explicit_namespace_id,
            )
            return RoutingResult(outputs=[output], misses=[])

        evaluation = evaluate_routes(
            message=message,
            routes=self._routes,
            fields=self._fields,
            stop_after_first_match=self._config.stop_after_first_match,
        )
        outputs: list[GatewayOutput] = []
        seen_namespace_ids: set[str] = set()

        for match in evaluation.matches:
            if match.namespace_id in seen_namespace_ids:
                continue
            namespace = self._require_active_namespace(match.namespace_id)
            outputs.append(
                self._build_output_message(
                    message=message,
                    namespace=namespace,
                    route_id=match.route_id,
                    mutations=match.mutations,
                    mutated_paths=match.mutated_paths,
                )
            )
            seen_namespace_ids.add(match.namespace_id)

        return RoutingResult(outputs=outputs, misses=list(evaluation.misses))

    def _build_explicit_namespace_output(
        self,
        *,
        message: dict[str, Any],
        namespace_id: str,
    ) -> GatewayOutput:
        namespace = self._require_active_namespace(namespace_id)
        return self._build_output_message(
            message=message,
            namespace=namespace,
            route_id=None,
            mutations={},
            mutated_paths=[],
        )

    def _build_output_message(
        self,
        *,
        message: dict[str, Any],
        namespace: GatewayNamespaceRecord,
        route_id: str | None,
        mutations: dict[str, Any],
        mutated_paths: list[str],
    ) -> GatewayOutput:
        outgoing = _deep_copy_dict(message)
        set_value(outgoing, self._config.namespace_id_path, namespace.namespace_id)
        for path, value in mutations.items():
            set_value(outgoing, path, value)
        return GatewayOutput(
            namespace_id=namespace.namespace_id,
            namespace=namespace,
            message=outgoing,
            route_id=route_id,
            mutated_paths=mutated_paths,
        )

    def _require_active_namespace(self, namespace_id: str) -> GatewayNamespaceRecord:
        namespace = self._namespace_registry.get_namespace(namespace_id)
        if namespace is None:
            raise GatewayInputError(f"unknown namespace: {namespace_id}")
        if namespace.archived:
            raise GatewayInputError(f"archived namespace: {namespace_id}")
        return namespace

    def _read_optional_string(self, message: dict[str, Any], path: str) -> str | None:
        raw_value = get_value(message, path)
        if raw_value is None:
            return None
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise GatewayInputError(f"{path} must be a non-empty string when provided")
        return raw_value.strip()


def _deep_copy_dict(value: dict[str, Any]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, dict):
            copied[key] = _deep_copy_dict(item)
        elif isinstance(item, list):
            copied[key] = [_deep_copy_value(entry) for entry in item]
        else:
            copied[key] = item
    return copied


def _deep_copy_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _deep_copy_dict(value)
    if isinstance(value, list):
        return [_deep_copy_value(item) for item in value]
    return value
