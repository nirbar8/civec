from __future__ import annotations

from typing import Any, Callable, Protocol

from pydantic import BaseModel, Field

from civec.rules.engine import evaluate_routes
from civec.rules.models import MessageFieldDefinition, RouteDefinition


class RuleQueue(Protocol):
    rule: Any


class RuleCandidate(Protocol):
    metadata: dict[str, Any]


class DetectionQueueRuleEvaluator(Protocol):
    def matches(
        self,
        *,
        queue: RuleQueue,
        candidate: RuleCandidate,
    ) -> bool: ...


class MessageRuleConfig(BaseModel):
    fields: list[MessageFieldDefinition] = Field(default_factory=list)
    rule: RouteDefinition | None = None


CandidateDocumentBuilder = Callable[[RuleCandidate], dict[str, Any]]


class MessageRuleEvaluator:
    def __init__(self, document_builder: CandidateDocumentBuilder) -> None:
        self._document_builder = document_builder

    def matches(
        self,
        *,
        queue: RuleQueue,
        candidate: RuleCandidate,
    ) -> bool:
        config = MessageRuleConfig.model_validate(queue.rule.config)
        if config.rule is None:
            return True
        evaluation = evaluate_routes(
            message=self._document_builder(candidate),
            routes=[config.rule],
            fields={field.id: field for field in config.fields},
            stop_after_first_match=True,
        )
        return bool(evaluation.matches)
