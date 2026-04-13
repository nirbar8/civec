from __future__ import annotations

from typing import Any, Protocol, TypeVar, cast

import httpx
from pydantic import BaseModel, TypeAdapter


class HttpResponse(Protocol):
    status_code: int

    def json(self) -> Any: ...

    def raise_for_status(self) -> object: ...


class HttpSession(Protocol):
    def get(
        self,
        url: str,
        *,
        params: object = None,
        timeout: float = 0,
    ) -> HttpResponse: ...

    def post(
        self,
        url: str,
        *,
        json: object = None,
        timeout: float = 0,
    ) -> HttpResponse: ...

    def close(self) -> None: ...


class ErrorResponse(BaseModel):
    detail: str = ""


ModelT = TypeVar("ModelT", bound=BaseModel)


class _HttpxSession:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def get(
        self,
        url: str,
        *,
        params: object = None,
        timeout: float = 0,
    ) -> HttpResponse:
        return self._client.get(url, params=cast(Any, params), timeout=timeout)

    def post(
        self,
        url: str,
        *,
        json: object = None,
        timeout: float = 0,
    ) -> HttpResponse:
        return self._client.post(url, json=json, timeout=timeout)

    def close(self) -> None:
        self._client.close()


def create_http_client() -> HttpSession:
    return _HttpxSession(httpx.Client())


def response_detail(response: HttpResponse) -> str:
    return ErrorResponse.model_validate(response.json()).detail


def response_model(response: HttpResponse, model_type: type[ModelT]) -> ModelT:
    return model_type.model_validate(response.json())


def response_model_list(response: HttpResponse, model_type: type[ModelT]) -> list[ModelT]:
    return TypeAdapter(list[model_type]).validate_python(response.json())


class JsonServiceClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        session: HttpSession | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = session or create_http_client()

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def timeout(self) -> float:
        return self._timeout

    @property
    def session(self) -> HttpSession:
        return self._session

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "JsonServiceClient":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def get_model(
        self,
        path: str,
        model_type: type[ModelT],
        *,
        params: object = None,
    ) -> ModelT:
        response = self._session.get(
            f"{self._base_url}{path}",
            params=params,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response_model(response, model_type)

    def get_model_list(
        self,
        path: str,
        model_type: type[ModelT],
        *,
        params: object = None,
    ) -> list[ModelT]:
        response = self._session.get(
            f"{self._base_url}{path}",
            params=params,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response_model_list(response, model_type)

    def post_model(
        self,
        path: str,
        payload: BaseModel,
        model_type: type[ModelT],
    ) -> ModelT:
        response = self._session.post(
            f"{self._base_url}{path}",
            json=payload.model_dump(mode="json", exclude_none=True),
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response_model(response, model_type)
