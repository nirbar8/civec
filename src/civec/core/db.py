from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import Engine, MetaData, create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import sessionmaker


class DatabaseConfig(Protocol):
    @property
    def dsn(self) -> str: ...

    def configure_engine(self, engine: Engine) -> None: ...

    def create_all(self, engine: Engine, metadata: MetaData, lock_name: str) -> None: ...


@dataclass(frozen=True)
class FixedDsnConfig:
    value: str

    @property
    def dsn(self) -> str:
        return self.value

    def configure_engine(self, engine: Engine) -> None:
        return None

    def create_all(self, engine: Engine, metadata: MetaData, lock_name: str) -> None:
        try:
            metadata.create_all(engine)
        except (IntegrityError, OperationalError) as exc:
            if not _is_already_exists_error(exc):
                raise


@dataclass(frozen=True)
class PostgresDBConfig:
    url: str

    @property
    def dsn(self) -> str:
        return self.url

    def configure_engine(self, engine: Engine) -> None:
        return None

    def create_all(self, engine: Engine, metadata: MetaData, lock_name: str) -> None:
        try:
            with engine.begin() as connection:
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(:lock_key)"),
                    {"lock_key": _schema_lock_key(lock_name)},
                )
                metadata.create_all(connection)
        except (IntegrityError, OperationalError) as exc:
            if not _is_already_exists_error(exc):
                raise


def create_session_factory(
    config: DatabaseConfig,
):
    engine = create_engine(config.dsn, future=True)
    config.configure_engine(engine)
    Session = sessionmaker(
        bind=engine,
        expire_on_commit=False,
        future=True,
    )
    return engine, Session


def _schema_lock_key(name: str) -> int:
    return int.from_bytes(
        hashlib.sha1(name.encode("utf-8")).digest()[:8],
        byteorder="big",
        signed=False,
    ) & 0x7FFF_FFFF_FFFF_FFFF


def _is_already_exists_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "already exists" in message or ("relation" in message and "exists" in message)
