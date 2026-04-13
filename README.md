# Civec

Package-ready vector infrastructure for building vector database pipelines.

The base install contains typed records, namespace policies, and protocol seams. Optional extras add storage, query, transport, HTTP, and geometry integrations without forcing those dependencies into the minimal install.

## Install

```bash
uv add civec
uv add "civec[query,lancedb,rules-geometry]"
```

## Develop

```bash
uv sync --all-extras
uv run python -m pytest
uv run pyright
```

## Core Concepts

- `VectorRecord`: generic stored vector with `id`, `namespace_id`, `vector`, `source_timestamp`, `ingested_at`, and metadata.
- `NamespaceRecord`: generic tenant-like namespace model with shard, index, retention, and metadata policies.
- `RecordTransformer`: converts raw app messages into normalized `VectorRecord` objects.
- `QueryVectorProvider`: converts text, image, or custom query payloads into query vectors.
- `ShardRouter`: maps records and queries to storage shards by namespace.
- `GenericQueryService`: searches LanceDB-like shards and returns JSON-safe result rows.
- `civec.extensions.reranking`: exact reranking helpers for retriever flows.
- `civec.extensions.detection_queues`: generic top-K review queue scoring and rule helpers.
- `civec.extensions.gateway`: generic namespace routing and message mutation service.
- `civec.clients`: typed HTTP client helpers for service adapters.
- `IngestionEngine`: transforms, partitions, batches, and writes records through injected app adapters.
- `rules`: reusable field access, condition models, and route evaluation used by gateways and review queues.

## Ingestion

```python
from dataclasses import dataclass
from datetime import datetime, timezone

from civec.ingestion import IngestionEngine, IngestionEngineConfig
from civec.models import VectorRecord


@dataclass(frozen=True)
class Event:
    event_id: str
    namespace_id: str
    vector: list[float]


class EventTransformer:
    def parse_records(self, message: Event, now: datetime) -> list[VectorRecord]:
        return [
            VectorRecord(
                id=message.event_id,
                namespace_id=message.namespace_id,
                vector=message.vector,
                source_timestamp=None,
                ingested_at=now,
                metadata={},
            )
        ]
```

Provide a partitioner and writer for your app. The library handles transformation and batching; your adapter owns transport acknowledgements, environment variables, and concrete storage wiring.

## Query And Reranking

`GenericQueryService` expects a shard registry and LanceDB-like connection. Configure column names for your schema:

```python
from civec.query import GenericQueryService, QueryColumnConfig
from civec.extensions.reranking import attach_cosine_similarity

service = GenericQueryService(
    db_conn,
    registry,
    logger,
    columns=QueryColumnConfig(
        namespace_column="namespace_id",
        timestamp_column="ingested_at",
        vector_column="vector",
    ),
)
```

App layers can keep legacy names like `tenant_id` or `ts_ingested` by setting `QueryColumnConfig` instead of changing external APIs.

## Rules

The `civec.rules` package evaluates generic JSON documents using typed field definitions, condition configs, and route definitions. It is suitable for routing gateways and rule-filtered review queues. Install `rules-geometry` when using WKT or GeoJSON intersection operators.

## Dependency Extras

- `clients`: HTTP client helper dependencies.
- `lancedb`: LanceDB and PyArrow helpers.
- `query`: NumPy/Pandas query helpers.
- `rabbitmq`: RabbitMQ transport adapter dependencies.
- `fastapi`: FastAPI adapter dependencies.
- `tenants-sql`: SQL tenant/shard registry dependencies.
- `rules-geometry`: Shapely-backed geometry rule operators.
- `detections`: Detection queue storage dependencies.
- `gateway`: Gateway support dependencies.
- `gateway-ui`: Streamlit UI dependencies.
- `dev`: test and type-check tooling.
