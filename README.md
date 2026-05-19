# Real-Time Distributed Task Orchestrator (RT-DTO)

This is a high-performance **task orchestration engine** for processing financial transactions through sequential, fault-tolerant pipelines. Built as part of a backend engineering challenge demonstrating distributed systems design, async Python and production-ready observability.

---

## Architecture Overview

Each submitted job is decomposed into three sequential tasks:

VALIDATION (CPU) → LEDGER_UPDATE (I/O) → NOTIFICATION (External API)

Tasks are distributed across a worker pool via Redis Streams. PostgreSQL tracks all state transitions. Workers chain tasks automatically — Task B only starts after Task A succeeds.

---

## Tech Stack

| Component | Choice | Reason |
|---|---|---|
| API | FastAPI | Async-native, auto Swagger docs |
| Message Broker | Redis Streams | Consumer groups, persistence, XAUTOCLAIM for dead worker recovery |
| Database | PostgreSQL | ACID guarantees for state transitions |
| ORM | SQLAlchemy 2.0 | Async support, type-safe queries |
| DB Driver | psycopg3 | Pre-built wheels, async-native |
| Workers | asyncio + ThreadPoolExecutor | CPU tasks in threads, I/O tasks async |
| Containers | Docker Compose | Reproducible environment |

---

## Why Redis Streams over Kafka/RabbitMQ

**Kafka** adds ZooKeeper/KRaft operational overhead inappropriate for this scope.

**RabbitMQ** lacks native stream replay — if a consumer dies, unacked messages require manual intervention.

**Redis Streams** gives us:
- `XREADGROUP` — atomic consumer group delivery (one message → one worker, guaranteed)
- Pending Entries List (PEL) — tracks unacknowledged messages per consumer
- `XAUTOCLAIM` — automatically reclaims messages from dead workers after 60s
- `appendonly yes` — stream data persists across Redis restarts
- Zero extra infrastructure — same Redis instance used for everything

---

## Race Condition Strategy

`XREADGROUP` is atomic at the Redis level — only one consumer in a group ever receives a given message. This eliminates the classic "two workers pick up the same task" race condition.

For the rare case of duplicate delivery (network partition causing redelivery after a worker already committed), every task processor begins with an **idempotency guard**:

```python
if task.status in (TaskStatus.COMPLETED, TaskStatus.DEAD):
    await _ack(stream_entry_id)
    return  # Skip silently — already processed
```

---

## Delivery Guarantee

This system implements **at-least-once delivery with idempotency guards** — the industry-standard tradeoff.

True exactly-once requires distributed transactions (two-phase commit) across Redis and PostgreSQL simultaneously, which adds significant latency and complexity. The idempotency guard achieves the same safety guarantee at a fraction of the cost.

---

## Quick Start

### Prerequisites
- Docker Desktop running

### Run the full stack

```bash
docker compose up --build
```

### Scale workers for higher throughput

```bash
docker compose up --scale worker=5
```

### Run tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Service info |
| `GET` | `/health` | Health check (includes Redis ping) |
| `POST` | `/jobs` | Submit a new job |
| `GET` | `/jobs/{job_id}` | Poll job + task status |
| `GET` | `/jobs/{job_id}/tasks` | List all tasks for a job |

Interactive docs available at **http://localhost:8000/docs** when running.

### Submit a job

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {
      "account_id": "ACC-001",
      "amount": 1500.00,
      "currency": "USD",
      "transaction_type": "DEBIT"
    }
  }'
```

### Poll status

```bash
curl http://localhost:8000/jobs/{job_id}
```

---

## Task Lifecycle
SCHEDULED → RUNNING → COMPLETED
↘ FAILED (retry with exponential backoff)
↘ DEAD (after 3 failures → DLQ)

### Timeout
Tasks exceeding 30 seconds are killed via `asyncio.wait_for` and marked `FAILED`.

### Retry
Failed tasks are re-enqueued with exponential backoff: 2s → 4s → 8s.

### Dead Letter Queue
Tasks failing 3+ times are moved to the `tasks_dlq` Redis Stream and the parent job is marked `FAILED`. DLQ entries are logged for manual inspection and can be replayed via `replay_dlq_task()`.

---

## Fault Tolerance

**Worker crash recovery:** If a worker dies mid-task, its unacknowledged message stays in the Redis PEL. After 60 seconds, `XAUTOCLAIM` transfers ownership to another live worker for reprocessing.

**Database:** PostgreSQL with named Docker volume (`pg_data`) — data survives container restarts.

**Redis:** AOF persistence enabled (`appendonly yes`) with named volume (`redis_data`) — stream data survives restarts.

---

## Project Structure
rt-dto/
├── app/
│   ├── main.py              # FastAPI entrypoint + lifespan
│   ├── config.py            # Settings via pydantic-settings
│   ├── api/
│   │   └── dispatcher.py    # REST endpoints
│   ├── core/
│   │   ├── orchestrator.py  # Redis Stream operations
│   │   ├── worker.py        # Task consumer + executor
│   │   ├── task_handlers.py # Business logic per task type
│   │   └── dlq.py           # Dead Letter Queue monitor + replay
│   ├── db/
│   │   └── session.py       # Async SQLAlchemy engine + session
│   ├── models/
│   │   ├── job.py           # ORM models + state enums
│   │   └── schemas.py       # Pydantic request/response schemas
│   └── observability/
│       └── logger.py        # Structured JSON logging
├── tests/
│   ├── conftest.py          # Shared fixtures
│   ├── test_dispatcher.py   # API endpoint tests
│   ├── test_worker.py       # Worker logic tests
│   └── test_edge_cases.py   # Timeout, DLQ, retry tests
├── worker_entrypoint.py     # Standalone worker process
├── docker-compose.yml
├── Dockerfile
└── docs/HLD.md

---

## Observable Output

Every task emits structured JSON logs with timestamp, level, job_id, task type, and duration:

```json
{"timestamp": "2026-05-18T10:41:31.344731+00:00", "level": "INFO",
 "message": "[Job:faae5dcc] Task VALIDATION → COMPLETED (1.40s)"}
```
