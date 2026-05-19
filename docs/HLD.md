# High-Level Design (HLD)

## System Overview

This Real-time distributed task orchestration engine is designed to process 2,500 concurrent financial transaction jobs, each consisting of three sequential tasks: **Validation → Ledger Update → Notification**

---

## Architecture Diagram
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT                               │
│              POST /jobs  |  GET /jobs/{id}                  │
└──────────────────────┬──────────────────────────────────────┘
│
┌────────▼────────┐
│  FastAPI API    │  ← Dispatcher
│  (port 8000)    │    Creates Job + 3 Tasks
└────────┬────────┘    Enqueues Task[0] only
│
┌────────▼────────┐
│  Redis Streams  │  ← Message Broker
│  tasks_stream   │    Consumer Group: workers
│  tasks_dlq      │    XREADGROUP / XAUTOCLAIM
└────────┬────────┘
│
┌──────────────┼──────────────┐
│              │              │
┌─────▼─────┐  ┌─────▼─────┐  ┌───▼──────┐
│ Worker 1  │  │ Worker 2  │  │ Worker N │  ← Horizontally scalable
│ asyncio   │  │ asyncio   │  │ asyncio  │
└─────┬─────┘  └─────┬─────┘  └───┬──────┘
│              │             │
└──────────────▼─────────────┘
│
┌────────▼────────┐
│   PostgreSQL    │  ← State Store
│  jobs + tasks   │    ACID transactions
└─────────────────┘

---

## Component Responsibilities

### Dispatcher (FastAPI)
- Accepts job submissions via `POST /jobs`
- Decomposes each job into exactly 3 Task records atomically
- Enqueues only Task[0] (VALIDATION) — workers chain the rest
- Returns 202 Accepted immediately (non-blocking)
- Serves status queries via `GET /jobs/{job_id}`

### Message Broker (Redis Streams)
- `tasks_stream` — main task queue
- `tasks_dlq` — dead letter queue for permanently failed tasks
- Consumer group `workers` ensures each message is delivered to exactly one worker
- Pending Entries List (PEL) tracks unacknowledged messages
- `XAUTOCLAIM` reclaims messages from crashed workers after 60s

### Workers
- Pull tasks via `XREADGROUP` (blocking, 5s timeout)
- Execute task handler based on task type
- Enforce 30s timeout via `asyncio.wait_for`
- On success: commit to DB → XACK → enqueue next task
- On failure: increment retry_count → re-enqueue with backoff OR promote to DLQ
- On startup: run `XAUTOCLAIM` periodically to recover orphaned tasks

### State Store (PostgreSQL)
- Single source of truth for all job and task states
- Tracks per-task timestamps: `scheduled_at`, `started_at`, `completed_at`
- Enables per-task latency calculation: `duration_seconds`
- ACID guarantees prevent partial state updates

---

## Scalability

### Horizontal Worker Scaling
```bash
docker compose up --scale worker=10
```
Each worker is an independent consumer in the Redis consumer group. Adding workers increases throughput linearly — no code changes required. Redis handles distribution automatically.

### Queue Depth-Based Scaling
In production, a metrics exporter would expose Redis stream length to Prometheus. A Kubernetes HPA (Horizontal Pod Autoscaler) would scale worker replicas based on queue depth:

queue_depth > 1000  →  scale workers up
queue_depth < 100   →  scale workers down

### Database Connection Pooling
SQLAlchemy engine configured with `pool_size=20, max_overflow=40` — supports up to 60 concurrent DB connections per API process.

---

## Fault Tolerance

### Worker Crash Recovery
Worker claims task via XREADGROUP → task enters PEL
Worker dies → task stays in PEL unacknowledged
60 seconds pass → XAUTOCLAIM transfers task to live worker
Live worker reprocesses → idempotency guard prevents double execution

### Retry with Exponential Backoff
Attempt 1 fails → wait 2s  → re-enqueue
Attempt 2 fails → wait 4s  → re-enqueue
Attempt 3 fails → wait 8s  → move to DLQ, mark job FAILED

### Dead Letter Queue
Permanently failed tasks are written to `tasks_dlq` stream with full context (task_id, reason, job_id). The DLQ monitor logs every entry. Operators can replay tasks via `replay_dlq_task(task_id)` after fixing the underlying issue.

---

## Observability

### Per-Task Latency Tracking
Every task records `started_at` and `completed_at` timestamps. The `duration_seconds` property gives exact execution time per task, enabling SLA monitoring:

```json
{
  "task_type": "VALIDATION",
  "duration_seconds": 1.40,
  "status": "COMPLETED"
}
```

### Structured JSON Logging
All logs are emitted as single-line JSON with consistent fields:
- `timestamp` — UTC ISO 8601
- `level` — INFO / WARNING / ERROR
- `logger` — module path
- `message` — human-readable with job_id and task_type context

This format is directly ingestible by Datadog, CloudWatch, Grafana Loki and ELK Stack.

### Health Endpoint
`GET /health` verifies Redis connectivity in addition to app health — suitable for Docker healthchecks and load balancer probes.

---

## Task Dependency Enforcement

The dependency chain (Task B only starts after Task A succeeds) is enforced by the `_chain_next_task` function in `worker.py`:

```python
# After Task[N] completes:
next_task = query(sequence_order == completed.sequence_order + 1)
if next_task:
    await enqueue_task(next_task.id)  # Chain fires here
else:
    await update_job_status(COMPLETED)  # All done
```

Tasks are never enqueued upfront in bulk — only the immediate next task is enqueued upon successful completion of its predecessor.

---

## Exactly-Once vs At-Least-Once

This system implements **at-least-once delivery with idempotency guards**.

True exactly-once across Redis and PostgreSQL would require a distributed transaction (two-phase commit), adding ~50-100ms latency per task and significant operational complexity.

The idempotency guard (status check before execution) achieves equivalent safety:
- If a task is redelivered after being completed, the guard detects `status=COMPLETED` and skips it
- No duplicate ledger updates, no duplicate notifications
- Same correctness guarantee at a fraction of the cost
