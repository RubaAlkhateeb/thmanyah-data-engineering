# Real-Time Engagement Events Processing Pipeline

A production-ready streaming pipeline that captures engagement events from PostgreSQL and processes them in real-time using CDC, Kafka, and multiple downstream consumers.

## Architecture

```
                                  ┌──────────────┐
                                  │  PostgreSQL  │
                                  │              │
                                  │  - content   │
                                  │  - events    │
                                  └──────┬───────┘
                                         │
                                         │ WAL (Write-Ahead Log)
                                         ▼
                                  ┌──────────────┐
                                  │   Debezium   │
                                  │     CDC      │
                                  └──────┬───────┘
                                         │
                                         │ Raw CDC Events
                                         ▼
                                  ┌──────────────┐
                                  │    Kafka     │
                                  │ Raw Events   │
                                  └──────┬───────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │ Transformer  │
                                  │ (Enrichment) │
                                  └──────┬───────┘
                                         │
                                         │ Transformed Events
                                         ▼
                                  ┌──────────────┐
                                  │    Kafka     │
                                  │  Enriched    │
                                  └──────┬───────┘
                                         │
                        ┌────────────────┼────────────────┐
                        │                │                │
                        ▼                ▼                ▼
                 ┌──────────┐     ┌──────────┐     ┌──────────┐
                 │  Redis   │     │ BigQuery │     │ External │
                 │          │     │          │     │   API    │
                 │   Time   │     │ Analytics│     │ Webhooks │
                 │ Windows  │     │ Storage  │     │          │
                 └──────────┘     └──────────┘     └──────────┘
```

**Flow:**
1. PostgreSQL → Debezium captures changes via WAL
2. Debezium → Kafka (raw CDC events)
3. Transformer → Enriches with content metadata + calculates engagement metrics
4. Kafka → Distributes to 3 independent consumers (Redis, BigQuery, External API)

## System Components

### 1. PostgreSQL Database
- **Tables**: `content` (metadata), `engagement_events` (user interactions)
- **CDC Config**: WAL level = logical, replication slots enabled

### 2. Debezium CDC
- Captures all database changes via PostgreSQL WAL
- Publishes to Kafka topic: `postgres.public.engagement_events`
- Handles initial snapshot + ongoing changes

### 3. Transformer Service
- Joins events with content table
- Calculates engagement metrics:
  - `engagement_seconds` = `duration_ms / 1000`
  - `engagement_pct` = `engagement_seconds / length_seconds`
- Publishes to: `engagement_events_transformed`

### 4. Redis Consumer
- Time-windowed aggregations (10-min windows)
- Tracks top content per window using Sorted Sets
- Monitors latency (target: <5s)

### 5. BigQuery Consumer
- Batch loading (500 rows or 10s idle)
- Uses Load Jobs API (cost-efficient)
- Stores in: `project.engagement.engagement_events`

### 6. External API Consumer
- Sends events to webhooks via HTTP POST
- Retry logic (3 attempts)
- Rate limit handling (stops on 429)

---

## Design Decisions

### Why CDC instead of application events?
- **Guaranteed capture**: Every DB write is captured (including SQL, migrations)
- **Zero code changes**: No application modifications needed
- **Schema evolution**: Automatic handling of table changes
- **Historical data**: Initial snapshot included

**Trade-off**: Requires WAL replication + Kafka Connect infrastructure

### Why Kafka?
- **Durability**: Messages persist to disk
- **Scalability**: Partitioning for parallel processing
- **Multiple consumers**: Independent consumption patterns
- **Replay**: Reprocess from any offset

**Alternatives**: RabbitMQ (not ideal for streaming), Kinesis (vendor lock-in)

### Why separate Transformer?
- **Single source of truth**: All consumers get consistent data
- **Reduced DB load**: Content join happens once, not per consumer
- **Reusability**: New consumers get enriched data automatically

**Alternative**: Each consumer enriches independently (causes duplication + inconsistency)

### Why batch loading for BigQuery?
- **Cost**: Load jobs are free vs $0.05/GB for streaming
- **Performance**: 10x faster than row-by-row
- **Strategy**: Flush at 500 rows OR 10s idle

---

## Prerequisites

- Docker & Docker Compose (v20.10+)
- Python 3.9+ (for local development)
- Google Cloud credentials JSON (for BigQuery)

---

## Setup

### 1. Environment Configuration

Create `.env` file:

```bash
# Database
DATABASE_USER=postgres
DATABASE_PASSWORD=v6Wo1DXbPwzBwcAO
DATABASE_HOST=postgres
DATABASE_NAME=engagement_db

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9093

# Redis
REDIS_HOST=redis

# BigQuery
BIGQUERY_PROJECT=your-project-id
BIGQUERY_DATASET=engagement
BIGQUERY_TABLE=engagement_events

# External API (optional)
EXTERNAL_API_URL=https://webhook.site/your-url
```

### 2. Google Cloud Credentials

```bash
# Download service account JSON from GCP Console
# Save as gcp-credentials.json in project root
ls -la gcp-credentials.json
```

### 3. Start the Pipeline

```bash
# Build the application (no cache for clean build)
docker-compose build --no-cache app

# Start all services and follow application logs
docker-compose up -d && docker-compose logs -f app
```

**Stop services:**
```bash
# Stop but keep data
docker-compose stop

# Stop and remove everything
docker-compose down -v
```

---

## Monitoring

### Application Logs

**All components:**
```bash
docker-compose logs -f app
```

**Specific consumers:**
```bash
# BigQuery Consumer
docker-compose logs -f app | grep bigquery_consumer

# Redis Consumer
docker-compose logs -f app | grep redis_consumer

# External API Consumer
docker-compose logs -f app | grep external_consumer

# Transformer
docker-compose logs -f app | grep Transformer
```

### Kafka UI
```bash
# Open in browser
http://localhost:8080

# View topics, messages, consumer lag, connector status
```

### PostgreSQL
```bash
# Connect to database
docker exec -it engagement_postgres psql -U postgres -d engagement_db

# Check data
SELECT COUNT(*) FROM engagement_events;
SELECT * FROM engagement_events LIMIT 5;
```

---

## Data Flow Example

**1. PostgreSQL Insert**
```sql
INSERT INTO engagement_events (content_id, user_id, event_type, duration_ms)
VALUES ('uuid-1', 'uuid-2', 'play', 45000);
```

**2. Debezium Captures**
```json
{"after": {"id": 123, "content_id": "uuid-1", "duration_ms": 45000, ...}, "op": "c"}
```

**3. Transformer Enriches**
```python
# Joins with content table, calculates metrics
{
  "event_id": 123,
  "content_type": "podcast",      # from content table
  "length_seconds": 3600,          # from content table
  "engagement_seconds": 45.0,      # duration_ms / 1000
  "engagement_pct": 0.0125,        # 45 / 3600
  ...
}
```

**4. Consumers Process**
- **Redis**: `ZINCRBY event_ts:2026-01-13T12:00 1 uuid-1`
- **BigQuery**: Batches 500 rows → Load Job
- **External API**: `POST https://webhook.site/...`

**Latency**: <1 second end-to-end (P95)

---

## Configuration

### Key Parameters (`config/config.py`)

```python
# Redis Time Windows
WINDOW_SIZE_MINUTES = 10        # Window duration
WINDOW_TTL_SECONDS = 3600       # Expiration time

# BigQuery Batching
MAX_BATCH_SIZE = 500            # Flush threshold
IDLE_FLUSH_SECONDS = 10         # Idle timeout

# External API
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
```

---