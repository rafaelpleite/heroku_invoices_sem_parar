# Invoice Phrase Search API

Simple FastAPI service to process invoice PDFs in parallel threads, search for phrases, and persist everything in Postgres.

## Requirements

- Python 3.11+
- Postgres running and reachable by `DATABASE_URL`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` with your values:

- `DATABASE_URL` (required)
- `HEROKU_API_KEY` (required)
- `INVOICE_API_BASE_URL` (optional, defaults to Heroku endpoint)
- `LOG_LEVEL` (optional, defaults to `INFO`)

## Run

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8010
```

On startup, schema and indexes are created automatically if they do not exist.

## API

### Health

```bash
curl -s http://localhost:8010/health
```

### Create job

```bash
curl -s -X POST http://localhost:8010/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "phrases": ["foo", "bar baz"],
    "batches": 4,
    "invoices": ["123", "456", "789", "abc"]
  }'
```

Response:

```json
{"job_id":"9f245953-c739-447c-bf47-6274f7be3282"}
```

### Get job status

```bash
curl -s http://localhost:8010/jobs/9f245953-c739-447c-bf47-6274f7be3282
```

### Get job results

```bash
curl -s http://localhost:8010/jobs/9f245953-c739-447c-bf47-6274f7be3282/results
```

This endpoint returns `409` while the job is still running.

## Processing behavior

- One background thread starts per job.
- Inside each job, `ThreadPoolExecutor(max_workers=batches)` runs one worker per batch.
- Each invoice is retried up to 3 attempts for transient errors.
- If any invoice ends in `error`, the job final status is `error`.

## Notes

- Implementation is intentionally simple: explicit SQL + `psycopg2` connection pool.
- No Celery/Redis/Kafka.
- Workers are in-process; if the server process restarts, active jobs are not resumed automatically.

