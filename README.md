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
- `API_BEARER_KEY` (required, protects all `/jobs*` endpoints)
- `HEROKU_API_KEY` (required)
- `INVOICE_API_BASE_URL` (optional, defaults to Heroku endpoint)
- `LOG_LEVEL` (optional, defaults to `INFO`)
- `INVOICE_API_TIMEOUT_SECONDS` (optional, defaults to `20`)
- `PDF_DOWNLOAD_TIMEOUT_SECONDS` (optional, defaults to `20`)
- `STALE_RUNNING_JOB_MINUTES` (optional, defaults to `30`)
- `MAX_BATCHES` (optional, defaults to `32`)
- `INVOICE_DEBUG_LOGS` (optional, defaults to `false`)
- `INVOICE_DEBUG_BODY_LIMIT` (optional, defaults to `300`)

Accepted `DATABASE_URL` formats:
- `postgresql://...` (canonical)
- `postgresql+psycopg2://...` (auto-normalized)
- `postgres://...` (auto-normalized)

Generate a strong API Bearer key directly in your terminal and write it to `.env`:

```bash
python - <<'PY'
import pathlib, re, secrets
env_path = pathlib.Path(".env")
text = env_path.read_text() if env_path.exists() else ""
key = secrets.token_urlsafe(48)
line = f"API_BEARER_KEY={key}"
if re.search(r"^API_BEARER_KEY=.*$", text, flags=re.M):
    text = re.sub(r"^API_BEARER_KEY=.*$", line, text, flags=re.M)
else:
    text += ("" if text.endswith("\n") or not text else "\n") + line + "\n"
env_path.write_text(text)
print("API_BEARER_KEY saved to .env")
PY
```

Use HTTPS in production so Bearer tokens are never sent over plain HTTP.

## Run

```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8010 --reload
```

On startup, schema and indexes are created automatically if they do not exist.
All DB objects used by this service are created and accessed only inside the `heroku` schema:
- `heroku.jobs`
- `heroku.job_invoices`

## API

### Health

```bash
curl -s http://localhost:8010/health
```

### Authentication

All `/jobs*` endpoints require:

```bash
-H "Authorization: Bearer $API_BEARER_KEY"
```

Invalid or missing token returns:
- `401`
- `{"detail":"unauthorized"}`
- `WWW-Authenticate: Bearer`

### Create job

```bash
curl -s -X POST http://localhost:8010/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_BEARER_KEY" \
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

Notes:
- `batches` must be between `1` and `32` (and cannot exceed `MAX_BATCHES`).
- Duplicate `invoice_id` values in the same job are rejected.

### Get job status

```bash
curl -s http://localhost:8010/jobs/9f245953-c739-447c-bf47-6274f7be3282 \
  -H "Authorization: Bearer $API_BEARER_KEY"
```

### Get job results

```bash
curl -s http://localhost:8010/jobs/9f245953-c739-447c-bf47-6274f7be3282/results \
  -H "Authorization: Bearer $API_BEARER_KEY"
```

This endpoint returns `409` while the job is still running.

### Cancel job

```bash
curl -s -X POST http://localhost:8010/jobs/9f245953-c739-447c-bf47-6274f7be3282/cancel \
  -H "Authorization: Bearer $API_BEARER_KEY"
```

Returns final status (`canceled` if the job was running).

## Processing behavior

- One background thread starts per job.
- Inside each job, `ThreadPoolExecutor(max_workers=batches)` runs one worker per batch.
- Invoice rows are claimed atomically (`UPDATE ... WHERE status='queued' RETURNING ...`), one at a time per batch worker.
- Each invoice is retried up to 3 attempts for:
  - network errors
  - invoice API HTTP codes `401/403/404/423` (explicit business rule)
  - PDF download failures
  - empty PDF extraction
- Results persist `matched_phrases`, `pdf_url`, and `last_error` for traceability.
- If any invoice ends in `error`, the job final status is `error`.
- Jobs can end as `finished`, `error`, or `canceled`.

## Notes

- Implementation is intentionally simple: explicit SQL + `psycopg2` connection pool.
- No Celery/Redis/Kafka.
- Workers are in-process.
- On startup, stale jobs that were `running` longer than `STALE_RUNNING_JOB_MINUTES` are reconciled to `error` (with invoice rows marked as worker error).
- The service does not migrate legacy `public.jobs`/`public.job_invoices` data. Existing `public` tables are left untouched.

## Troubleshooting

- Error similar to `invalid dsn: missing "=" after "postgresql+psycopg2://..."`:
  - Keep using your existing URL if desired; the app now normalizes this format automatically.
  - If you still see it, confirm the server is running the current code and run with venv Python:
    - `python -m uvicorn app.main:app --host 0.0.0.0 --port 8010 --reload`
- Systematic `erro_401` on all invoices:
  - This upstream endpoint expects `TOKEN` header auth, not Bearer auth.
  - Validate your token directly:
    - `curl --insecure -X GET "https://semparar-production.herokuapp.com/api/v1/invoices/2640312487" -H "TOKEN: $HEROKU_API_KEY"`
