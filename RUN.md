```bash
cd /Users/rafaelleite/Documents/heroku_invoices_sem_parar

python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
```

Set your `.env` values (`DATABASE_URL`, `HEROKU_API_KEY`, etc.), then run:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8010 --reload
```

Quick check:

```bash
curl -s http://localhost:8010/health
```

Optional test run:

```bash
pytest -q
```

If you need a quick local Postgres via Docker:

```bash
docker run --name invoice-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=invoice_search -p 5432:5432 -d postgres:16
```

And use this in `.env`:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/invoice_search
```