# E-commerce Analytics Platform

A data pipeline for an e-commerce analytics platform: PostgreSQL schema → Python ETL →
GraphQL API → Flyte orchestration.

See [docs/technical-document.md](docs/technical-document.md) for design rationale
(schema decisions, ETL architecture, query optimization, scaling, and production
deployment considerations).

## Stack

- **Database**: PostgreSQL 16, monthly-partitioned `orders` table
- **ETL**: Python (pandas), bulk `COPY`-based loads
- **API**: FastAPI + [Strawberry](https://strawberry.rocks/) GraphQL
- **Orchestration**: Flyte (flytekit, run locally for this demo)
- **Containerization**: Docker Compose

## Project layout

```
db/migrations/       SQL schema + partition/aggregation maintenance functions
etl/                 extract / validate / transform / load pipeline
workflows/           Flyte workflow wrapping the etl/ pipeline as tasks
api/                 FastAPI app + Strawberry GraphQL schema
scripts/             dev-data generation helper
tests/               pytest unit tests for validate/transform
data-generator.py    provided synthetic data generator (env-var overridable sizes)
database-schema.sql  provided starter schema (see db/migrations/ for the corrected version)
```

## Notes on the provided files

Two bugs were found and fixed in the provided `data-generator.py` while testing this
end-to-end (not present in the ETL/API/Flyte code, which is new):

- **Non-deterministic dates broke ETL idempotency.** `order_date` (and other dates) were
  computed relative to `datetime.datetime.now()`, which isn't seeded — only the random
  *offsets* were. Regenerating the dataset at a different wall-clock time shifted every
  date, even though `Faker.seed(42)` / `np.random.seed(42)` / `random.seed(42)` are set.
  Since `order_date` is part of the `orders` table's upsert key (required, because it's
  also the partition key — see `docs/technical-document.md`), a shifted date meant a
  re-load of regenerated data no longer matched existing rows and inserted duplicates
  instead of updating them. Fixed by anchoring all date math to a fixed `NOW` constant
  (see above) instead of the wall clock — confirmed regenerating now produces a
  byte-identical `orders.csv` every time.
- **`faker` 25.2.0 rejected a formatted date string.** `generate_customers()` passed
  `registration_date` (already `strftime`-formatted to a string) as `start_date` to
  `fake.date_time_between(...)`, which raised `ParseError` — that call needs a
  `datetime` object (or a relative string like `"-30y"`), not an arbitrary formatted
  string. Fixed by keeping the unformatted `datetime` object around for the faker call
  and only formatting to a string for the CSV output.

## Setup

### 1. Start PostgreSQL

```bash
cp .env.example .env
docker compose up -d postgres
```

The schema in `db/migrations/` is applied automatically on first container init
(mounted as `/docker-entrypoint-initdb.d`). Postgres is exposed on **host port 5434**
(not the default 5432/5433, to avoid clashing with other local Postgres instances) —
see `docker-compose.yml` / `.env.example` if you need to change it.

### 2. Install Python dependencies

```bash
python -m venv .venv
.venv/Scripts/activate      # Windows
# source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```

### 3. Generate sample data

```bash
python scripts/generate_dev_data.py
```

Produces `ecommerce_data/*.csv` at a small dev-friendly scale (2k customers, 5k orders,
15k order items). To generate the full spec-sized dataset (1M customers / 5M orders /
20M order items, ~1GB) instead, just run `python data-generator.py` directly.

All dates are generated relative to a fixed anchor (`NOW` in `data-generator.py`, default
`2026-08-06T12:00:00`, overridable via `GENERATOR_NOW`) rather than wall-clock time, so
regenerating the dataset always produces identical output — see "Notes on the provided
files" below for why that matters.

### 4. Run the ETL pipeline

```bash
python -m etl.pipeline
```

Reads `ecommerce_data/*.csv`, validates/transforms, and bulk-loads into Postgres.
Logs each stage (extract/validate/transform/load) with row counts and any rejected rows.

### 5. Start the GraphQL API

```bash
uvicorn api.main:app --reload
```

Open [http://localhost:8000/graphql](http://localhost:8000/graphql) for the GraphiQL
playground.

### 6. Run the Flyte workflow

```bash
pyflyte run workflows/etl_workflow.py ecommerce_etl_workflow
```

The workflow's defaults already point at `ecommerce_data` and the Dockerized Postgres
(port 5434), so no flags are needed. Runs the same extract/validate/transform/load
pipeline as distinct Flyte tasks (locally, no cluster needed), followed by a
data-quality gate. For an incremental run, pass `--since_date 2026-07-01` to only
re-process recent orders.

> **Windows PowerShell note:** if you want to pass flags explicitly, either put them
> all on one line, or use a backtick `` ` `` for line continuation — **not** a
> backslash `\` (that's bash syntax and will error in PowerShell):
> ```powershell
> pyflyte run workflows\etl_workflow.py ecommerce_etl_workflow `
>     --data_dir ecommerce_data `
>     --database_url postgresql://ecommerce:ecommerce@localhost:5434/ecommerce
> ```

### 7. Run tests

```bash
pytest tests/
```

## Everything via Docker Compose

```bash
docker compose up -d postgres
docker compose run --rm etl          # one-shot ETL run (profile: etl)
docker compose up -d api             # GraphQL API on :8000
```

## Example GraphQL queries

```graphql
query TopProducts {
  topSellingProducts(limit: 5) {
    productId
    productName
    categoryName
    revenue
  }
}

query CustomerHistory {
  customerPurchaseHistory(customerId: 1, page: 1, pageSize: 5) {
    email
    lifetimeValue
    orderCount
    orders {
      orderId
      orderDate
      totalAmount
      items {
        productName
        quantity
        total
      }
    }
  }
}

query Trends {
  salesTrends(startDate: "2026-01-01", endDate: "2026-08-01", interval: MONTH) {
    periodStart
    revenue
    unitsSold
  }
}

mutation UpdatePrice {
  updateProduct(productId: 1, price: 24.99) {
    productId
    price
  }
}
```
