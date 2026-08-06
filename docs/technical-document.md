# Technical Design Document

## E-commerce Analytics Platform — ETL / GraphQL / Flyte

## 1. Database Design

The schema (`db/migrations/001_init_schema.sql`) is a corrected version of the
provided `database-schema.sql`, normalized around five entities: `product_categories`
(self-referencing hierarchy), `products`, `customers`, `orders`, `order_items`, plus
a `dim_time` dimension table and two derived tables (`daily_sales_aggregation`,
`product_sales_summary` materialized view) for analytical queries.

**Fixes applied to the provided schema:**
- The original `orders` table declared `order_id SERIAL PRIMARY KEY` while also being
  `PARTITION BY RANGE (order_date)`. Postgres requires every unique/primary key on a
  partitioned table to include the partition key, so this would fail to even create.
  Fixed by making the PK `(order_id, order_date)`.
- The original file hand-wrote two example monthly partitions with a `-- ... create more`
  comment in between — not valid, executable coverage. Partitions are now generated
  programmatically (a `DO` block loop) for 2020-01 through 2027-12, plus a `DEFAULT`
  partition as a safety net so inserts never hard-fail on an uncovered date.
- `order_items` intentionally stays a single, non-partitioned table. Partitioning it too
  would need a denormalized `order_date` column (duplicating data already on `orders`)
  for the partition key, with no clear query-performance win at this scale over the
  existing btree indexes on `order_id`/`product_id` — not worth the added write/maintenance
  complexity for a v1.

**Partitioning strategy for `orders`:** monthly range partitions on `order_date`. At
millions of rows/year, this keeps each partition small enough for fast sequential scans,
lets old partitions be dropped/archived cheaply (e.g. `DETACH PARTITION` + archive to cold
storage instead of a slow `DELETE`), and lets the planner prune irrelevant months entirely
for time-bounded queries (the common case — "sales last quarter", "orders this month").
`create_future_order_partitions()` (`002_partition_maintenance.sql`) is provided for ongoing
maintenance once the pre-generated range is exhausted; it's designed to be called from a
monthly cron job or a Flyte scheduled task.

**Indexing:** btree indexes on all FK columns and frequently filtered columns
(`order_date`, `status`, product `category_id`/`is_active`), a trigram GIN index on
`products.name` for fuzzy search, and a composite index on customer location. The
materialized view and `daily_sales_aggregation` table exist specifically so the GraphQL
API's aggregate queries (sales by period, top products, trends) never have to scan/join
raw `orders`/`order_items` at request time — they read pre-aggregated rows instead.

## 2. ETL Pipeline Architecture

`etl/` is split into four single-responsibility stages (`extract.py`, `validate.py`,
`transform.py`, `load.py`), orchestrated by `etl/pipeline.py`. This mirrors the Flyte
workflow's task boundaries 1:1 (`workflows/etl_workflow.py`), so the same pipeline code
runs identically whether invoked directly (`python -m etl.pipeline`) or via Flyte —
no duplicated business logic between "the real pipeline" and "the orchestrated version".

- **Extract**: reads the 5 CSVs with pandas, with typed dtypes to fail fast on malformed
  columns; `order_items` extraction supports chunked reads for the full-scale (20M row)
  file.
- **Validate**: per-entity checks (nulls, dedup on primary key, referential integrity
  against already-validated parents) that log and drop invalid rows rather than crashing
  the whole run — a batch with a few bad rows shouldn't block the other 99.9%.
- **Transform**: implements the five business rules from the spec — category join,
  recomputed per-line revenue (`price * quantity - discount`), customer lifetime value,
  daily product/category sales aggregation, and a generated `dim_time` table spanning the
  observed order date range.
- **Load**: bulk-loads via `COPY` into a temp staging table, then a single
  `INSERT ... SELECT ... ON CONFLICT DO UPDATE` from staging into the target table. This
  is both fast (COPY, not row-by-row INSERT) and idempotent (safe to re-run the same batch,
  which matters for retries and incremental loads). SERIAL sequences are resynced after
  each run since rows are loaded with explicit IDs from the source CSVs.
  One caveat surfaced during testing: `orders`' upsert key is `(order_id, order_date)`,
  not `order_id` alone, because Postgres requires the partition key in any unique
  constraint on a partitioned table. If a source system ever re-emits the same
  `order_id` with a *different* `order_date` (a genuine date correction, or — as
  happened here — a non-deterministic data generator), the old and new rows won't match
  on conflict and both will exist. The dev data generator's dates are now pinned to a
  fixed anchor to avoid this (see the README's "Notes on the provided files"); a
  production source feed that can legitimately re-date an order would need an explicit
  delete-by-`order_id`-then-insert step instead of a plain upsert.

Error handling: `pipeline.py` wraps each stage and re-raises as `ETLStageError` with the
failing stage name attached, so a failure's blast radius (which stage, which exception) is
immediately visible in logs — critical when the same code runs unattended as a Flyte task.

## 3. Query Optimization Strategy

The GraphQL resolvers (`api/schema.py`) query `daily_sales_aggregation` (not raw
`orders`/`order_items`) for anything aggregate — sales by period, top products, trends —
so response time stays roughly constant regardless of how many raw orders exist. Every
list query is offset/limit paginated with a server-enforced page-size cap (100), and
`sort_by` is resolved through a column whitelist rather than interpolating client input
directly into SQL. A `strawberry.dataloader.DataLoader` batches per-request category/product
lookups (e.g. `Product.category`) to avoid N+1 queries when a list of products is returned.
For customer purchase history, order items for a page of orders are fetched in one
`WHERE order_id = ANY($1)` query rather than one query per order.

## 4. Scaling Considerations

- At 5M+ orders/20M+ order_items, monthly partition pruning is what keeps `orders` queries
  fast — always filter on `order_date` where possible so the planner can skip partitions.
- `order_items` (unpartitioned) is the largest table; if it becomes a bottleneck, the next
  step is partitioning it by a denormalized `order_date` copied from its parent order, or
  moving to a columnar OLAP store (e.g. warehouse-side, via CDC) for pure analytics reads
  while Postgres stays the OLTP system of record.
- The ETL's chunked `order_items` extraction and COPY-based bulk load are the two levers
  for handling the full 20M-row file without loading it all into memory at once — chunk
  size is the main tuning knob.
- For horizontal read scaling, `daily_sales_aggregation`/`product_sales_summary` are
  natural candidates for a read replica, since the API only reads from them.

## 5. Production Deployment Considerations

- **Scheduling**: the Flyte workflow would be registered with a `LaunchPlan` on a cron
  schedule (e.g. nightly), passing `since_date` for incremental runs so only new/changed
  orders are re-processed instead of the full dataset each time.
- **Monitoring**: Flyte's own console gives per-task success/failure/duration out of the
  box; `load_task` is configured with `retries=2` to absorb transient DB connection issues.
  `data_quality_check_task` is a hard gate — it fails the workflow (rather than just
  logging a warning) if core tables are empty or referential/aggregate invariants are
  violated, so a bad load never silently succeeds.
- **Secrets/config**: `DATABASE_URL` is read from the environment (`.env` locally, injected
  as a secret in a real deployment) rather than hardcoded, consistent across the CLI
  pipeline, the API, and the Flyte tasks.
- **Idempotency**: every load is an upsert keyed on the source primary key, so re-running a
  failed or partially-completed batch (a task retry, a rerun after fixing bad data) never
  double-counts rows.
