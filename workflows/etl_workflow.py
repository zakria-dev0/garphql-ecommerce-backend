"""Flyte workflow orchestrating the ETL pipeline defined in etl/.

Each stage is a separate @task so Flyte can retry, monitor, and cache them
independently. Intermediate DataFrames are checkpointed to parquet files under
a working directory rather than passed in-memory between tasks, since that's
what lets each task run (and be retried) as an isolated unit of work in a real
Flyte deployment, and it doubles as a natural point to support incremental
loads (only the orders/order_items checkpoint needs to change for a
since_date-filtered run).

Run locally with, e.g.:
    pyflyte run workflows/etl_workflow.py ecommerce_etl_workflow \
        --data_dir ecommerce_data --database_url postgresql://ecommerce:ecommerce@localhost:5434/ecommerce
"""
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from flytekit import task, workflow

from etl import extract, load, transform, validate
from etl.logging_config import configure_logging

logger = configure_logging()

_ENTITIES = ("categories", "products", "customers", "orders", "order_items")


def _write_stage(stage_dir: str, data: Dict[str, pd.DataFrame]) -> str:
    path = Path(stage_dir)
    path.mkdir(parents=True, exist_ok=True)
    for name, df in data.items():
        df.to_parquet(path / f"{name}.parquet", index=False)
    return str(path)


def _read_stage(stage_dir: str, names) -> Dict[str, pd.DataFrame]:
    path = Path(stage_dir)
    return {name: pd.read_parquet(path / f"{name}.parquet") for name in names}


@task
def extract_task(data_dir: str, prefix: str = "", since_date: Optional[str] = None) -> str:
    raw = {
        "categories": extract.read_categories(data_dir, prefix),
        "products": extract.read_products(data_dir, prefix),
        "customers": extract.read_customers(data_dir, prefix),
        "orders": extract.read_orders(data_dir, prefix),
        "order_items": extract.read_order_items(data_dir, prefix),
    }

    if since_date:
        cutoff = pd.Timestamp(since_date)
        before = len(raw["orders"])
        raw["orders"] = raw["orders"][raw["orders"]["order_date"] >= cutoff]
        raw["order_items"] = raw["order_items"][
            raw["order_items"]["order_id"].isin(raw["orders"]["order_id"])
        ]
        logger.info(
            "Incremental load since %s: kept %d/%d orders", since_date, len(raw["orders"]), before
        )

    workdir = tempfile.mkdtemp(prefix="etl_extract_")
    return _write_stage(workdir, raw)


@task
def validate_task(extract_dir: str) -> str:
    raw = _read_stage(extract_dir, _ENTITIES)

    categories = validate.validate_categories(raw["categories"])
    products = validate.validate_products(raw["products"], categories["category_id"])
    customers = validate.validate_customers(raw["customers"])
    orders = validate.validate_orders(raw["orders"], customers["customer_id"])
    order_items = validate.validate_order_items(raw["order_items"], orders["order_id"], products["product_id"])

    workdir = tempfile.mkdtemp(prefix="etl_validate_")
    result = _write_stage(
        workdir,
        {
            "categories": categories,
            "products": products,
            "customers": customers,
            "orders": orders,
            "order_items": order_items,
        },
    )
    shutil.rmtree(extract_dir, ignore_errors=True)
    return result


@task
def transform_task(validate_dir: str) -> str:
    clean = _read_stage(validate_dir, _ENTITIES)

    order_items = transform.compute_order_item_revenue(clean["order_items"])
    products = transform.join_products_categories(clean["products"], clean["categories"])
    customers = transform.enrich_customers(clean["customers"], clean["orders"])
    daily_sales = transform.build_daily_sales_aggregation(clean["orders"], order_items, clean["products"])
    dim_time = transform.build_dim_time(clean["orders"])

    workdir = tempfile.mkdtemp(prefix="etl_transform_")
    result = _write_stage(
        workdir,
        {
            "categories": clean["categories"],
            "products": products,
            "customers": customers,
            "orders": clean["orders"],
            "order_items": order_items,
            "daily_sales": daily_sales,
            "dim_time": dim_time,
        },
    )
    shutil.rmtree(validate_dir, ignore_errors=True)
    return result


@task(retries=2)
def load_task(transform_dir: str, database_url: str) -> Dict[str, int]:
    transformed = _read_stage(
        transform_dir, ("categories", "products", "customers", "orders", "order_items", "daily_sales", "dim_time")
    )

    conn = load.get_connection(database_url)
    try:
        counts = {
            "categories": load.load_categories(conn, transformed["categories"]),
            "products": load.load_products(conn, transformed["products"]),
            "customers": load.load_customers(conn, transformed["customers"]),
            "orders": load.load_orders(conn, transformed["orders"]),
            "order_items": load.load_order_items(conn, transformed["order_items"]),
            "daily_sales_aggregation": load.load_daily_sales_aggregation(conn, transformed["daily_sales"]),
            "dim_time": load.load_dim_time(conn, transformed["dim_time"]),
        }
        load.reset_sequences(conn)
    finally:
        conn.close()

    shutil.rmtree(transform_dir, ignore_errors=True)
    return counts


@task
def data_quality_check_task(database_url: str, load_counts: Dict[str, int]) -> Dict[str, int]:
    """Basic data-quality gate: fails the workflow if core tables are empty or
    if referential/aggregate invariants are violated after load."""
    conn = load.get_connection(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders")
            (order_count,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM order_items oi LEFT JOIN products p ON p.product_id = oi.product_id WHERE p.product_id IS NULL")
            (orphan_items,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM daily_sales_aggregation WHERE revenue < 0")
            (negative_revenue,) = cur.fetchone()
    finally:
        conn.close()

    if order_count == 0:
        raise ValueError("Data quality check failed: orders table is empty after load")
    if orphan_items > 0:
        raise ValueError(f"Data quality check failed: {orphan_items} order_items reference missing products")
    if negative_revenue > 0:
        raise ValueError(f"Data quality check failed: {negative_revenue} daily_sales_aggregation rows have negative revenue")

    logger.info("Data quality checks passed: %d orders, 0 orphan items, 0 negative-revenue rows", order_count)
    return {**load_counts, "quality_check_orders_seen": order_count}


@workflow
def ecommerce_etl_workflow(
    data_dir: str = "ecommerce_data",
    database_url: str = "postgresql://ecommerce:ecommerce@localhost:5434/ecommerce",
    prefix: str = "",
    since_date: Optional[str] = None,
) -> Dict[str, int]:
    extracted = extract_task(data_dir=data_dir, prefix=prefix, since_date=since_date)
    validated = validate_task(extract_dir=extracted)
    transformed = transform_task(validate_dir=validated)
    load_counts = load_task(transform_dir=transformed, database_url=database_url)
    return data_quality_check_task(database_url=database_url, load_counts=load_counts)
