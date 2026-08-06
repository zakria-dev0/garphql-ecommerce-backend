"""ETL pipeline entrypoint: extract -> validate -> transform -> load.

Runnable standalone (`python -m etl.pipeline`) and importable by the Flyte
workflow in workflows/etl_workflow.py, which wraps each stage as a separate task.
"""
import logging
import time
from pathlib import Path
from typing import Optional

from etl import extract, load, transform, validate
from etl.config import get_settings
from etl.logging_config import configure_logging

logger = configure_logging()


class ETLStageError(RuntimeError):
    def __init__(self, stage: str, cause: Exception):
        super().__init__(f"ETL stage '{stage}' failed: {cause}")
        self.stage = stage
        self.cause = cause


def _run_stage(stage_name: str, fn, *args, **kwargs):
    logger.info("Starting stage: %s", stage_name)
    start = time.monotonic()
    try:
        result = fn(*args, **kwargs)
    except Exception as exc:
        logger.exception("Stage '%s' failed", stage_name)
        raise ETLStageError(stage_name, exc) from exc
    logger.info("Finished stage: %s (%.2fs)", stage_name, time.monotonic() - start)
    return result


def extract_all(data_dir: Path, prefix: str = "") -> dict:
    return {
        "categories": extract.read_categories(data_dir, prefix),
        "products": extract.read_products(data_dir, prefix),
        "customers": extract.read_customers(data_dir, prefix),
        "orders": extract.read_orders(data_dir, prefix),
        "order_items": extract.read_order_items(data_dir, prefix),
    }


def validate_all(raw: dict) -> dict:
    categories = validate.validate_categories(raw["categories"])
    products = validate.validate_products(raw["products"], categories["category_id"])
    customers = validate.validate_customers(raw["customers"])
    orders = validate.validate_orders(raw["orders"], customers["customer_id"])
    order_items = validate.validate_order_items(raw["order_items"], orders["order_id"], products["product_id"])
    return {
        "categories": categories,
        "products": products,
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
    }


def transform_all(clean: dict) -> dict:
    order_items = transform.compute_order_item_revenue(clean["order_items"])
    products = transform.join_products_categories(clean["products"], clean["categories"])
    customers = transform.enrich_customers(clean["customers"], clean["orders"])
    daily_sales = transform.build_daily_sales_aggregation(clean["orders"], order_items, clean["products"])
    dim_time = transform.build_dim_time(clean["orders"])
    return {
        "categories": clean["categories"],
        "products": products,
        "customers": customers,
        "orders": clean["orders"],
        "order_items": order_items,
        "daily_sales": daily_sales,
        "dim_time": dim_time,
    }


def load_all(database_url: str, transformed: dict) -> dict:
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
        return counts
    finally:
        conn.close()


def run_pipeline(
    data_dir: Optional[Path] = None, database_url: Optional[str] = None, prefix: str = ""
) -> dict:
    settings = get_settings()
    data_dir = Path(data_dir) if data_dir else settings.data_dir
    database_url = database_url or settings.database_url

    logger.info("Running ETL pipeline against data_dir=%s", data_dir)

    raw = _run_stage("extract", extract_all, data_dir, prefix)
    clean = _run_stage("validate", validate_all, raw)
    transformed = _run_stage("transform", transform_all, clean)
    counts = _run_stage("load", load_all, database_url, transformed)

    logger.info("ETL pipeline complete: %s", counts)
    return counts


if __name__ == "__main__":
    run_pipeline()
