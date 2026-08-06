import logging
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

logger = logging.getLogger("etl")

_FILES = {
    "categories": "product_categories.csv",
    "products": "products.csv",
    "customers": "customers.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
}


def _path(data_dir: Path, entity: str, prefix: str = "") -> Path:
    filename = f"{prefix}{_FILES[entity]}"
    return Path(data_dir) / filename


def read_categories(data_dir: Path, prefix: str = "") -> pd.DataFrame:
    path = _path(data_dir, "categories", prefix)
    logger.info("Reading categories from %s", path)
    return pd.read_csv(path, dtype={"category_id": "Int64", "parent_id": "Int64"})


def read_products(data_dir: Path, prefix: str = "") -> pd.DataFrame:
    path = _path(data_dir, "products", prefix)
    logger.info("Reading products from %s", path)
    return pd.read_csv(path, dtype={"product_id": "Int64", "category_id": "Int64"})


def read_customers(data_dir: Path, prefix: str = "") -> pd.DataFrame:
    path = _path(data_dir, "customers", prefix)
    logger.info("Reading customers from %s", path)
    return pd.read_csv(path, dtype={"customer_id": "Int64"})


def read_orders(data_dir: Path, prefix: str = "") -> pd.DataFrame:
    path = _path(data_dir, "orders", prefix)
    logger.info("Reading orders from %s", path)
    return pd.read_csv(
        path,
        dtype={"order_id": "Int64", "customer_id": "Int64"},
        parse_dates=["order_date", "processing_date", "shipping_date", "delivery_date"],
    )


def read_order_items(
    data_dir: Path, prefix: str = "", chunksize: Optional[int] = None
) -> "pd.DataFrame | Iterator[pd.DataFrame]":
    path = _path(data_dir, "order_items", prefix)
    logger.info("Reading order items from %s%s", path, f" (chunksize={chunksize})" if chunksize else "")
    dtype = {
        "order_item_id": "Int64",
        "order_id": "Int64",
        "product_id": "Int64",
        "quantity": "Int64",
    }
    if chunksize:
        return pd.read_csv(path, dtype=dtype, chunksize=chunksize)
    return pd.read_csv(path, dtype=dtype)
