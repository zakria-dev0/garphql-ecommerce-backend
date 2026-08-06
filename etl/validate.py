import logging
from typing import Iterable, Tuple

import pandas as pd

logger = logging.getLogger("etl")


def _split(df: pd.DataFrame, mask: pd.Series, entity: str, reason: str) -> pd.DataFrame:
    """Logs and drops rows where `mask` is False, returns the remaining valid rows."""
    invalid_count = int((~mask).sum())
    if invalid_count:
        logger.warning("Rejected %d %s rows: %s", invalid_count, entity, reason)
    return df[mask]


def validate_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["category_id"])
    df = _split(df, df["category_id"].notna(), "category", "missing category_id")
    df = _split(df, df["name"].notna() & (df["name"].str.strip() != ""), "category", "missing name")
    return df


def validate_products(df: pd.DataFrame, valid_category_ids: Iterable[int]) -> pd.DataFrame:
    valid_category_ids = set(valid_category_ids)
    df = df.drop_duplicates(subset=["product_id"])
    df = _split(df, df["product_id"].notna(), "product", "missing product_id")
    df = _split(df, df["price"].notna() & (df["price"] >= 0), "product", "negative/missing price")
    df = _split(
        df,
        df["category_id"].isin(valid_category_ids),
        "product",
        "category_id not found in product_categories",
    )
    return df


def validate_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["customer_id"])
    df = _split(df, df["customer_id"].notna(), "customer", "missing customer_id")
    df = _split(df, df["email"].notna() & df["email"].str.contains("@", na=False), "customer", "invalid email")
    df = df.drop_duplicates(subset=["email"], keep="first")
    return df


def validate_orders(df: pd.DataFrame, valid_customer_ids: Iterable[int]) -> pd.DataFrame:
    valid_customer_ids = set(valid_customer_ids)
    df = df.drop_duplicates(subset=["order_id"])
    df = _split(df, df["order_id"].notna(), "order", "missing order_id")
    df = _split(
        df, df["customer_id"].isin(valid_customer_ids), "order", "customer_id not found in customers"
    )
    df = _split(df, df["order_date"].notna(), "order", "missing order_date")
    df = _split(df, df["total_amount"].notna() & (df["total_amount"] >= 0), "order", "negative/missing total_amount")
    return df


def validate_order_items(
    df: pd.DataFrame, valid_order_ids: Iterable[int], valid_product_ids: Iterable[int]
) -> pd.DataFrame:
    valid_order_ids = set(valid_order_ids)
    valid_product_ids = set(valid_product_ids)
    df = df.drop_duplicates(subset=["order_item_id"])
    df = _split(df, df["order_id"].isin(valid_order_ids), "order_item", "order_id not found in orders")
    df = _split(df, df["product_id"].isin(valid_product_ids), "order_item", "product_id not found in products")
    df = _split(df, df["quantity"].notna() & (df["quantity"] > 0), "order_item", "non-positive/missing quantity")
    df = _split(df, df["price"].notna() & (df["price"] >= 0), "order_item", "negative/missing price")
    return df
