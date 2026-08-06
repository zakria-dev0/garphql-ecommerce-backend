import io
import logging
from typing import Iterable, Optional, Sequence, Union

import pandas as pd
import psycopg2

logger = logging.getLogger("etl")

ConflictCol = Union[str, Sequence[str]]


def get_connection(database_url: str):
    return psycopg2.connect(database_url)


def _conflict_target(conflict_col: ConflictCol) -> str:
    if isinstance(conflict_col, str):
        return conflict_col
    return ", ".join(conflict_col)


def _copy_upsert(
    conn,
    df: pd.DataFrame,
    table: str,
    columns: Sequence[str],
    conflict_col: ConflictCol,
    update_cols: Optional[Iterable[str]] = None,
) -> int:
    """Bulk-loads a DataFrame into `table` via COPY into a temp staging table,
    then upserts into the target table. Efficient for large batches — avoids
    row-by-row INSERTs while still being idempotent on re-run."""
    if df.empty:
        return 0

    buf = io.StringIO()
    df[list(columns)].to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    cols_sql = ", ".join(columns)
    staging_table = f"staging_{table}"

    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
        cur.copy_expert(
            f"COPY {staging_table} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL '')", buf
        )

        if update_cols:
            set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            conflict_sql = f"ON CONFLICT ({_conflict_target(conflict_col)}) DO UPDATE SET {set_sql}"
        else:
            conflict_sql = f"ON CONFLICT ({_conflict_target(conflict_col)}) DO NOTHING"

        cur.execute(
            f"INSERT INTO {table} ({cols_sql}) SELECT {cols_sql} FROM {staging_table} {conflict_sql}"
        )
        row_count = cur.rowcount

    conn.commit()
    logger.info("Loaded %d rows into %s (%d rows in batch)", row_count, table, len(df))
    return row_count


def load_categories(conn, df: pd.DataFrame) -> int:
    columns = ["category_id", "name", "description", "parent_id", "created_at"]
    return _copy_upsert(conn, df, "product_categories", columns, "category_id", ["name", "description", "parent_id"])


def load_products(conn, df: pd.DataFrame) -> int:
    columns = [
        "product_id", "name", "description", "price", "cost", "category_id",
        "sku", "inventory_count", "weight", "created_at", "is_active",
    ]
    update_cols = ["name", "description", "price", "cost", "category_id", "inventory_count", "weight", "is_active"]
    return _copy_upsert(conn, df, "products", columns, "product_id", update_cols)


def load_customers(conn, df: pd.DataFrame) -> int:
    columns = [
        "customer_id", "email", "first_name", "last_name", "street_address",
        "city", "state", "zip_code", "country", "phone", "registration_date", "last_login",
    ]
    update_cols = ["first_name", "last_name", "street_address", "city", "state", "zip_code", "phone", "last_login"]
    return _copy_upsert(conn, df, "customers", columns, "customer_id", update_cols)


def load_orders(conn, df: pd.DataFrame) -> int:
    columns = [
        "order_id", "customer_id", "order_date", "status", "payment_method",
        "shipping_address", "shipping_city", "shipping_state", "shipping_zip", "shipping_country",
        "processing_date", "shipping_date", "delivery_date", "total_amount",
    ]
    update_cols = ["status", "processing_date", "shipping_date", "delivery_date", "total_amount"]
    return _copy_upsert(conn, df, "orders", columns, ["order_id", "order_date"], update_cols)


def load_order_items(conn, df: pd.DataFrame) -> int:
    columns = ["order_item_id", "order_id", "product_id", "quantity", "price", "discount", "total"]
    update_cols = ["quantity", "price", "discount", "total"]
    return _copy_upsert(conn, df, "order_items", columns, "order_item_id", update_cols)


def load_daily_sales_aggregation(conn, df: pd.DataFrame) -> int:
    columns = ["date", "product_id", "category_id", "units_sold", "revenue", "order_count", "avg_unit_price"]
    update_cols = ["units_sold", "revenue", "order_count", "avg_unit_price"]
    return _copy_upsert(conn, df, "daily_sales_aggregation", columns, ["date", "product_id"], update_cols)


def load_dim_time(conn, df: pd.DataFrame) -> int:
    columns = [
        "date", "day_of_week", "day_of_month", "day_of_year", "week_of_year",
        "month", "month_name", "quarter", "year", "is_weekend", "is_holiday",
    ]
    return _copy_upsert(conn, df, "dim_time", columns, "date")


_SEQUENCES = [
    ("product_categories", "category_id"),
    ("products", "product_id"),
    ("customers", "customer_id"),
    ("orders", "order_id"),
    ("order_items", "order_item_id"),
]


def reset_sequences(conn) -> None:
    """Syncs SERIAL sequences after bulk-loading rows with explicit IDs, so future
    inserts (e.g. via the GraphQL mutation) don't collide with loaded data.
    Table/column names come from the fixed `_SEQUENCES` list above, not external
    input, so building the identifier portion of the query with f-strings here is safe.
    """
    with conn.cursor() as cur:
        for table, id_col in _SEQUENCES:
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence(%s, %s), "
                f"COALESCE((SELECT MAX({id_col}) FROM {table}), 1))",
                (table, id_col),
            )
    conn.commit()
