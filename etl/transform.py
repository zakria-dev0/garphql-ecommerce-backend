import logging

import pandas as pd

logger = logging.getLogger("etl")

US_FIXED_HOLIDAYS = {(1, 1), (7, 4), (12, 25)}


def join_products_categories(products_df: pd.DataFrame, categories_df: pd.DataFrame) -> pd.DataFrame:
    """Rule 1: enrich products with their category name."""
    merged = products_df.merge(
        categories_df[["category_id", "name"]].rename(columns={"name": "category_name"}),
        on="category_id",
        how="left",
    )
    missing = int(merged["category_name"].isna().sum())
    if missing:
        logger.warning("%d products have no matching category after join", missing)
    return merged


def compute_order_item_revenue(order_items_df: pd.DataFrame) -> pd.DataFrame:
    """Rule 2: revenue per order line = price * quantity - discount."""
    df = order_items_df.copy()
    df["discount"] = df["discount"].astype(float).fillna(0.0)
    df["total"] = (df["price"] * df["quantity"] - df["discount"]).round(2)
    return df


def enrich_customers(customers_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """Rule 3: derive customer lifetime value / order stats from their order history."""
    completed_orders = orders_df[~orders_df["status"].isin(["Cancelled", "Returned"])]
    agg = completed_orders.groupby("customer_id").agg(
        total_lifetime_value=("total_amount", "sum"),
        order_count=("order_id", "count"),
        first_order_date=("order_date", "min"),
        last_order_date=("order_date", "max"),
    )
    enriched = customers_df.merge(agg, on="customer_id", how="left")
    enriched["total_lifetime_value"] = enriched["total_lifetime_value"].fillna(0).round(2)
    enriched["order_count"] = enriched["order_count"].fillna(0).astype(int)
    return enriched


def build_daily_sales_aggregation(
    orders_df: pd.DataFrame, order_items_df: pd.DataFrame, products_df: pd.DataFrame
) -> pd.DataFrame:
    """Rule 4: aggregate daily sales by product and category."""
    completed_orders = orders_df[~orders_df["status"].isin(["Cancelled", "Returned"])][
        ["order_id", "order_date"]
    ].copy()
    completed_orders["date"] = completed_orders["order_date"].dt.date

    items = order_items_df.merge(completed_orders, on="order_id", how="inner")
    items = items.merge(products_df[["product_id", "category_id"]], on="product_id", how="left")

    agg = items.groupby(["date", "product_id", "category_id"]).agg(
        units_sold=("quantity", "sum"),
        revenue=("total", "sum"),
        order_count=("order_id", "nunique"),
    )
    agg["avg_unit_price"] = (agg["revenue"] / agg["units_sold"]).round(2)
    agg["revenue"] = agg["revenue"].round(2)
    return agg.reset_index()


def build_dim_time(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Rule 5: generate a time dimension table spanning the observed order date range."""
    if orders_df.empty:
        return pd.DataFrame(
            columns=[
                "date", "day_of_week", "day_of_month", "day_of_year", "week_of_year",
                "month", "month_name", "quarter", "year", "is_weekend", "is_holiday",
            ]
        )

    start = orders_df["order_date"].min().normalize()
    end = orders_df["order_date"].max().normalize()
    dates = pd.date_range(start=start, end=end, freq="D")

    dim = pd.DataFrame({"date": dates.date})
    dim["day_of_week"] = dates.dayofweek
    dim["day_of_month"] = dates.day
    dim["day_of_year"] = dates.dayofyear
    dim["week_of_year"] = dates.isocalendar().week.values
    dim["month"] = dates.month
    dim["month_name"] = dates.month_name()
    dim["quarter"] = dates.quarter
    dim["year"] = dates.year
    dim["is_weekend"] = dates.dayofweek >= 5
    dim["is_holiday"] = [(d.month, d.day) in US_FIXED_HOLIDAYS for d in dates]
    return dim
