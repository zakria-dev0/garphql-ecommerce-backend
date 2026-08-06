import pandas as pd

from etl import transform


def test_compute_order_item_revenue():
    df = pd.DataFrame(
        {
            "order_item_id": [1, 2],
            "order_id": [1, 1],
            "product_id": [1, 2],
            "quantity": [2, 1],
            "price": [10.0, 5.0],
            "discount": [1.0, 0.0],
        }
    )
    result = transform.compute_order_item_revenue(df)
    assert list(result["total"]) == [19.0, 5.0]


def test_compute_order_item_revenue_fills_missing_discount():
    df = pd.DataFrame(
        {
            "order_item_id": [1],
            "order_id": [1],
            "product_id": [1],
            "quantity": [3],
            "price": [4.0],
            "discount": [None],
        }
    )
    result = transform.compute_order_item_revenue(df)
    assert result["total"].iloc[0] == 12.0


def test_join_products_categories():
    products = pd.DataFrame({"product_id": [1, 2], "category_id": [10, 20]})
    categories = pd.DataFrame({"category_id": [10, 20], "name": ["Books", "Toys"]})
    result = transform.join_products_categories(products, categories)
    assert list(result["category_name"]) == ["Books", "Toys"]


def test_enrich_customers_excludes_cancelled_and_returned():
    customers = pd.DataFrame({"customer_id": [1, 2]})
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [1, 1, 2],
            "order_date": pd.to_datetime(["2024-01-01", "2024-01-05", "2024-02-01"]),
            "total_amount": [100.0, 50.0, 999.0],
            "status": ["Delivered", "Delivered", "Cancelled"],
        }
    )
    result = transform.enrich_customers(customers, orders)
    row1 = result[result["customer_id"] == 1].iloc[0]
    row2 = result[result["customer_id"] == 2].iloc[0]
    assert row1["total_lifetime_value"] == 150.0
    assert row1["order_count"] == 2
    assert row2["total_lifetime_value"] == 0
    assert row2["order_count"] == 0


def test_build_daily_sales_aggregation_excludes_cancelled():
    orders = pd.DataFrame(
        {
            "order_id": [1, 2],
            "order_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "status": ["Delivered", "Cancelled"],
        }
    )
    order_items = pd.DataFrame(
        {
            "order_item_id": [1, 2],
            "order_id": [1, 2],
            "product_id": [100, 100],
            "quantity": [2, 5],
            "price": [10.0, 10.0],
            "discount": [0.0, 0.0],
            "total": [20.0, 50.0],
        }
    )
    products = pd.DataFrame({"product_id": [100], "category_id": [1]})
    result = transform.build_daily_sales_aggregation(orders, order_items, products)
    assert len(result) == 1
    assert result.iloc[0]["units_sold"] == 2
    assert result.iloc[0]["revenue"] == 20.0


def test_build_dim_time_flags_weekend():
    orders = pd.DataFrame({"order_date": pd.to_datetime(["2024-01-06", "2024-01-07"])})  # Sat, Sun
    dim = transform.build_dim_time(orders)
    assert len(dim) == 2
    assert dim["is_weekend"].all()
