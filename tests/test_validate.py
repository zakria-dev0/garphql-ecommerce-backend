import pandas as pd

from etl import validate


def test_validate_categories_drops_missing_name():
    df = pd.DataFrame(
        {
            "category_id": [1, 2, 3],
            "name": ["Electronics", None, "Books"],
            "parent_id": [None, None, None],
        }
    )
    result = validate.validate_categories(df)
    assert set(result["category_id"]) == {1, 3}


def test_validate_categories_dedupes_by_id():
    df = pd.DataFrame(
        {
            "category_id": [1, 1, 2],
            "name": ["Electronics", "Electronics-dup", "Books"],
            "parent_id": [None, None, None],
        }
    )
    result = validate.validate_categories(df)
    assert len(result) == 2


def test_validate_products_rejects_unknown_category():
    df = pd.DataFrame(
        {
            "product_id": [1, 2],
            "price": [9.99, 19.99],
            "category_id": [1, 999],
        }
    )
    result = validate.validate_products(df, valid_category_ids=[1, 2, 3])
    assert list(result["product_id"]) == [1]


def test_validate_products_rejects_negative_price():
    df = pd.DataFrame(
        {
            "product_id": [1, 2],
            "price": [9.99, -5.0],
            "category_id": [1, 1],
        }
    )
    result = validate.validate_products(df, valid_category_ids=[1])
    assert list(result["product_id"]) == [1]


def test_validate_customers_rejects_invalid_email_and_dedupes():
    df = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "email": ["a@example.com", "not-an-email", "a@example.com"],
        }
    )
    result = validate.validate_customers(df)
    assert list(result["customer_id"]) == [1]


def test_validate_orders_rejects_unknown_customer():
    df = pd.DataFrame(
        {
            "order_id": [1, 2],
            "customer_id": [1, 999],
            "order_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "total_amount": [10.0, 20.0],
        }
    )
    result = validate.validate_orders(df, valid_customer_ids=[1])
    assert list(result["order_id"]) == [1]


def test_validate_order_items_rejects_bad_references_and_quantity():
    df = pd.DataFrame(
        {
            "order_item_id": [1, 2, 3],
            "order_id": [1, 1, 999],
            "product_id": [1, 999, 1],
            "quantity": [1, 1, 1],
            "price": [10.0, 10.0, 10.0],
        }
    )
    result = validate.validate_order_items(df, valid_order_ids=[1], valid_product_ids=[1])
    assert list(result["order_item_id"]) == [1]
