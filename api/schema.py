import datetime
from typing import List, Optional

import strawberry

from api.types import (
    Category,
    CustomerPurchaseHistory,
    OrderItemSummary,
    OrderSummary,
    Product,
    ProductSalesPage,
    ProductSalesPeriod,
    SalesInterval,
    SalesTrendPoint,
    TopSellingProduct,
)

_SALES_SORT_COLUMNS = {
    "revenue": "revenue",
    "units_sold": "units_sold",
    "order_count": "order_count",
    "product_name": "product_name",
}


def _clamp_page_size(page_size: int) -> int:
    return max(1, min(page_size, 100))


@strawberry.type
class Query:
    @strawberry.field
    async def product_sales_by_period(
        self,
        info: strawberry.types.Info,
        start_date: datetime.date,
        end_date: datetime.date,
        product_id: Optional[int] = None,
        category_id: Optional[int] = None,
        sort_by: str = "revenue",
        page: int = 1,
        page_size: int = 20,
    ) -> ProductSalesPage:
        pool = info.context["pool"]
        page = max(1, page)
        page_size = _clamp_page_size(page_size)
        sort_col = _SALES_SORT_COLUMNS.get(sort_by, "revenue")

        filters = ["d.date BETWEEN $1 AND $2"]
        params: List = [start_date, end_date]
        if product_id is not None:
            params.append(product_id)
            filters.append(f"d.product_id = ${len(params)}")
        if category_id is not None:
            params.append(category_id)
            filters.append(f"d.category_id = ${len(params)}")
        where_sql = " AND ".join(filters)

        base_query = f"""
            SELECT d.product_id, p.name AS product_name, pc.name AS category_name,
                   SUM(d.units_sold)::int AS units_sold,
                   SUM(d.revenue)::float AS revenue,
                   SUM(d.order_count)::int AS order_count
            FROM daily_sales_aggregation d
            JOIN products p ON p.product_id = d.product_id
            JOIN product_categories pc ON pc.category_id = d.category_id
            WHERE {where_sql}
            GROUP BY d.product_id, p.name, pc.name
        """
        count_query = f"SELECT COUNT(*) FROM ({base_query}) sub"
        total_count = await pool.fetchval(count_query, *params)

        params.extend([page_size, (page - 1) * page_size])
        page_query = f"{base_query} ORDER BY {sort_col} DESC LIMIT ${len(params) - 1} OFFSET ${len(params)}"
        rows = await pool.fetch(page_query, *params)

        items = [ProductSalesPeriod(**dict(r)) for r in rows]
        return ProductSalesPage(items=items, total_count=total_count or 0, page=page, page_size=page_size)

    @strawberry.field
    async def customer_purchase_history(
        self,
        info: strawberry.types.Info,
        customer_id: int,
        page: int = 1,
        page_size: int = 20,
    ) -> Optional[CustomerPurchaseHistory]:
        pool = info.context["pool"]
        page = max(1, page)
        page_size = _clamp_page_size(page_size)

        customer_row = await pool.fetchrow(
            """
            SELECT c.customer_id, c.email,
                   COALESCE(SUM(o.total_amount) FILTER (WHERE o.status NOT IN ('Cancelled', 'Returned')), 0)::float AS lifetime_value,
                   COUNT(o.order_id) FILTER (WHERE o.status NOT IN ('Cancelled', 'Returned'))::int AS order_count
            FROM customers c
            LEFT JOIN orders o ON o.customer_id = c.customer_id
            WHERE c.customer_id = $1
            GROUP BY c.customer_id, c.email
            """,
            customer_id,
        )
        if customer_row is None:
            return None

        total_orders = await pool.fetchval(
            "SELECT COUNT(*) FROM orders WHERE customer_id = $1", customer_id
        )
        order_rows = await pool.fetch(
            """
            SELECT order_id, order_date, status, total_amount
            FROM orders
            WHERE customer_id = $1
            ORDER BY order_date DESC
            LIMIT $2 OFFSET $3
            """,
            customer_id,
            page_size,
            (page - 1) * page_size,
        )

        order_ids = [r["order_id"] for r in order_rows]
        items_by_order: dict = {oid: [] for oid in order_ids}
        if order_ids:
            item_rows = await pool.fetch(
                """
                SELECT oi.order_id, oi.product_id, p.name AS product_name,
                       oi.quantity, oi.price::float, oi.discount::float, oi.total::float
                FROM order_items oi
                LEFT JOIN products p ON p.product_id = oi.product_id
                WHERE oi.order_id = ANY($1::int[])
                """,
                order_ids,
            )
            for r in item_rows:
                items_by_order[r["order_id"]].append(
                    OrderItemSummary(
                        product_id=r["product_id"],
                        product_name=r["product_name"],
                        quantity=r["quantity"],
                        price=r["price"],
                        discount=r["discount"],
                        total=r["total"],
                    )
                )

        orders = [
            OrderSummary(
                order_id=r["order_id"],
                order_date=r["order_date"],
                status=r["status"],
                total_amount=float(r["total_amount"]),
                items=items_by_order.get(r["order_id"], []),
            )
            for r in order_rows
        ]

        return CustomerPurchaseHistory(
            customer_id=customer_row["customer_id"],
            email=customer_row["email"],
            lifetime_value=customer_row["lifetime_value"],
            order_count=customer_row["order_count"],
            orders=orders,
            total_orders=total_orders or 0,
            page=page,
            page_size=page_size,
        )

    @strawberry.field
    async def top_selling_products(
        self,
        info: strawberry.types.Info,
        category_id: Optional[int] = None,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
        limit: int = 10,
    ) -> List[TopSellingProduct]:
        pool = info.context["pool"]
        limit = max(1, min(limit, 100))

        filters = []
        params: List = []
        if start_date is not None:
            params.append(start_date)
            filters.append(f"d.date >= ${len(params)}")
        if end_date is not None:
            params.append(end_date)
            filters.append(f"d.date <= ${len(params)}")
        if category_id is not None:
            params.append(category_id)
            filters.append(f"d.category_id = ${len(params)}")
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""

        params.append(limit)
        query = f"""
            SELECT d.product_id, p.name AS product_name, pc.name AS category_name,
                   SUM(d.units_sold)::int AS units_sold,
                   SUM(d.revenue)::float AS revenue
            FROM daily_sales_aggregation d
            JOIN products p ON p.product_id = d.product_id
            JOIN product_categories pc ON pc.category_id = d.category_id
            {where_sql}
            GROUP BY d.product_id, p.name, pc.name
            ORDER BY revenue DESC
            LIMIT ${len(params)}
        """
        rows = await pool.fetch(query, *params)
        return [TopSellingProduct(**dict(r)) for r in rows]

    @strawberry.field
    async def sales_trends(
        self,
        info: strawberry.types.Info,
        start_date: datetime.date,
        end_date: datetime.date,
        interval: SalesInterval = SalesInterval.DAY,
    ) -> List[SalesTrendPoint]:
        pool = info.context["pool"]
        rows = await pool.fetch(
            f"""
            SELECT date_trunc('{interval.value}', date)::date AS period_start,
                   SUM(units_sold)::int AS units_sold,
                   SUM(revenue)::float AS revenue,
                   SUM(order_count)::int AS order_count
            FROM daily_sales_aggregation
            WHERE date BETWEEN $1 AND $2
            GROUP BY period_start
            ORDER BY period_start
            """,
            start_date,
            end_date,
        )
        return [SalesTrendPoint(**dict(r)) for r in rows]


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def update_product(
        self,
        info: strawberry.types.Info,
        product_id: int,
        price: Optional[float] = None,
        inventory_count: Optional[int] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Product]:
        pool = info.context["pool"]
        row = await pool.fetchrow(
            """
            UPDATE products
            SET price = COALESCE($2, price),
                inventory_count = COALESCE($3, inventory_count),
                is_active = COALESCE($4, is_active)
            WHERE product_id = $1
            RETURNING product_id, name, sku, price::float, inventory_count, is_active, category_id
            """,
            product_id,
            price,
            inventory_count,
            is_active,
        )
        return Product(**dict(row)) if row else None


schema = strawberry.Schema(query=Query, mutation=Mutation)
