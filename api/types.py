import datetime
import enum
from typing import List, Optional

import strawberry


@strawberry.enum
class SalesInterval(enum.Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


@strawberry.type
class Category:
    category_id: int
    name: str


@strawberry.type
class Product:
    product_id: int
    name: str
    sku: str
    price: float
    inventory_count: int
    is_active: bool
    category_id: int

    @strawberry.field
    async def category(self, info: strawberry.types.Info) -> Optional[Category]:
        row = await info.context["category_loader"].load(self.category_id)
        return Category(**row) if row else None


@strawberry.type
class ProductSalesPeriod:
    product_id: int
    product_name: str
    category_name: Optional[str]
    units_sold: int
    revenue: float
    order_count: int


@strawberry.type
class ProductSalesPage:
    items: List[ProductSalesPeriod]
    total_count: int
    page: int
    page_size: int


@strawberry.type
class TopSellingProduct:
    product_id: int
    product_name: str
    category_name: Optional[str]
    units_sold: int
    revenue: float


@strawberry.type
class SalesTrendPoint:
    period_start: datetime.date
    units_sold: int
    revenue: float
    order_count: int


@strawberry.type
class OrderItemSummary:
    product_id: int
    product_name: Optional[str]
    quantity: int
    price: float
    discount: float
    total: float


@strawberry.type
class OrderSummary:
    order_id: int
    order_date: datetime.datetime
    status: str
    total_amount: float
    items: List[OrderItemSummary]


@strawberry.type
class CustomerPurchaseHistory:
    customer_id: int
    email: str
    lifetime_value: float
    order_count: int
    orders: List[OrderSummary]
    total_orders: int
    page: int
    page_size: int
