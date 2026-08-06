from typing import List, Optional

import asyncpg
from strawberry.dataloader import DataLoader


def make_category_loader(pool: asyncpg.Pool) -> DataLoader:
    async def load_fn(keys: List[int]) -> List[Optional[dict]]:
        rows = await pool.fetch(
            "SELECT category_id, name FROM product_categories WHERE category_id = ANY($1::int[])",
            list(keys),
        )
        by_id = {r["category_id"]: dict(r) for r in rows}
        return [by_id.get(k) for k in keys]

    return DataLoader(load_fn=load_fn)


def make_product_loader(pool: asyncpg.Pool) -> DataLoader:
    async def load_fn(keys: List[int]) -> List[Optional[dict]]:
        rows = await pool.fetch(
            "SELECT product_id, name, sku, price, category_id FROM products WHERE product_id = ANY($1::int[])",
            list(keys),
        )
        by_id = {r["product_id"]: dict(r) for r in rows}
        return [by_id.get(k) for k in keys]

    return DataLoader(load_fn=load_fn)
