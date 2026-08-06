from typing import Any, Dict

import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

from api.dataloaders import make_category_loader, make_product_loader
from api.db import close_pool, get_pool
from api.schema import schema


async def get_context() -> Dict[str, Any]:
    pool = await get_pool()
    return {
        "pool": pool,
        "category_loader": make_category_loader(pool),
        "product_loader": make_product_loader(pool),
    }


graphql_router = GraphQLRouter(schema, context_getter=get_context)

app = FastAPI(title="E-commerce Analytics API")
app.include_router(graphql_router, prefix="/graphql")


@app.on_event("startup")
async def on_startup() -> None:
    await get_pool()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await close_pool()


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
