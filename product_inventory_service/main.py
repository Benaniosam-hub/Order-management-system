from decimal import Decimal
from typing import Annotated

import httpx
from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel, Field

from product_inventory_service.config import API_GATEWAY_URL

app = FastAPI(title="OMS Product & Inventory Service")


class ProductCreate(BaseModel):
    name: str = Field(min_length=1)
    category: str = Field(min_length=1)
    price: Decimal = Field(gt=0)
    stock_quantity: int = Field(ge=0)


class StockAdjustment(BaseModel):
    change: int = Field(ne=0)


async def forward_request(method: str, path: str, **kwargs):
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.request(
                method,
                f"{API_GATEWAY_URL}{path}",
                **kwargs
            )

        if response.is_error:
            try:
                detail = response.json()
            except ValueError:
                detail = response.text

            raise HTTPException(
                status_code=response.status_code,
                detail=detail
            )

        return response.json()

    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not reach Product API: {exc}"
        ) from exc


@app.get("/")
async def home():
    return {"message": "OMS Product & Inventory Service is running"}


@app.post("/products", status_code=201)
async def create_product(product: ProductCreate):
    return await forward_request(
        "POST",
        "/products",
        json=product.model_dump(mode="json")
    )


@app.get("/products")
async def get_products():
    return await forward_request("GET", "/products")


@app.get("/products/{product_id}")
async def get_product(product_id: str):
    return await forward_request(
        "GET",
        f"/products/{product_id}"
    )


@app.get("/products/{product_id}/stock")
async def get_product_stock(product_id: str):
    return await forward_request(
        "GET",
        f"/products/{product_id}/stock"
    )


@app.patch("/products/{product_id}/stock")
async def adjust_product_stock(
    product_id: str,
    adjustment: StockAdjustment
):
    return await forward_request(
        "PATCH",
        f"/products/{product_id}/stock",
        json=adjustment.model_dump()
    ) 