from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
import httpx

from customer_service.config import API_GATEWAY_URL

app = FastAPI(title="OMS Customer Service")


class CustomerCreate(BaseModel):
    name: str
    email: EmailStr
    phone: str


@app.get("/")
def home():
    return {"message": "OMS Customer Service is running"}


@app.post("/customers")
async def create_customer(customer: CustomerCreate):
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(
            f"{API_GATEWAY_URL}/customers",
            json=customer.model_dump()
        )

    if response.is_error:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text
        )

    return response.json()


@app.get("/customers")
async def get_customers():
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            f"{API_GATEWAY_URL}/customers"
        )

    if response.is_error:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text
        )

    return response.json()


@app.get("/customers/{customer_id}")
async def get_customer(customer_id: str):
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            f"{API_GATEWAY_URL}/customers/{customer_id}"
        )

    if response.is_error:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text
        )

    return response.json()