from fastapi import FastAPI

app = FastAPI(title="Customer Service")


@app.get("/")
def home():
    return {"message": "Customer Service is running"}


@app.get("/customers/{customer_id}")
def get_customer(customer_id: str):
    return {
        "customer_id": customer_id,
        "name": "Arun Kumar"
    }