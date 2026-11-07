
import asyncio
import logging
import uuid

from datetime import datetime, timezone
from typing import Annotated

import boto3
import httpx

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from pydantic import BaseModel, Field

from order_service.config import (
    AWS_REGION,
    ORDERS_TABLE,
    SNS_TOPIC_ARN,
    CUSTOMER_SERVICE_URL,
    PRODUCT_SERVICE_URL,
)


# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# AWS clients
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
)

sns = boto3.client(
    "sns",
    region_name=AWS_REGION,
)

orders_table = dynamodb.Table(ORDERS_TABLE)


# FastAPI application
app = FastAPI(
    title="OMS Order Service",
    description="Order management using FastAPI, DynamoDB, and SNS",
    version="1.0.0",
)


# Request schemas
class OrderItem(BaseModel):
    product_id: Annotated[str, Field(min_length=1)]
    quantity: Annotated[int, Field(gt=0)]


class OrderCreate(BaseModel):
    customer_id: Annotated[str, Field(min_length=1)]
    items: Annotated[list[OrderItem], Field(min_length=1)]


# Error response helper
def error_response(
    status_code: int,
    error_code: str,
    message: str,
    details: list | None = None,
):
    response = {
        "success": False,
        "error_code": error_code,
        "message": message,
    }

    if details is not None:
        response["details"] = details

    raise HTTPException(
        status_code=status_code,
        detail=response,
    )


# Customer validation
def validate_customer(
    client: httpx.Client,
    customer_id: str,
):
    url = f"{CUSTOMER_SERVICE_URL}/customers/{customer_id}"

    try:
        response = client.get(url)

        if response.status_code == 404:
            return False

        response.raise_for_status()

        return True

    except httpx.HTTPStatusError as exc:
        logger.exception("Customer Service returned an error")

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "success": False,
                "error_code": "CUSTOMER_SERVICE_ERROR",
                "message": "Unable to validate customer with Customer Service.",
            },
        ) from exc

    except httpx.RequestError as exc:
        logger.exception("Customer Service is unavailable")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "success": False,
                "error_code": "CUSTOMER_SERVICE_UNAVAILABLE",
                "message": "Customer Service is currently unavailable.",
            },
        ) from exc


# Product and stock validation
def validate_product(
    client: httpx.Client,
    product_id: str,
    requested_quantity: int,
):
    url = f"{PRODUCT_SERVICE_URL}/products/{product_id}"

    try:
        response = client.get(url)

        if response.status_code == 404:
            return {
                "available": False,
                "reason": "PRODUCT_NOT_FOUND",
                "message": f"Product {product_id} does not exist.",
            }

        response.raise_for_status()

        product = response.json()

        stock_quantity = product.get("stock_quantity")

        if stock_quantity is None:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail={
                    "success": False,
                    "error_code": "INVALID_PRODUCT_RESPONSE",
                    "message": (
                        f"Product Service did not return stock_quantity "
                        f"for product {product_id}."
                    ),
                },
            )

        if stock_quantity < requested_quantity:
            return {
                "available": False,
                "reason": "INSUFFICIENT_STOCK",
                "message": (
                    f"Product {product_id} has only "
                    f"{stock_quantity} units available. "
                    f"Requested quantity: {requested_quantity}."
                ),
            }

        return {
            "available": True,
            "product": product,
        }

    except httpx.HTTPStatusError as exc:
        logger.exception("Product Service returned an error")

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "success": False,
                "error_code": "PRODUCT_SERVICE_ERROR",
                "message": f"Unable to validate product {product_id}.",
            },
        ) from exc

    except httpx.RequestError as exc:
        logger.exception("Product Service is unavailable")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "success": False,
                "error_code": "PRODUCT_SERVICE_UNAVAILABLE",
                "message": "Product Service is currently unavailable.",
            },
        ) from exc


# Asynchronous SNS notification
async def send_order_notification(order_id: str):

    message = (
        "Your order successfully placed and order id "
        + order_id
    )

    logger.info("SNS notification message: %s", message)

    if not SNS_TOPIC_ARN:
        logger.error(
            "SNS topic ARN is not configured for order %s",
            order_id,
        )
        return

    try:
        response = await asyncio.to_thread(
            sns.publish,
            TopicArn=SNS_TOPIC_ARN,
            Subject="Order Confirmation",
            Message=message,
        )

        logger.info(
            "SNS notification published successfully for order %s",
            order_id,
        )

        logger.info("SNS Message ID: %s", response["MessageId"])

    except (ClientError, BotoCoreError):
        logger.exception(
            "SNS notification failed for order %s",
            order_id,
        )


# Home route
@app.get("/")
def home():
    return {
        "message": "OMS Order Service is running"
    }


# Create order endpoint
@app.post(
    "/orders",
    status_code=status.HTTP_201_CREATED,
)
def create_order(
    payload: OrderCreate,
    background_tasks: BackgroundTasks,
):

    # Step 1: Connect to Customer and Product Services
    validation_errors = []

    try:
        with httpx.Client(timeout=10.0) as client:

            # Step 2: Validate customer
            customer_exists = validate_customer(
                client,
                payload.customer_id,
            )

            if not customer_exists:
                validation_errors.append({
                    "type": "CUSTOMER_NOT_FOUND",
                    "customer_id": payload.customer_id,
                    "message": (
                        f"Customer {payload.customer_id} does not exist."
                    ),
                })

            # Step 3: Validate every requested product
            for item in payload.items:

                product_result = validate_product(
                    client,
                    item.product_id,
                    item.quantity,
                )

                if not product_result["available"]:
                    validation_errors.append({
                        "type": product_result["reason"],
                        "product_id": item.product_id,
                        "message": product_result["message"],
                    })

    except HTTPException:
        raise

    # Step 4: Stop order creation if any validation fails
    if validation_errors:
        error_response(
            status.HTTP_400_BAD_REQUEST,
            "ORDER_VALIDATION_FAILED",
            "Order cannot be placed. Please check customer and product availability.",
            details=validation_errors,
        )

    # Step 5: Generate dynamic order ID
    order_id = f"ORD-{uuid.uuid4()}"

    # Step 6: Prepare DynamoDB payload
    order = {
        "order_id": order_id,
        "customer_id": payload.customer_id,
        "items": [
            {
                "product_id": item.product_id,
                "quantity": item.quantity,
            }
            for item in payload.items
        ],
        "status": "PLACED",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Step 7: Save order in DynamoDB
    try:
        orders_table.put_item(
            Item=order,
            ConditionExpression="attribute_not_exists(order_id)",
        )

        logger.info(
            "Order saved successfully: %s",
            order_id,
        )

    except ClientError as exc:

        error = exc.response.get("Error", {})
        aws_code = error.get("Code", "UnknownError")

        logger.exception(
            "DynamoDB failed to save order %s",
            order_id,
        )

        if aws_code in (
            "UnrecognizedClientException",
            "InvalidSignatureException",
        ):
            error_response(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "AWS_CREDENTIALS_ERROR",
                "AWS credentials are invalid or expired.",
            )

        if aws_code in (
            "AccessDeniedException",
            "AccessDenied",
        ):
            error_response(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "DYNAMODB_ACCESS_DENIED",
                "The application does not have permission to save orders.",
            )

        if aws_code in (
            "ResourceNotFoundException",
            "ResourceNotFound",
        ):
            error_response(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "ORDERS_TABLE_NOT_FOUND",
                "The Orders DynamoDB table was not found.",
            )

        error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "DYNAMODB_ERROR",
            "Unable to save the order to DynamoDB.",
        )

    except BotoCoreError:
        logger.exception(
            "DynamoDB connection failed for order %s",
            order_id,
        )

        error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "DYNAMODB_UNAVAILABLE",
            "DynamoDB is temporarily unavailable.",
        )

    # Step 8: Schedule SNS notification
    background_tasks.add_task(
        send_order_notification,
        order_id,
    )

    # Step 9: Return success
    return {
        "success": True,
        "message": "Order placed successfully.",
        "order_id": order_id,
        "customer_id": payload.customer_id,
        "status": "PLACED",
        "notification_status": "SCHEDULED",
    }
