
import asyncio
import logging
import uuid

from datetime import datetime, timezone
from typing import Annotated

import boto3

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from pydantic import BaseModel, Field

from order_service.config import (
    AWS_REGION,
    ORDERS_TABLE,
    SNS_TOPIC_ARN,
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


# Response helper
def error_response(
    status_code: int,
    error_code: str,
    message: str,
    order_id: str | None = None,
):
    detail = {
        "success": False,
        "error_code": error_code,
        "message": message,
    }

    if order_id:
        detail["order_id"] = order_id

    raise HTTPException(
        status_code=status_code,
        detail=detail,
    )


# Asynchronous SNS notification method
async def send_order_notification(order_id: str):

    message = (
        "Your order successfully placed and order id "
        + order_id
    )

    # Log the notification message
    logger.info("SNS notification message: %s", message)

    # Check SNS configuration
    if not SNS_TOPIC_ARN:
        logger.error(
            "SNS topic ARN is not configured for order %s",
            order_id,
        )
        return

    try:
        # Publish SNS message without blocking the event loop
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

        logger.info(
            "SNS Message ID: %s",
            response["MessageId"],
        )

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

    # Step 1: Generate a dynamic order ID
    order_id = f"ORD-{uuid.uuid4()}"

    # Step 2: Prepare the DynamoDB payload
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

    # Step 3: Insert the order into DynamoDB
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

    # Step 4: Schedule SNS notification in background
    background_tasks.add_task(
        send_order_notification,
        order_id,
    )

    # Step 5: Return success without waiting for SNS
    return {
        "success": True,
        "message": "Order placed successfully.",
        "order_id": order_id,
        "status": "PLACED",
        "notification_status": "SCHEDULED",
    }
