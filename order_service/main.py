import json
import logging
import uuid

from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated

import boto3

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from order_service.config import (
    AWS_REGION,
    ORDERS_TABLE,
    SNS_TOPIC_ARN,
)


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


@app.get("/")
def home():
    return {
        "message": "OMS Order Service is running"
    }


@app.post(
    "/orders",
    status_code=status.HTTP_201_CREATED,
)
def create_order(payload: OrderCreate):
    # Step 1: Validate SNS configuration.
    if not SNS_TOPIC_ARN:
        error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "SNS_NOT_CONFIGURED",
            "SNS topic ARN is not configured.",
        )

    # Step 2: Generate a dynamic order ID.
    order_id = f"ORD-{uuid.uuid4()}"

    # Step 3: Prepare the DynamoDB payload.
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

    # Step 4: Insert the order into DynamoDB.
    try:
        orders_table.put_item(
            Item=order,
            ConditionExpression="attribute_not_exists(order_id)",
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

    # Step 5: Publish the SNS notification.
    message = (
        "Your order successfully placed and order id "
        + order_id
    )

    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Order Confirmation",
            Message=message,
        )

    except (ClientError, BotoCoreError):
        logger.exception(
            "Order %s was saved, but SNS publishing failed",
            order_id,
        )

        # The order has already been saved.
        # Do not claim that the order was never created.
        error_response(
            status.HTTP_502_BAD_GATEWAY,
            "ORDER_SAVED_NOTIFICATION_FAILED",
            (
                "The order was saved successfully, but the "
                "notification could not be published. "
                "The order exists; do not submit it again "
                "without checking its status."
            ),
            order_id=order_id,
        )

    # Step 6: Return success.
    return {
        "success": True,
        "message": "Order placed successfully.",
        "order_id": order_id,
        "status": "PLACED",
        "notification_status": "PUBLISHED",
    }