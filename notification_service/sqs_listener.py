
import boto3
import json
import asyncio
import os
import logging

from dotenv import load_dotenv
from botocore.exceptions import ClientError, BotoCoreError


# --------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# --------------------------------------------------

load_dotenv()

from config import AWS_REGION, SNS_TOPIC_ARN

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

DYNAMODB_TABLE = os.getenv("ORDERS_TABLE", "Orders")

POLL_INTERVAL = 60


# --------------------------------------------------
# LOGGING CONFIGURATION
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# AWS CLIENTS
# --------------------------------------------------

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)

dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION
)

sns = boto3.client(
    "sns",
    region_name=AWS_REGION
)


# --------------------------------------------------
# BACKGROUND TASK REFERENCES
# --------------------------------------------------

background_tasks = set()


# --------------------------------------------------
# SNS ORDER CANCELLATION NOTIFICATION
# --------------------------------------------------

async def send_order_cancellation_notification(order_id: str):

    message = (
        "Your order cancelled successfully. "
        "Order ID: " + order_id
    )

    logger.info(
        "SNS cancellation message: %s",
        message
    )

    if not SNS_TOPIC_ARN:

        logger.error(
            "SNS topic ARN is not configured for order %s",
            order_id
        )

        raise ValueError("SNS_TOPIC_ARN is not configured")

    try:

        response = await asyncio.to_thread(
            sns.publish,
            TopicArn=SNS_TOPIC_ARN,
            Subject="Order Cancellation Confirmation",
            Message=message
        )

        logger.info(
            "SNS cancellation notification published successfully for order %s",
            order_id
        )

        logger.info(
            "SNS Message ID: %s",
            response["MessageId"]
        )

    except (ClientError, BotoCoreError):

        logger.exception(
            "SNS cancellation notification failed for order %s",
            order_id
        )

        raise


# --------------------------------------------------
# ASYNC DELETE ORDER FROM DYNAMODB
# --------------------------------------------------

async def delete_order(order_id: str):

    try:

        logger.info(
            "[ASYNC] Deleting order from DynamoDB: %s",
            order_id
        )

        response = await asyncio.to_thread(
            dynamodb.delete_item,
            TableName=DYNAMODB_TABLE,
            Key={
                "order_id": {
                    "S": order_id
                }
            }
        )

        logger.info(
            "[SUCCESS] Order deleted from DynamoDB: %s",
            order_id
        )

        # Send notification only after DynamoDB deletion
        await send_order_cancellation_notification(order_id)

    except Exception:

        logger.exception(
            "[FAILED] Order cancellation processing failed for %s",
            order_id
        )


# --------------------------------------------------
# EXTRACT ORDER ID
# --------------------------------------------------

def extract_order_id(message_body: str):

    payload = json.loads(message_body)

    order_id = payload.get("order_id")

    if isinstance(order_id, dict):
        order_id = order_id.get("S")

    return order_id


# --------------------------------------------------
# PROCESS SQS MESSAGE
# --------------------------------------------------

async def process_message(message):

    receipt_handle = message["ReceiptHandle"]

    try:

        body = message["Body"]

        # Extract order ID
        order_id = extract_order_id(body)

        if not order_id:

            logger.error(
                "[ERROR] order_id not found in payload"
            )

            return

        logger.info(
            "[RECEIVED] Order ID: %s",
            order_id
        )

        # Start DynamoDB deletion in background
        task = asyncio.create_task(
            delete_order(order_id)
        )

        background_tasks.add(task)

        task.add_done_callback(
            background_tasks.discard
        )

        # Allow background task to start
        await asyncio.sleep(0)

        # Delete SQS message without waiting for DynamoDB
        await asyncio.to_thread(
            sqs.delete_message,
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle
        )

        logger.info(
            "[SQS] Message deleted successfully for order %s",
            order_id
        )

    except Exception:

        logger.exception(
            "[ERROR] SQS message processing failed"
        )


# --------------------------------------------------
# POLL SQS EVERY 60 SECONDS
# --------------------------------------------------

async def sqs_listener():

    logger.info("===================================")
    logger.info("       SQS LISTENER STARTED")
    logger.info("===================================")

    logger.info("AWS Region: %s", AWS_REGION)
    logger.info("DynamoDB Table: %s", DYNAMODB_TABLE)
    logger.info("SNS Topic ARN: %s", SNS_TOPIC_ARN)
    logger.info("Polling Interval: %s seconds", POLL_INTERVAL)

    try:

        while True:

            start_time = asyncio.get_running_loop().time()

            try:

                # Receive messages from SQS
                response = await asyncio.to_thread(
                    sqs.receive_message,
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20
                )

                messages = response.get("Messages", [])

                if messages:

                    logger.info(
                        "[INFO] Received %s message(s)",
                        len(messages)
                    )

                    for message in messages:

                        await process_message(message)

                else:

                    logger.info(
                        "[SQS] No messages available"
                    )

            except Exception:

                logger.exception(
                    "[SQS POLLING ERROR]"
                )

            # Maintain 60-second polling interval
            elapsed = (
                asyncio.get_running_loop().time() - start_time
            )

            remaining_time = max(
                0,
                POLL_INTERVAL - elapsed
            )

            logger.info(
                "[INFO] Next poll in %.1f seconds",
                remaining_time
            )

            await asyncio.sleep(remaining_time)

    finally:

        # Wait for pending background tasks on shutdown
        if background_tasks:

            logger.info(
                "[INFO] Waiting for pending background tasks..."
            )

            await asyncio.gather(
                *background_tasks,
                return_exceptions=True
            )


# --------------------------------------------------
# START LISTENER
# --------------------------------------------------

if __name__ == "__main__":

    if not SQS_QUEUE_URL:

        raise ValueError(
            "SQS_QUEUE_URL is missing from .env"
        )

    try:

        asyncio.run(sqs_listener())

    except KeyboardInterrupt:

        logger.info("[INFO] SQS Listener stopped.")
