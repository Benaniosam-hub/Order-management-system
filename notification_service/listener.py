import boto3
import json
import os
import time
import logging

from dotenv import load_dotenv

from services.order_service import delete_order
from services.email_service import send_cancellation_email


load_dotenv()


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# AWS SQS configuration
sqs_client = boto3.client(
    "sqs",
    region_name=os.getenv("AWS_REGION")
)

QUEUE_URL = os.getenv("SQS_QUEUE_URL")


def process_message(message):

    body = json.loads(message["Body"])

    event_type = body.get("eventType")
    event_id = body.get("eventId")
    order_id = body.get("orderId")
    customer_email = body.get("customerEmail")

    logger.info(f"Received event: {event_id}")

    # Validate event
    if event_type != "OrderCancellationRequested":
        logger.warning("Unknown event type. Skipping.")
        return

    if not order_id or not customer_email:
        raise ValueError("Order ID or customer email missing")

    # Validate and delete order
    order = delete_order(order_id)

    if order is None:
        logger.warning(f"Order {order_id} does not exist.")
        return

    logger.info(f"Order {order_id} deleted successfully.")

    # Send cancellation email
    send_cancellation_email(
        customer_email=customer_email,
        order_id=order_id
    )

    logger.info(
        f"Cancellation email sent to {customer_email}"
    )


def start_listener():

    logger.info("Notification Service SQS Listener Started")

    while True:

        try:

            response = sqs_client.receive_message(

                QueueUrl=QUEUE_URL,

                MaxNumberOfMessages=10,

                WaitTimeSeconds=20,

                VisibilityTimeout=60

            )

            messages = response.get("Messages", [])

            for message in messages:

                try:

                    process_message(message)

                    # Delete SQS message after successful processing
                    sqs_client.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=message["ReceiptHandle"]
                    )

                    logger.info("SQS message deleted successfully.")

                except Exception as error:

                    logger.exception(
                        f"Message processing failed: {error}"
                    )

                    # Do not delete failed messages.
                    # SQS will make them available again
                    # after the visibility timeout.

        except Exception:

            logger.exception("SQS polling failed")

            time.sleep(5)


if __name__ == "__main__":
    start_listener()