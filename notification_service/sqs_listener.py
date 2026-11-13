
import boto3
import json
import asyncio
import os
import logging

from dotenv import load_dotenv


# -----------------------------
# LOAD ENVIRONMENT VARIABLES
# -----------------------------

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

DYNAMODB_TABLE = os.getenv("ORDERS_TABLE", "Orders")

POLL_INTERVAL = 60


# -----------------------------
# LOGGING CONFIGURATION
# -----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# -----------------------------
# AWS CLIENTS
# -----------------------------

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)

dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION
)


# Keep references to running background tasks
background_tasks = set()


# -----------------------------
# ASYNC DELETE ORDER FROM DYNAMODB
# -----------------------------

async def delete_order(order_id: str):

    try:
        print(f"[ASYNC] Deleting order: {order_id}")

        response = await asyncio.to_thread(
            dynamodb.delete_item,
            TableName=DYNAMODB_TABLE,
            Key={
                "order_id": {
                    "S": order_id
                }
            }
        )

        print(f"[SUCCESS] Order deleted from DynamoDB: {order_id}")

    except Exception as error:

        logging.error(
            f"[FAILED] DynamoDB deletion failed for {order_id}: {error}"
        )


# -----------------------------
# EXTRACT ORDER ID
# -----------------------------

def extract_order_id(message_body: str):

    payload = json.loads(message_body)

    order_id = payload.get("order_id")

    if isinstance(order_id, dict):
        order_id = order_id.get("S")

    return order_id


# -----------------------------
# PROCESS SQS MESSAGE
# -----------------------------

async def process_message(message):

    receipt_handle = message["ReceiptHandle"]

    try:

        body = message["Body"]

        # Extract order ID
        order_id = extract_order_id(body)

        if not order_id:
            logging.error("[ERROR] order_id not found in payload")
            return

        print(f"\n[RECEIVED] Order ID: {order_id}")

        # Start DynamoDB deletion asynchronously
        task = asyncio.create_task(
            delete_order(order_id)
        )

        background_tasks.add(task)

        task.add_done_callback(background_tasks.discard)

        # Allow the async task to start
        await asyncio.sleep(0)

        # Delete SQS message immediately
        await asyncio.to_thread(
            sqs.delete_message,
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle
        )

        print("[SQS] Message deleted successfully")

    except Exception as error:

        logging.error(
            f"[ERROR] SQS message processing failed: {error}"
        )


# -----------------------------
# POLL SQS EVERY 60 SECONDS
# -----------------------------

async def sqs_listener():

    print("\n===================================")
    print("       SQS LISTENER STARTED")
    print("===================================")

    print(f"AWS Region: {AWS_REGION}")
    print(f"DynamoDB Table: {DYNAMODB_TABLE}")
    print("Polling Interval: 60 seconds\n")

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

                    print(f"\n[INFO] Received {len(messages)} message(s)")

                    for message in messages:

                        await process_message(message)

                else:

                    print("[SQS] No messages available")

            except Exception as error:

                logging.error(f"[SQS POLLING ERROR] {error}")

            # Maintain 60-second polling interval
            elapsed = (
                asyncio.get_running_loop().time() - start_time
            )

            remaining_time = max(0, POLL_INTERVAL - elapsed)

            print(f"\n[INFO] Next poll in {remaining_time:.1f} seconds")

            await asyncio.sleep(remaining_time)

    finally:

        # Finish running DynamoDB deletion tasks on shutdown
        if background_tasks:

            print("\n[INFO] Waiting for pending DynamoDB deletions...")

            await asyncio.gather(
                *background_tasks,
                return_exceptions=True
            )


# -----------------------------
# START LISTENER
# -----------------------------

if __name__ == "__main__":

    if not SQS_QUEUE_URL:
        raise ValueError("SQS_QUEUE_URL is missing from .env")

    try:

        asyncio.run(sqs_listener())

    except KeyboardInterrupt:

        print("\n[INFO] SQS Listener stopped.")
