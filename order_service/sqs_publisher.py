import boto3
import json
import os
import secrets
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
QUEUE_URL = os.getenv("SQS_QUEUE_URL")

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)

print("SQS QUEUE URL:", QUEUE_URL)

 
def publish_cancellation_event(order_id, customer_email):

    event = {
        "eventType": "OrderCancellationRequested",
        "eventId": f"EVT-{secrets.token_hex(6).upper()}",
        "orderId": order_id,
        "customerEmail": customer_email
    }

    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(event)
    )

    return response