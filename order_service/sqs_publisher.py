import boto3
import json
import os
import secrets

from dotenv import load_dotenv

load_dotenv()

sqs = boto3.client(
    "sqs",
    region_name="us-east-1"
)

QUEUE_URL = os.getenv("SQS_QUEUE_URL")

 
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