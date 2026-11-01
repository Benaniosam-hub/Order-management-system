import os

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

ORDERS_TABLE = os.getenv("ORDERS_TABLE", "Orders")

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:535237074578:order_notification")