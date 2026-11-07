import os

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

ORDERS_TABLE = os.getenv("ORDERS_TABLE", "Orders")

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:535237074578:order_notification")

CUSTOMER_SERVICE_URL = os.getenv(
    "CUSTOMER_SERVICE_URL",
    "https://uezop61wd0.execute-api.us-east-1.amazonaws.com/dev"
)

PRODUCT_SERVICE_URL = os.getenv(
    "PRODUCT_SERVICE_URL",
    "https://bycrw1ema0.execute-api.us-east-1.amazonaws.com/dev"
)