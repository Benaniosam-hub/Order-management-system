import boto3
import os

from dotenv import load_dotenv

load_dotenv()

ses_client = boto3.client(
    "ses",
    region_name=os.getenv("AWS_REGION")
)

SENDER_EMAIL = os.getenv("SENDER_EMAIL")


def send_cancellation_email(customer_email, order_id):

    response = ses_client.send_email(

        Source=SENDER_EMAIL,

        Destination={
            "ToAddresses": [customer_email]
        },

        Message={

            "Subject": {
                "Data": f"Your Order {order_id} Has Been Cancelled"
            },

            "Body": {

                "Text": {
                    "Data": f"""
Hello,

Your order {order_id} has been successfully cancelled.

We apologize for any inconvenience.

Thank you for shopping with us.
                    """
                }

            }
        }
    )

    return response