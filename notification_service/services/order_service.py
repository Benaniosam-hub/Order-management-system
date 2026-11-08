import httpx
import os

from dotenv import load_dotenv

load_dotenv()

ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL")


def delete_order(order_id: str):

    with httpx.Client(timeout=10.0) as client:

        # Validate order ID
        response = client.get(
            f"{ORDER_SERVICE_URL}/orders/{order_id}"
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        # Order exists, delete it
        delete_response = client.delete(
            f"{ORDER_SERVICE_URL}/orders/{order_id}"
        )

        if delete_response.status_code == 404:
            return None

        delete_response.raise_for_status()

        return delete_response.json()