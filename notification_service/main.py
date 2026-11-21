from fastapi import FastAPI

app = FastAPI(
    title="OMS Notification Service",
    description="SQS Listener and Email Notification Service"
)


@app.get("/")
def home():
    return {
        "service": "Notification Service",
        "status": "running"
    }
