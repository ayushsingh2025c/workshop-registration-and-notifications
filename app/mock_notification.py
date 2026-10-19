import os
import time

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Mock Notification Service")
accepted: set[str] = set()


class Notification(BaseModel):
    job_id: int
    notification_type: str
    recipient: str
    workshop_title: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/notify")
def notify(
    body: Notification,
    idempotency_key: str = Header(),
    x_mock_failure: str | None = Header(default=None),
):
    mode = x_mock_failure or os.getenv("MOCK_FAILURE_MODE", "success")
    if mode == "timeout":
        time.sleep(10)
    if mode == "error":
        raise HTTPException(status_code=503, detail="Simulated notification failure")
    if mode != "success":
        raise HTTPException(status_code=400, detail="Unknown mock failure mode")
    duplicate = idempotency_key in accepted
    accepted.add(idempotency_key)
    return {"accepted": True, "duplicate": duplicate}
