from datetime import datetime

import re

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MemberCreate(BaseModel):
    email: str = Field(max_length=320)
    name: str = Field(min_length=1, max_length=200)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        value = value.strip().lower()
        if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
            raise ValueError("Invalid email address")
        return value


class MemberRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    email: str
    name: str


class WorkshopCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    description: str | None = None


class WorkshopRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    title: str
    description: str | None
    status: str


class RegistrationCreate(BaseModel):
    workshop_id: int
    member_id: int


class RegistrationRead(BaseModel):
    id: int
    workshop_id: int
    member_id: int
    status: str
    notification_job_id: int


class JobRead(BaseModel):
    id: int
    notification_type: str
    status: str
    attempts: int
    max_attempts: int
    next_retry_at: datetime
    last_error: str | None
