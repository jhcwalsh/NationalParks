"""Pydantic schemas for the campsite alert engine."""

from __future__ import annotations

import re
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ScanCreate(BaseModel):
    """Payload for creating a new availability scan."""

    user_id: str
    facility_id: str
    park_name: str
    arrival_date: date
    flexible_arrival: bool = False
    num_nights: int = Field(ge=1, le=30)
    site_type: str = "any"
    vehicle_length_max: Optional[int] = None
    specific_site_ids: Optional[list[str]] = None
    notify_sms: Optional[str] = None
    notify_email: Optional[str] = None

    @field_validator("arrival_date")
    @classmethod
    def arrival_not_in_past(cls, v: date) -> date:
        if v < date.today():
            raise ValueError("arrival_date cannot be in the past")
        return v

    @field_validator("site_type")
    @classmethod
    def valid_site_type(cls, v: str) -> str:
        allowed = {"tent", "rv", "group", "any"}
        if v.lower() not in allowed:
            raise ValueError(f"site_type must be one of {allowed}")
        return v.lower()

    @field_validator("notify_sms")
    @classmethod
    def valid_e164(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.match(r"^\+[1-9]\d{1,14}$", v):
            raise ValueError("notify_sms must be E.164 format (e.g. +14155551234)")
        return v

    def model_post_init(self, __context: object) -> None:
        if self.notify_sms is None and self.notify_email is None:
            raise ValueError(
                "At least one notification channel required (notify_sms or notify_email)"
            )


class ScanUpdate(BaseModel):
    """Payload for updating an existing scan."""

    arrival_date: Optional[date] = None
    flexible_arrival: Optional[bool] = None
    num_nights: Optional[int] = Field(None, ge=1, le=30)
    site_type: Optional[str] = None
    vehicle_length_max: Optional[int] = None
    specific_site_ids: Optional[list[str]] = None
    notify_sms: Optional[str] = None
    notify_email: Optional[str] = None
    active: Optional[bool] = None

    @field_validator("site_type")
    @classmethod
    def valid_site_type(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            allowed = {"tent", "rv", "group", "any"}
            if v.lower() not in allowed:
                raise ValueError(f"site_type must be one of {allowed}")
            return v.lower()
        return v

    @field_validator("notify_sms")
    @classmethod
    def valid_e164(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.match(r"^\+[1-9]\d{1,14}$", v):
            raise ValueError("notify_sms must be E.164 format (e.g. +14155551234)")
        return v


class ScanResponse(BaseModel):
    """Response schema for a scan."""

    id: int
    user_id: str
    facility_id: str
    park_name: str
    arrival_date: date
    flexible_arrival: bool
    num_nights: int
    site_type: str
    vehicle_length_max: Optional[int] = None
    specific_site_ids: Optional[list[str]] = None
    notify_sms: Optional[str] = None
    notify_email: Optional[str] = None
    active: bool
    alert_count: int
    created_at: str


class AvailabilityEvent(BaseModel):
    """A newly detected available campsite slot."""

    facility_id: str
    site_id: str
    available_date: date
    site_type: Optional[str] = None
    vehicle_length: Optional[int] = None
    loop_name: Optional[str] = None
