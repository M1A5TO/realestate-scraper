# core/validate.py
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl, field_validator


class Offer(BaseModel):
    offer_id: str = Field(..., min_length=3)
    source: str = Field(..., pattern=r"^[a-z0-9_]+$")
    url: HttpUrl
    title: str = ""
    price_amount: float | None = Field(default=None, ge=0)
    price_currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    property_type: str | None = None
    market_type: str | None = None
    city: str | None = None
    district: str | None = None
    street: str | None = None
    lat: float | None = Field(default=None, ge=-90, le=90)
    lon: float | None = Field(default=None, ge=-180, le=180)
    area_m2: float | None = Field(default=None, ge=0)
    rooms: int | None = Field(default=None, ge=0, le=20)
    floor: int | None = Field(default=None, ge=-2, le=200)
    max_floor: int | None = Field(default=None, ge=0, le=200)
    year_built: int | None = Field(default=None, ge=1800, le=datetime.now().year + 1)
    building_type: str | None = None
    ownership: str | None = None
    agent: str | None = None
    agency: str | None = None
    phone: str | None = None
    description: str = ""
    features: list[str] = Field(default_factory=list)
    json_raw: str | None = None
    posted_at: datetime | None = None
    updated_at: datetime | None = None
    first_seen: datetime | None = None
    last_seen: datetime | None = None

    @field_validator("price_currency")
    @classmethod
    def norm_currency(cls, v: str | None) -> str | None:
        return v.upper() if v else v

    @field_validator("features")
    @classmethod
    def strip_features(cls, v: list[str]) -> list[str]:
        return [s.strip() for s in v if s and s.strip()]


class Photo(BaseModel):
    offer_id: str
    source: str = Field(..., pattern=r"^[a-z0-9_]+$")
    seq: int = Field(..., ge=0, le=9999)
    url: HttpUrl
    local_path: str | None = None
    width: int | None = Field(default=None, ge=1, le=20000)
    height: int | None = Field(default=None, ge=1, le=20000)
    bytes: int | None = Field(default=None, ge=1)
    hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{32,64}$")
    status: str | None = Field(default=None)  # ok, failed, retry
    downloaded_at: datetime | None = None
