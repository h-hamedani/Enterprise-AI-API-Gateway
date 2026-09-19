from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ApplicationCreate(StrictModel):
    name: str = Field(min_length=1, max_length=200)


class ApplicationPatch(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    status: Literal["ACTIVE", "DISABLED"] | None = None

    @model_validator(mode="after")
    def require_change(self) -> ApplicationPatch:
        if self.name is None and self.status is None:
            raise ValueError("At least one field is required.")
        return self


class ApplicationResponse(BaseModel):
    id: UUID
    name: str
    status: Literal["ACTIVE", "DISABLED"]


class ApplicationPage(BaseModel):
    data: list[ApplicationResponse]
    next_cursor: str | None


class ApiKeyCreate(StrictModel):
    application_id: UUID
    name: str = Field(max_length=200)
    expires_at: datetime | None = None


class ApiKeyMetadata(BaseModel):
    id: UUID
    name: str
    key_prefix: str
    status: Literal["ACTIVE", "DISABLED", "REVOKED"]
    expires_at: datetime | None


class ApiKeyCreated(ApiKeyMetadata):
    key: str = Field(pattern=r"^gw_")


class ApiKeyPage(BaseModel):
    data: list[ApiKeyMetadata]
    next_cursor: str | None
