from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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


PermissionResource = Literal["SERVICE", "ROUTE", "LLM_ALIAS", "LLM_MODEL"]


class ApiKeyPermissionWrite(StrictModel):
    resource_type: PermissionResource
    resource_id: UUID
    action: Literal["INVOKE"]


class ApiKeyPermission(ApiKeyPermissionWrite):
    id: UUID


class ApiKeyPermissionReplacement(StrictModel):
    permissions: list[ApiKeyPermissionWrite]


class ApiKeyPermissionList(BaseModel):
    data: list[ApiKeyPermission]


ServiceStatus = Literal["ACTIVE", "DISABLED"]
HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]


class ServiceCreate(StrictModel):
    name: str = Field(min_length=1, max_length=200)
    base_url: str = Field(min_length=1, max_length=2048)
    slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$", max_length=63)
    connect_timeout_seconds: int = Field(default=5, ge=1)
    pool_timeout_seconds: int = Field(default=5, ge=1)
    write_timeout_seconds: int = Field(default=30, ge=1)
    read_idle_timeout_seconds: int = Field(default=60, ge=1)
    pre_response_timeout_seconds: int = Field(default=120, ge=1)
    status: ServiceStatus = "ACTIVE"

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        from urllib.parse import urlsplit

        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url must be an absolute HTTP(S) URL.")
        return value


class ServicePatch(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    base_url: str | None = Field(default=None, min_length=1, max_length=2048)
    connect_timeout_seconds: int | None = Field(default=None, ge=1)
    pool_timeout_seconds: int | None = Field(default=None, ge=1)
    write_timeout_seconds: int | None = Field(default=None, ge=1)
    read_idle_timeout_seconds: int | None = Field(default=None, ge=1)
    pre_response_timeout_seconds: int | None = Field(default=None, ge=1)
    status: ServiceStatus | None = None

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str | None) -> str | None:
        if value is not None:
            ServiceCreate.validate_base_url(value)
        return value

    @model_validator(mode="after")
    def require_change(self) -> ServicePatch:
        if not self.model_fields_set:
            raise ValueError("At least one field is required.")
        if any(getattr(self, field) is None for field in self.model_fields_set):
            raise ValueError("Service patch fields cannot be null.")
        return self


class ServiceResponse(BaseModel):
    id: UUID
    name: str
    base_url: str
    slug: str
    connect_timeout_seconds: int
    pool_timeout_seconds: int
    write_timeout_seconds: int
    read_idle_timeout_seconds: int
    pre_response_timeout_seconds: int
    status: ServiceStatus


class ServicePage(BaseModel):
    data: list[ServiceResponse]
    next_cursor: str | None


class RouteCreate(StrictModel):
    service_id: UUID
    path_pattern: str = Field(min_length=1, max_length=2048, pattern=r"^/")
    method: HttpMethod
    upstream_path_template: str = Field(min_length=1, max_length=1024)
    header_policy: dict | None = None
    priority: int = Field(ge=0)
    timeout_ms: int | None = Field(default=None, ge=1)
    status: ServiceStatus = "ACTIVE"


class RoutePatch(StrictModel):
    path_pattern: str | None = Field(
        default=None, min_length=1, max_length=2048, pattern=r"^/"
    )
    method: HttpMethod | None = None
    upstream_path_template: str | None = Field(
        default=None, min_length=1, max_length=1024
    )
    header_policy: dict | None = None
    priority: int | None = Field(default=None, ge=0)
    timeout_ms: int | None = Field(default=None, ge=1)
    status: ServiceStatus | None = None

    @model_validator(mode="after")
    def require_change(self) -> RoutePatch:
        if not self.model_fields_set:
            raise ValueError("At least one field is required.")
        non_nullable = self.model_fields_set - {"header_policy", "timeout_ms"}
        if any(getattr(self, field) is None for field in non_nullable):
            raise ValueError("Route patch field cannot be null.")
        return self


class RouteResponse(BaseModel):
    id: UUID
    service_id: UUID
    path_pattern: str
    method: HttpMethod
    upstream_path_template: str
    header_policy: dict | None
    priority: int
    timeout_ms: int | None
    status: ServiceStatus


class RoutePage(BaseModel):
    data: list[RouteResponse]
    next_cursor: str | None


class ServiceCredentialWrite(StrictModel):
    auth_type: Literal["NONE", "STATIC_BEARER", "STATIC_HEADER"]
    secret: str | None = Field(default=None, min_length=1)
    header_name: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def validate_auth_fields(self) -> ServiceCredentialWrite:
        import re

        if self.auth_type == "NONE" and (
            self.secret is not None or self.header_name is not None
        ):
            raise ValueError("NONE does not accept secret or header_name.")
        if self.auth_type == "STATIC_BEARER" and (
            self.secret is None or self.header_name is not None
        ):
            raise ValueError("STATIC_BEARER requires only secret.")
        if self.auth_type == "STATIC_HEADER":
            if self.secret is None or self.header_name is None:
                raise ValueError("STATIC_HEADER requires secret and header_name.")
            if re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", self.header_name) is None:
                raise ValueError("header_name is invalid.")
        return self


class CredentialMetadata(BaseModel):
    id: UUID
    status: ServiceStatus
    secret_type: Literal["NONE", "STATIC_BEARER", "STATIC_HEADER"]
    created_at: datetime
    rotated_at: datetime | None
