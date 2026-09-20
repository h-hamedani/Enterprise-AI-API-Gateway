from __future__ import annotations

from datetime import datetime
from decimal import Decimal
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


class AuditLogResponse(BaseModel):
    id: UUID
    actor_admin_user_id: UUID | None
    actor_admin_token_id: UUID | None
    action: str
    resource_type: str
    resource_id: UUID | None
    request_id: UUID | None
    result: Literal["SUCCESS", "FAILED"]
    created_at: datetime


class AuditPage(BaseModel):
    data: list[AuditLogResponse]
    next_cursor: str | None


class ConfigVersionResponse(BaseModel):
    config_version: int = Field(ge=0)


class DependencyHealth(BaseModel):
    status: str
    reason: str | None = None


class AdminHealthResponse(BaseModel):
    status: str
    degraded_mode: bool
    dependencies: dict[str, DependencyHealth]


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
    secret_type: str
    created_at: datetime
    rotated_at: datetime | None


class ProviderCredentialWrite(StrictModel):
    secret: str
    secret_type: str = "API_KEY"


ProviderTypeValue = Literal["OPENAI", "ANTHROPIC", "VLLM", "GENERIC_OPENAI_COMPAT"]
CapabilityValue = Literal["CHAT", "STREAMING", "TOOLS", "EMBEDDINGS"]


class LlmProviderCreate(StrictModel):
    name: str = Field(min_length=1, max_length=160)
    provider_type: ProviderTypeValue
    status: ServiceStatus = "ACTIVE"


class LlmProviderPatch(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    status: ServiceStatus | None = None

    @model_validator(mode="after")
    def validate_patch(self):
        if not self.model_fields_set or any(
            getattr(self, field) is None for field in self.model_fields_set
        ):
            raise ValueError("At least one non-null field is required.")
        return self


class LlmProviderResponse(LlmProviderCreate):
    id: UUID


class LlmProviderPage(BaseModel):
    data: list[LlmProviderResponse]
    next_cursor: str | None


class LlmTargetCreate(StrictModel):
    provider_id: UUID
    name: str = Field(min_length=1, max_length=160)
    base_url: str = Field(min_length=1, max_length=2048)
    timeout_ms: int = Field(default=30000, ge=1)
    max_concurrent_requests: int | None = Field(default=None, ge=1)
    status: ServiceStatus = "ACTIVE"
    pre_output_idle_timeout_ms: int = Field(default=20000, ge=1000)
    pre_output_budget_ms: int = Field(default=30000, ge=1001, le=120000)
    post_output_idle_timeout_ms: int = Field(default=60000, ge=1000)
    allow_uncertified_runtime: bool = False

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        ServiceCreate.validate_base_url(value)
        return value

    @model_validator(mode="after")
    def validate_budget(self):
        if self.pre_output_budget_ms <= self.pre_output_idle_timeout_ms:
            raise ValueError("pre_output_budget_ms must exceed idle timeout.")
        return self


class LlmTargetPatch(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    timeout_ms: int | None = Field(default=None, ge=1)
    pre_output_idle_timeout_ms: int | None = Field(default=None, ge=1000)
    pre_output_budget_ms: int | None = Field(default=None, ge=1001, le=120000)
    post_output_idle_timeout_ms: int | None = Field(default=None, ge=1000)
    max_concurrent_requests: int | None = Field(default=None, ge=1)
    status: ServiceStatus | None = None
    allow_uncertified_runtime: bool | None = None

    @model_validator(mode="after")
    def validate_patch(self):
        if not self.model_fields_set:
            raise ValueError("At least one field is required.")
        nullable = {"max_concurrent_requests"}
        if any(
            getattr(self, field) is None for field in self.model_fields_set - nullable
        ):
            raise ValueError("Supplied field cannot be null.")
        return self


class LlmTargetResponse(LlmTargetCreate):
    id: UUID
    certification_status: Literal["UNVERIFIED", "CERTIFIED", "FAILED"]
    certified_at: datetime | None = None
    certification_metadata: dict | None = None


class LlmTargetPage(BaseModel):
    data: list[LlmTargetResponse]
    next_cursor: str | None


class LlmModelCreate(StrictModel):
    provider_target_id: UUID
    provider_model_name: str = Field(min_length=1, max_length=255)
    display_name: str | None = Field(default=None, max_length=200)
    status: ServiceStatus = "ACTIVE"


class LlmModelPatch(StrictModel):
    display_name: str | None = Field(default=None, max_length=200)
    status: ServiceStatus | None = None

    @model_validator(mode="after")
    def validate_patch(self):
        if not self.model_fields_set:
            raise ValueError("At least one field is required.")
        if "status" in self.model_fields_set and self.status is None:
            raise ValueError("status cannot be null.")
        return self


class LlmModelResponse(LlmModelCreate):
    id: UUID
    capabilities: list[CapabilityValue]


class LlmModelPage(BaseModel):
    data: list[LlmModelResponse]
    next_cursor: str | None


class CapabilityReplacement(StrictModel):
    capabilities: list[CapabilityValue]

    @field_validator("capabilities")
    @classmethod
    def unique_capabilities(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("Duplicate capability.")
        return value


class AliasTargetWrite(StrictModel):
    model_id: UUID
    priority: int = Field(ge=1)


class AliasTargetReplacement(StrictModel):
    targets: list[AliasTargetWrite] = Field(min_length=1)

    @field_validator("targets")
    @classmethod
    def unique_targets(cls, value):
        if len({item.model_id for item in value}) != len(value) or len(
            {item.priority for item in value}
        ) != len(value):
            raise ValueError("Duplicate model or priority.")
        return value


class LlmAliasCreate(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    status: ServiceStatus = "ACTIVE"


class LlmAliasPatch(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=128)
    status: ServiceStatus | None = None

    @model_validator(mode="after")
    def validate_patch(self):
        if not self.model_fields_set or any(
            getattr(self, field) is None for field in self.model_fields_set
        ):
            raise ValueError("At least one non-null field is required.")
        return self


class LlmAliasResponse(LlmAliasCreate):
    id: UUID
    targets: list[AliasTargetWrite]


class LlmAliasPage(BaseModel):
    data: list[LlmAliasResponse]
    next_cursor: str | None


class ModelPriceCreate(StrictModel):
    input_price_per_unit: Decimal = Field(ge=0, max_digits=20, decimal_places=10)
    output_price_per_unit: Decimal = Field(ge=0, max_digits=20, decimal_places=10)
    unit_tokens: int = Field(ge=1)
    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    effective_from: datetime
    effective_to: datetime | None = None

    @model_validator(mode="after")
    def validate_window(self):
        if self.effective_to is not None and self.effective_to <= self.effective_from:
            raise ValueError("effective_to must follow effective_from.")
        return self


class ModelPricePatch(StrictModel):
    input_price_per_unit: Decimal | None = Field(
        default=None, ge=0, max_digits=20, decimal_places=10
    )
    output_price_per_unit: Decimal | None = Field(
        default=None, ge=0, max_digits=20, decimal_places=10
    )
    unit_tokens: int | None = Field(default=None, ge=1)
    currency: str | None = Field(
        default=None, min_length=3, max_length=3, pattern=r"^[A-Z]{3}$"
    )
    effective_from: datetime | None = None
    effective_to: datetime | None = None

    @model_validator(mode="after")
    def validate_patch(self):
        if not self.model_fields_set:
            raise ValueError("At least one field is required.")
        if any(
            getattr(self, field) is None
            for field in self.model_fields_set - {"effective_to"}
        ):
            raise ValueError("Supplied economic fields cannot be null.")
        return self


class ModelPriceResponse(ModelPriceCreate):
    id: UUID
    model_id: UUID
