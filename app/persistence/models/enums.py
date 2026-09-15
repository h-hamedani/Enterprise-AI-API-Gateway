from enum import StrEnum


class ResourceStatus(StrEnum):
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"


class IdempotencyState(StrEnum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RateScopeType(StrEnum):
    API_KEY = "API_KEY"
    ROUTE = "ROUTE"
    SERVICE = "SERVICE"
    LLM_ALIAS = "LLM_ALIAS"
    LLM_MODEL = "LLM_MODEL"
    PROVIDER_TARGET = "PROVIDER_TARGET"
    ADMIN_TOKEN = "ADMIN_TOKEN"


class CertificationStatus(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    CERTIFIED = "CERTIFIED"
    FAILED = "FAILED"


class RetentionState(StrEnum):
    STARTED = "STARTED"
    ROLLED_UP = "ROLLED_UP"
    VERIFIED = "VERIFIED"
    PURGING = "PURGING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
