from enum import StrEnum


class ResourceStatus(StrEnum):
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"


class PrincipalStatus(StrEnum):
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"
    REVOKED = "REVOKED"


class PermissionResourceType(StrEnum):
    SERVICE = "SERVICE"
    ROUTE = "ROUTE"
    LLM_ALIAS = "LLM_ALIAS"
    LLM_MODEL = "LLM_MODEL"


class PermissionAction(StrEnum):
    INVOKE = "INVOKE"


class ServiceAuthType(StrEnum):
    NONE = "NONE"
    STATIC_BEARER = "STATIC_BEARER"
    STATIC_HEADER = "STATIC_HEADER"


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


class ProviderType(StrEnum):
    OPENAI = "OPENAI"
    ANTHROPIC = "ANTHROPIC"
    VLLM = "VLLM"
    GENERIC_OPENAI_COMPAT = "GENERIC_OPENAI_COMPAT"


class ModelCapability(StrEnum):
    CHAT = "CHAT"
    STREAMING = "STREAMING"
    TOOLS = "TOOLS"
    EMBEDDINGS = "EMBEDDINGS"


class RetentionState(StrEnum):
    STARTED = "STARTED"
    ROLLED_UP = "ROLLED_UP"
    VERIFIED = "VERIFIED"
    PURGING = "PURGING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WorkloadType(StrEnum):
    NORMAL = "NORMAL"
    LLM = "LLM"


class LlmFinalStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CLIENT_CANCELLED = "CLIENT_CANCELLED"


class AttemptStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AuditResult(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class RetentionJobType(StrEnum):
    API_RETENTION = "API_RETENTION"
    LLM_RETENTION = "LLM_RETENTION"
