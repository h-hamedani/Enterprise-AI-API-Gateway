"""Versioned Redis namespace constants without workload-specific key builders."""

REDIS_NAMESPACE_PREFIX = "gw:v1:"
M3_INVALIDATION_CHANNEL = f"{REDIS_NAMESPACE_PREFIX}invalidate"
HISTORICAL_M2_INVALIDATION_CHANNEL = "gateway:config"
