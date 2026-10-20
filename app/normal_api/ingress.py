"""One raw-ASGI-path interpretation; query bytes are deliberately separate."""

import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import quote, unquote_to_bytes

from app.core.errors import GatewayHttpError, invalid_request, resource_not_found


def unavailable():
    raise GatewayHttpError(500, "gateway_internal_error", "Internal gateway error.")


@dataclass(frozen=True, slots=True, repr=False)
class CanonicalIngress:
    service_slug: str
    remaining_segments: tuple[str, ...]
    encoded_remaining_path: bytes


def canonical_ingress(scope: dict) -> CanonicalIngress:
    raw = scope.get("raw_path")
    if not isinstance(raw, bytes) or not raw:
        unavailable()
    if not raw.startswith(b"/") or b"//" in raw:
        invalid_request()
    if re.search(rb"%(?![0-9a-fA-F]{2})", raw):
        invalid_request()
    try:
        segments = tuple(
            unquote_to_bytes(part).decode("utf-8", "strict")
            for part in raw.split(b"/")[1:]
        )
    except UnicodeDecodeError:
        invalid_request()
    if any("/" in part or "\\" in part or part in {".", ".."} for part in segments):
        invalid_request()
    decoded = "/" + "/".join(segments)
    if unicodedata.normalize("NFC", decoded) != decoded:
        invalid_request()
    if "path" in scope and scope["path"] != decoded:
        unavailable()
    if len(segments) < 2 or segments[0] != "api" or not segments[1]:
        resource_not_found()
    remaining = segments[2:] or ("",)
    return CanonicalIngress(
        segments[1],
        remaining,
        ("/" + "/".join(quote(part, safe="-._~") for part in remaining)).encode(
            "ascii"
        ),
    )


def pattern_matches(pattern: str, segments: tuple[str, ...]) -> bool:
    """Static exact / whole named single-segment parameters; no wildcard DSL."""
    if not isinstance(pattern, str) or not pattern.startswith("/"):
        return False
    if "//" in pattern or unicodedata.normalize("NFC", pattern) != pattern:
        return False  # R22/R23 legacy patterns never authorize execution.
    parts = pattern.split("/")[1:]
    if len(parts) != len(segments):
        return False
    for part, value in zip(parts, segments, strict=True):
        if part in {".", ".."} or "\\" in part:
            return False
        if (
            part.startswith("{")
            and part.endswith("}")
            and len(part) > 2
            and "{" not in part[1:-1]
            and "}" not in part[1:-1]
        ):
            if not value:
                return False
        elif part != value:
            return False
    return True
