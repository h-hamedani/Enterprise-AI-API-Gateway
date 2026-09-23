from types import SimpleNamespace
from uuid import uuid4

from app.control_plane.protection import (
    PRE_AUTH_CLASS,
    pre_auth_policy,
    resolve_client_ip,
)
from app.redis.rate_limit import rate_limit_key


def request(peer: str, **headers):
    return SimpleNamespace(
        client=SimpleNamespace(host=peer),
        headers={key.lower(): value for key, value in headers.items()},
    )


def test_untrusted_forwarding_headers_cannot_spoof_identity():
    item = request("198.51.100.10", **{"X-Forwarded-For": "203.0.113.7"})
    assert resolve_client_ip(item, ["10.0.0.0/8"]) == "198.51.100.10"


def test_trusted_proxy_uses_leftmost_forwarded_client():
    item = request("10.1.2.3", **{"X-Forwarded-For": "203.0.113.7, 10.2.3.4"})
    assert resolve_client_ip(item, ["10.0.0.0/8"]) == "203.0.113.7"


def test_malformed_forwarding_data_falls_back_to_peer():
    item = request("10.1.2.3", **{"X-Forwarded-For": "not-an-ip, 10.2.3.4"})
    assert resolve_client_ip(item, ["10.0.0.0/8"]) == "10.1.2.3"


def test_pre_auth_key_is_shared_class_and_does_not_contain_raw_ip():
    raw_ip = "203.0.113.7"
    key = rate_limit_key(pre_auth_policy(raw_ip, b"s" * 32))
    assert PRE_AUTH_CLASS in key
    assert raw_ip not in key
    assert "adm_" not in key
    assert str(uuid4()) not in key
