from uuid import UUID

from app.core.request_context import new_request_id


def test_request_id_is_uuid_v7() -> None:
    request_id = new_request_id()

    assert isinstance(request_id, UUID)
    assert request_id.version == 7
