import httpx
import pytest

from app.main import create_app


@pytest.mark.asyncio
async def test_normal_upstream_client_is_lifespan_owned() -> None:
    app = create_app()
    async with app.router.lifespan_context(app):
        owned = app.state.normal_api_upstream_client
        assert isinstance(owned, httpx.AsyncClient)
        assert not owned.is_closed
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as caller:
            for _ in range(2):
                response = await caller.get("/health/live")
                assert response.status_code == 200
                assert app.state.normal_api_upstream_client is owned
    assert owned.is_closed
