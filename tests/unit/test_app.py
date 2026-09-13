from fastapi import FastAPI

from app.main import create_app


def test_create_app() -> None:
    app = create_app()

    assert isinstance(app, FastAPI)
    assert app.title == "Enterprise AI & API Gateway"
