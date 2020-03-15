import datasette
from datasette.app import Datasette
import sqlite_utils
import pytest
import json
import httpx
import re

whitespace = re.compile(r"\s+")


@pytest.fixture
def db_path(tmpdir):
    path = str(tmpdir / "data.db")
    db = sqlite_utils.Database(path)
    db["creatures"].insert_all(
        [
            {"name": "Cleo", "description": "A medium sized dog"},
            {"name": "Siroco", "description": "A troublesome Kakapo"},
        ]
    )
    return path


@pytest.mark.asyncio
async def test_csrf_required(db_path):
    app = Datasette([db_path]).app()
    async with httpx.AsyncClient(app=app) as client:
        response = await client.post(
            "http://localhost/-/edit-tables/data/creatures", data={"delete_table": "1"}
        )
    assert 403 == response.status_code


@pytest.mark.asyncio
async def test_delete_table(db_path):
    app = Datasette([db_path]).app()
    db = sqlite_utils.Database(db_path)
    assert "creatures" in db.table_names()
    async with httpx.AsyncClient(app=app) as client:
        # Get a csrftoken
        csrftoken = (
            await client.get("http://localhost/-/edit-tables/data/creatures")
        ).cookies["csrftoken"]
        response = await client.post(
            "http://localhost/-/edit-tables/data/creatures",
            data={"delete_table": "1", "csrftoken": csrftoken},
            allow_redirects=False,
        )
    assert 302 == response.status_code
    assert "creatures" not in db.table_names()
