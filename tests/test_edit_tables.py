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
    ds = Datasette([db_path])
    async with httpx.AsyncClient(app=ds.app()) as client:
        response = await client.post(
            "http://localhost/-/edit-tables/data/creatures",
            data={"delete_table": "1"},
            allow_redirects=False,
            cookies={"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")},
        )
    assert 403 == response.status_code


@pytest.mark.asyncio
async def test_post_without_operation_errror(db_path):
    app = Datasette([db_path]).app()
    async with httpx.AsyncClient(app=app) as client:
        # Get a csrftoken
        csrftoken = (
            await client.get("http://localhost/-/edit-tables/data/creatures")
        ).cookies["ds_csrftoken"]
        response = await client.post(
            "http://localhost/-/edit-tables/data/creatures",
            data={"csrftoken": csrftoken},
            allow_redirects=False,
        )
    assert 400 == response.status_code


@pytest.mark.asyncio
async def test_delete_table(db_path):
    app = Datasette([db_path]).app()
    db = sqlite_utils.Database(db_path)
    assert "creatures" in db.table_names()
    async with httpx.AsyncClient(app=app) as client:
        # Get a csrftoken
        csrftoken = (
            await client.get("http://localhost/-/edit-tables/data/creatures")
        ).cookies["ds_csrftoken"]
        response = await client.post(
            "http://localhost/-/edit-tables/data/creatures",
            data={"delete_table": "1", "csrftoken": csrftoken},
            allow_redirects=False,
        )
    assert 302 == response.status_code
    assert "creatures" not in db.table_names()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "col_type,expected_type",
    [("text", str), ("integer", int), ("float", float), ("blob", bytes)],
)
async def test_add_column(db_path, col_type, expected_type):
    app = Datasette([db_path]).app()
    db = sqlite_utils.Database(db_path)
    table = db["creatures"]
    assert {"name": str, "description": str} == table.columns_dict
    async with httpx.AsyncClient(app=app) as client:
        # Get a csrftoken
        csrftoken = (
            await client.get("http://localhost/-/edit-tables/data/creatures")
        ).cookies["ds_csrftoken"]
        response = await client.post(
            "http://localhost/-/edit-tables/data/creatures",
            data={
                "add_column": "1",
                "csrftoken": csrftoken,
                "name": "new_col",
                "type": col_type,
            },
            allow_redirects=False,
        )
    assert 302 == response.status_code
    assert {
        "name": str,
        "description": str,
        "new_col": expected_type,
    } == table.columns_dict


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "post_data,expected_columns_dict",
    [
        (
            {
                "type.name": "REAL",
            },
            {"rowid": int, "name": float, "description": str},
        ),
    ],
)
async def test_transform_table(db_path, post_data, expected_columns_dict):
    app = Datasette([db_path]).app()
    db = sqlite_utils.Database(db_path)
    table = db["creatures"]
    assert table.columns_dict == {"name": str, "description": str}
    async with httpx.AsyncClient(app=app) as client:
        csrftoken = (
            await client.get("http://localhost/-/edit-tables/data/creatures")
        ).cookies["ds_csrftoken"]
        post_data["csrftoken"] = csrftoken
        post_data["action"] = "update_columns"
        response = await client.post(
            "http://localhost/-/edit-tables/data/creatures",
            data=post_data,
            allow_redirects=False,
        )
    assert 302 == response.status_code
    assert table.columns_dict == expected_columns_dict


@pytest.mark.asyncio
async def test_static_assets(db_path):
    ds = Datasette([db_path])
    async with httpx.AsyncClient(app=ds.app()) as client:
        for path in (
            "/-/static-plugins/datasette-edit-tables/draggable.1.0.0-beta.11.bundle.min.js",
        ):
            response = await client.post(
                "http://localhost" + path,
            )
            assert response.status_code == 200
