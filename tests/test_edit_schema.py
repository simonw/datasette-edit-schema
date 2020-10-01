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
            "http://localhost/-/edit-schema/data/creatures",
            data={"delete_table": "1"},
            allow_redirects=False,
            cookies={"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")},
        )
    assert 403 == response.status_code


@pytest.mark.asyncio
async def test_post_without_operation_raises_error(db_path):
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    async with httpx.AsyncClient(app=ds.app()) as client:
        # Get a csrftoken
        csrftoken = (
            await client.get(
                "http://localhost/-/edit-schema/data/creatures", cookies=cookies
            )
        ).cookies["ds_csrftoken"]
        cookies["ds_csrftoken"] = csrftoken
        response = await client.post(
            "http://localhost/-/edit-schema/data/creatures",
            data={"csrftoken": csrftoken},
            allow_redirects=False,
            cookies=cookies,
        )
    assert 400 == response.status_code


@pytest.mark.asyncio
async def test_delete_table(db_path):
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    db = sqlite_utils.Database(db_path)
    assert "creatures" in db.table_names()
    async with httpx.AsyncClient(app=ds.app()) as client:
        # Get a csrftoken
        csrftoken = (
            await client.get(
                "http://localhost/-/edit-schema/data/creatures", cookies=cookies
            )
        ).cookies["ds_csrftoken"]
        response = await client.post(
            "http://localhost/-/edit-schema/data/creatures",
            data={"delete_table": "1", "csrftoken": csrftoken},
            allow_redirects=False,
            cookies=cookies,
        )
    assert 302 == response.status_code
    assert "creatures" not in db.table_names()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "col_type,expected_type",
    [("text", str), ("integer", int), ("float", float), ("blob", bytes)],
)
async def test_add_column(db_path, col_type, expected_type):
    ds = Datasette([db_path])
    db = sqlite_utils.Database(db_path)
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    table = db["creatures"]
    assert {"name": str, "description": str} == table.columns_dict
    async with httpx.AsyncClient(app=ds.app()) as client:
        # Get a csrftoken
        csrftoken = (
            await client.get(
                "http://localhost/-/edit-schema/data/creatures", cookies=cookies
            )
        ).cookies["ds_csrftoken"]
        response = await client.post(
            "http://localhost/-/edit-schema/data/creatures",
            data={
                "add_column": "1",
                "csrftoken": csrftoken,
                "name": "new_col",
                "type": col_type,
            },
            allow_redirects=False,
            cookies=cookies,
        )
    assert 302 == response.status_code
    assert {
        "name": str,
        "description": str,
        "new_col": expected_type,
    } == table.columns_dict


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "post_data,expected_columns_dict,expected_order",
    [
        # Change column type
        (
            {
                "type.name": "REAL",
            },
            {"rowid": int, "name": float, "description": str},
            ["rowid", "name", "description"],
        ),
        (
            {
                "type.name": "INTEGER",
            },
            {"rowid": int, "name": int, "description": str},
            ["rowid", "name", "description"],
        ),
        # Changing order
        (
            {
                "sort.description": "0",
                "sort.name": "2",
            },
            {"rowid": int, "name": str, "description": str},
            ["rowid", "description", "name"],
        ),
        # Change names
        (
            {
                "name.name": "name2",
                "name.description": "description2",
            },
            {"rowid": int, "name2": str, "description2": str},
            ["rowid", "name2", "description2"],
        ),
    ],
)
async def test_transform_table(
    db_path, post_data, expected_columns_dict, expected_order
):
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    db = sqlite_utils.Database(db_path)
    table = db["creatures"]
    assert table.columns_dict == {"name": str, "description": str}
    async with httpx.AsyncClient(app=ds.app()) as client:
        csrftoken = (
            await client.get(
                "http://localhost/-/edit-schema/data/creatures", cookies=cookies
            )
        ).cookies["ds_csrftoken"]
        post_data["csrftoken"] = csrftoken
        post_data["action"] = "update_columns"
        response = await client.post(
            "http://localhost/-/edit-schema/data/creatures",
            data=post_data,
            allow_redirects=False,
            cookies=cookies,
        )
    assert 302 == response.status_code
    assert table.columns_dict == expected_columns_dict
    assert [c.name for c in table.columns] == expected_order


@pytest.mark.asyncio
async def test_static_assets(db_path):
    ds = Datasette([db_path])
    async with httpx.AsyncClient(app=ds.app()) as client:
        for path in (
            "/-/static-plugins/datasette-edit-schema/draggable.1.0.0-beta.11.bundle.min.js",
        ):
            response = await client.post(
                "http://localhost" + path,
            )
            assert response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path", ["/-/edit-schema", "/-/edit-schema/data", "/-/edit-schema/data/creatures"]
)
async def test_permission_check(db_path, path):
    ds = Datasette([db_path])
    someuser_cookies = {"ds_actor": ds.sign({"a": {"id": "someuser"}}, "actor")}
    root_cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    async with httpx.AsyncClient(app=ds.app()) as client:
        response = await client.get(
            "http://localhost" + path,
        )
        assert response.status_code == 403
        # Should deny with someuser cookie
        response = await client.get("http://localhost" + path, cookies=someuser_cookies)
        assert response.status_code == 403
        # Should allow with root cookies
        response = await client.get(
            "http://localhost" + path, cookies=root_cookies, allow_redirects=False
        )
        assert response.status_code in (200, 302)
