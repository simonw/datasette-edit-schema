import datasette
from datasette.app import Datasette
import sqlite_utils
import pytest
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
    db["other_table"].insert({"foo": "bar"})
    return path


@pytest.mark.asyncio
async def test_csrf_required(db_path):
    ds = Datasette([db_path])
    response = await ds.client.post(
        "/edit-schema/data/creatures",
        data={"delete_table": "1"},
        cookies={"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 403


@pytest.mark.parametrize("authenticate", [True, False])
@pytest.mark.asyncio
async def test_table_actions(db_path, authenticate):
    ds = Datasette([db_path])
    cookies = None
    if authenticate:
        cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    response = await ds.client.get("/data/creatures", cookies=cookies)
    assert response.status_code == 200
    fragment = '<li><a href="/-/edit-schema/data/creatures">Edit table schema</a></li>'
    if authenticate:
        # Should have column actions
        assert fragment in response.text
    else:
        assert fragment not in response.text


@pytest.mark.asyncio
async def test_post_without_operation_raises_error(db_path):
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    # Get a csrftoken
    csrftoken = (
        await ds.client.get("/-/edit-schema/data/creatures", cookies=cookies)
    ).cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data={"csrftoken": csrftoken},
        cookies=cookies,
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_delete_table(db_path):
    ds = Datasette([db_path])
    db = sqlite_utils.Database(db_path)
    assert "creatures" in db.table_names()
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    # Get a csrftoken
    csrftoken = (
        await ds.client.get("/-/edit-schema/data/creatures", cookies=cookies)
    ).cookies["ds_csrftoken"]
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data={"delete_table": "1", "csrftoken": csrftoken},
        cookies=dict(cookies, ds_csrftoken=csrftoken),
    )
    assert response.status_code == 302
    assert "creatures" not in db.table_names()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "col_type,expected_type",
    [("text", str), ("integer", int), ("real", float), ("blob", bytes)],
)
async def test_add_column(db_path, col_type, expected_type):
    ds = Datasette([db_path])
    db = sqlite_utils.Database(db_path)
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    table = db["creatures"]
    assert {"name": str, "description": str} == table.columns_dict
    # Get a csrftoken
    csrftoken = (
        await ds.client.get("/-/edit-schema/data/creatures", cookies=cookies)
    ).cookies["ds_csrftoken"]
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data={
            "add_column": "1",
            "csrftoken": csrftoken,
            "name": "new_col",
            "type": col_type,
        },
        cookies=dict(cookies, ds_csrftoken=csrftoken),
    )
    assert response.status_code == 302
    if "ds_messages" in response.cookies:
        messages = ds.unsign(response.cookies["ds_messages"], "messages")
        # None of these should be errors
        assert all(m[1] == Datasette.INFO for m in messages), "Got an error: {}".format(
            messages
        )
    assert {
        "name": str,
        "description": str,
        "new_col": expected_type,
    } == table.columns_dict


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,type,expected_error",
    [
        ("name", "text", "A column called 'name' already exists"),
        ("", "text", "Column name is required"),
        ("]]]", "integer", 'unrecognized token: "]"'),
        ("name", "blop", "Invalid type: blop"),
    ],
)
async def test_add_column_errors(db_path, name, type, expected_error):
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    csrftoken = (
        await ds.client.get("/-/edit-schema/data/creatures", cookies=cookies)
    ).cookies["ds_csrftoken"]
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data={
            "add_column": "1",
            "name": name,
            "type": type,
            "csrftoken": csrftoken,
        },
        cookies=dict(cookies, ds_csrftoken=csrftoken),
    )
    assert response.status_code == 302
    assert response.headers["location"] == "/-/edit-schema/data/creatures"
    messages = ds.unsign(response.cookies["ds_messages"], "messages")
    assert len(messages) == 1
    assert messages[0][0] == expected_error


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
    csrftoken = (
        await ds.client.get("/-/edit-schema/data/creatures", cookies=cookies)
    ).cookies["ds_csrftoken"]
    post_data["csrftoken"] = csrftoken
    post_data["action"] = "update_columns"
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data=post_data,
        cookies=dict(cookies, ds_csrftoken=csrftoken),
    )
    assert response.status_code == 302
    assert table.columns_dict == expected_columns_dict
    assert [c.name for c in table.columns] == expected_order


@pytest.mark.asyncio
async def test_static_assets(db_path):
    ds = Datasette([db_path])
    for path in (
        "/-/static-plugins/datasette-edit-schema/draggable.1.0.0-beta.11.bundle.min.js",
    ):
        response = await ds.client.post(path)
        assert response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path", ["/-/edit-schema", "/-/edit-schema/data", "/-/edit-schema/data/creatures"]
)
async def test_permission_check(db_path, path):
    ds = Datasette([db_path])
    someuser_cookies = {"ds_actor": ds.sign({"a": {"id": "someuser"}}, "actor")}
    root_cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    response = await ds.client.get(path)
    assert response.status_code == 403
    # Should deny with someuser cookie
    response2 = await ds.client.get("" + path, cookies=someuser_cookies)
    assert response2.status_code == 403
    # Should allow with root cookies
    response3 = await ds.client.get("" + path, cookies=root_cookies)
    assert response3.status_code in (200, 302)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "new_name,should_work,expected_message",
    [
        ("valid", True, "Table renamed to 'valid'"),
        ("]]]", False, 'Error renaming table: unrecognized token: "]"'),
        ("creatures", True, "Table name was the same"),
        ("", False, "New table name is required"),
        ("other_table", False, "A table called 'other_table' already exists"),
    ],
)
async def test_rename_table(db_path, new_name, should_work, expected_message):
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    csrftoken = (
        await ds.client.get("/-/edit-schema/data/creatures", cookies=cookies)
    ).cookies["ds_csrftoken"]
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data={
            "rename_table": "1",
            "name": new_name,
            "csrftoken": csrftoken,
        },
        cookies=dict(cookies, ds_csrftoken=csrftoken),
    )
    assert response.status_code == 302
    if should_work:
        expected_path = "/-/edit-schema/data/{}".format(new_name)
    else:
        expected_path = "/-/edit-schema/data/creatures"
    assert response.headers["location"] == expected_path
    messages = ds.unsign(response.cookies["ds_messages"], "messages")
    assert len(messages) == 1
    assert messages[0][0] == expected_message
