from datasette.app import Datasette
from datasette_edit_schema.utils import potential_foreign_keys, get_primary_keys
import sqlite_utils
import pytest
import re
from bs4 import BeautifulSoup

whitespace = re.compile(r"\s+")


@pytest.fixture
def db_and_path(tmpdir):
    path = str(tmpdir / "data.db")
    db = sqlite_utils.Database(path)
    db["creatures"].insert_all(
        [
            {"name": "Cleo", "description": "A medium sized dog"},
            {"name": "Siroco", "description": "A troublesome Kakapo"},
        ]
    )
    db["other_table"].insert({"foo": "bar"})
    db["empty_table"].create({"id": int, "name": str}, pk="id")
    # Tables for testing foreign key editing
    db["museums"].insert_all(
        [
            {
                "id": "moma",
                "name": "Museum of Modern Art",
                "city_id": "nyc",
            },
            {
                "id": "tate",
                "name": "Tate Modern",
                "city_id": "london",
            },
            {
                "id": "exploratorium",
                "name": "Exploratorium",
                "city_id": "sf",
            },
            {
                "id": "cablecars",
                "name": "Cable Car Museum",
                "city_id": "sf",
            },
        ],
        pk="id",
    )
    db["cities"].insert_all(
        [
            {
                "id": "nyc",
                "name": "New York City",
            },
            {
                "id": "london",
                "name": "London",
            },
            {
                "id": "sf",
                "name": "San Francisco",
            },
        ],
        pk="id",
    )
    db["distractions"].insert_all(
        [
            {
                "id": "nyc",
                "name": "Nice Yummy Cake",
            }
        ],
        pk="id",
    )
    db["has_foreign_keys"].insert(
        {
            "id": 1,
            "distraction_id": "nyc",
        },
        pk="id",
        foreign_keys=(("distraction_id", "distractions"),),
    )
    return db, path


@pytest.fixture
def db_path(db_and_path):
    return db_and_path[1]


@pytest.fixture
def db(db_and_path):
    return db_and_path[0]


@pytest.mark.asyncio
async def test_csrf_required(db_path):
    ds = Datasette([db_path])
    response = await ds.client.post(
        "/edit-schema/data/creatures",
        data={"delete_table": "1"},
        cookies={"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 403


@pytest.mark.parametrize(
    "authenticated,path,should_allow",
    (
        (False, "/data/creatures", False),
        (True, "/data/creatures", True),
        (True, "/_internal/tables", False),
    ),
)
@pytest.mark.asyncio
async def test_table_actions(db_path, authenticated, path, should_allow):
    ds = Datasette([db_path])
    cookies = None
    if authenticated:
        cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    response = await ds.client.get(path, cookies=cookies)
    assert response.status_code == 200
    fragment = '<li><a href="/-/edit-schema{}">Edit table schema</a></li>'.format(path)
    if should_allow:
        # Should have table action
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
    "post_data,action,expected_columns_dict,expected_order,expected_message",
    [
        # Change column type
        (
            {
                "type.name": "REAL",
            },
            "update_columns",
            {"name": float, "description": str},
            ["name", "description"],
            "Changes to table have been saved",
        ),
        (
            {
                "type.name": "INTEGER",
            },
            "update_columns",
            {"name": int, "description": str},
            ["name", "description"],
            "Changes to table have been saved",
        ),
        # Changing order
        (
            {
                "sort.description": "0",
                "sort.name": "2",
            },
            "update_columns",
            {"name": str, "description": str},
            ["description", "name"],
            "Changes to table have been saved",
        ),
        # Change names
        (
            {
                "name.name": "name2",
                "name.description": "description2",
            },
            "update_columns",
            {"name2": str, "description2": str},
            ["name2", "description2"],
            "Changes to table have been saved",
        ),
        # Add new columns
        (
            {
                "add_column": "1",
                "name": "new_text",
                "type": "text",
            },
            None,
            {"name": str, "description": str, "new_text": str},
            ["name", "description", "new_text"],
            "Column has been added",
        ),
        (
            {
                "add_column": "1",
                "name": "new_integer",
                "type": "integer",
            },
            None,
            {"name": str, "description": str, "new_integer": int},
            ["name", "description", "new_integer"],
            "Column has been added",
        ),
        (
            {
                "add_column": "1",
                "name": "new_float",
                "type": "real",
            },
            None,
            {"name": str, "description": str, "new_float": float},
            ["name", "description", "new_float"],
            "Column has been added",
        ),
        (
            {
                "add_column": "1",
                "name": "new_blob",
                "type": "blob",
            },
            None,
            {"name": str, "description": str, "new_blob": bytes},
            ["name", "description", "new_blob"],
            "Column has been added",
        ),
        # Drop column
        (
            {
                "delete.description": "1",
            },
            "update_columns",
            {"name": str},
            ["name"],
            "Changes to table have been saved",
        ),
    ],
)
async def test_transform_table(
    db_path, action, post_data, expected_columns_dict, expected_order, expected_message
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
    if action:
        post_data["action"] = action
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data=post_data,
        cookies=dict(cookies, ds_csrftoken=csrftoken),
    )
    assert response.status_code == 302
    messages = ds.unsign(response.cookies["ds_messages"], "messages")
    assert table.columns_dict == expected_columns_dict
    assert [c.name for c in table.columns] == expected_order
    assert len(messages) == 1
    assert messages[0][0] == expected_message


@pytest.mark.asyncio
async def test_drop_column_from_table_that_is_part_of_a_view(db_path):
    # https://github.com/simonw/datasette-edit-schema/issues/35
    ds = Datasette([db_path], pdb=True)
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    db = sqlite_utils.Database(db_path)
    db.create_view("creatures_view", "select * from creatures")
    table = db["creatures"]
    assert table.columns_dict == {"name": str, "description": str}
    csrftoken = (
        await ds.client.get("/-/edit-schema/data/creatures", cookies=cookies)
    ).cookies["ds_csrftoken"]
    post_data = {
        "delete.description": "1",
        "csrftoken": csrftoken,
        "action": "update_columns",
    }
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data=post_data,
        cookies=dict(cookies, ds_csrftoken=csrftoken),
    )
    assert response.status_code == 302
    messages = ds.unsign(response.cookies["ds_messages"], "messages")
    assert table.columns_dict == {"name": str}
    assert [c.name for c in table.columns] == ["name"]
    assert len(messages) == 1
    assert messages[0][0] == "Changes to table have been saved"


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,expected_breadcrumbs",
    (
        ("/-/edit-schema/data", ['<a href="/">home</a>', '<a href="/data">data</a>']),
        (
            "/-/edit-schema/data/creatures",
            [
                '<a href="/">home</a>',
                '<a href="/data">data</a>',
                '<a href="/data/creatures">creatures</a>',
            ],
        ),
    ),
)
async def test_breadcrumbs(db_path, path, expected_breadcrumbs):
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    response = await ds.client.get(path, cookies=cookies)
    assert response.status_code == 200
    breadcrumbs = response.text.split('<p class="crumbs">')[1].split("</p>")[0]
    for crumb in expected_breadcrumbs:
        assert crumb in breadcrumbs


def test_potential_foreign_keys(db):
    potentials = potential_foreign_keys(
        db.conn,
        "museums",
        ["name", "city_id"],
        get_primary_keys(db.conn),
    )
    assert potentials == {"name": [], "city_id": [("cities", "id")]}


@pytest.mark.asyncio
async def test_edit_form_shows_suggestions(db_path):
    # Test for suggested foreign keys and primary keys
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    response = await ds.client.get("/-/edit-schema/data/museums", cookies=cookies)
    assert response.status_code == 200
    # Should suggest two of the three columns as primary keys
    soup = BeautifulSoup(response.text, "html5lib")
    assert "<h2>Change the primary key</h2>" in response.text
    pk_options = get_options(soup, "primary_key")
    assert pk_options == [
        {"value": "id", "text": "id (current)", "selected": True},
        {"value": "name", "text": "name", "selected": False},
    ]

    # Test foreign key suggestions
    selects = soup.find_all("select", attrs={"name": re.compile("^fk.")})
    select_options = [(s["name"], get_options(soup, s["name"])) for s in selects]
    assert select_options == [
        (
            "fk.name",
            [
                {
                    "value": "-- no suggestions --",
                    "text": "-- no suggestions --",
                    "selected": False,
                },
                {"value": "cities.id", "text": "cities.id", "selected": False},
                {
                    "value": "distractions.id",
                    "text": "distractions.id",
                    "selected": False,
                },
            ],
        ),
        (
            "fk.city_id",
            [
                {"value": "-- none --", "text": "-- none --", "selected": False},
                {
                    "value": "cities.id",
                    "text": "cities.id (suggested)",
                    "selected": False,
                },
                {
                    "value": "distractions.id",
                    "text": "distractions.id",
                    "selected": False,
                },
            ],
        ),
    ]


@pytest.mark.asyncio
async def test_edit_form_for_empty_table(db_path):
    # https://github.com/simonw/datasette-edit-schema/issues/38
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    response = await ds.client.get("/-/edit-schema/data/empty_table", cookies=cookies)
    assert response.status_code == 200

    # It shouldn't suggest any foreign keys, since there are no records
    assert " (suggested)" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table,post_data,expected_fks,expected_pk,expected_message",
    (
        # Foreign key edit
        (
            "museums",
            {"action": "update_foreign_keys", "fk.city_id": "cities.id"},
            [("museums", "city_id", "cities", "id")],
            ["id"],
            "Foreign keys updated to city_id → cities.id",
        ),
        # No changes to foreign keys
        (
            "museums",
            {"action": "update_foreign_keys"},
            [],
            ["id"],
            "No changes to foreign keys",
        ),
        # Remove foreign keys
        (
            "has_foreign_keys",
            {"action": "update_foreign_keys", "fk.distraction_id": ""},
            [],
            ["id"],
            "Foreign keys removed",
        ),
        # Point existing foreign key at something else
        (
            "has_foreign_keys",
            {"action": "update_foreign_keys", "fk.distraction_id": "cities.id"},
            [("has_foreign_keys", "distraction_id", "cities", "id")],
            ["id"],
            "Foreign keys updated to distraction_id → cities.id",
        ),
        # Change primary key in a way that works
        (
            "museums",
            {"action": "update_primary_key", "primary_key": "name"},
            [],
            ["name"],
            "Primary key for 'museums' is now 'name'",
        ),
        # And a way that returns an error
        (
            "museums",
            {"action": "update_primary_key", "primary_key": "city_id"},
            [],
            ["id"],
            "Column 'city_id' is not unique",
        ),
    ),
)
async def test_edit_keys(
    db_path, table, post_data, expected_fks, expected_pk, expected_message
):
    ds = Datasette([db_path])
    # Grab a csrftoken
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    csrftoken_r = await ds.client.get(
        "/-/edit-schema/data/{}".format(table), cookies=cookies
    )
    csrftoken = csrftoken_r.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    post_data["csrftoken"] = csrftoken
    response = await ds.client.post(
        "/-/edit-schema/data/{}".format(table),
        data=post_data,
        cookies=cookies,
    )
    assert response.status_code == 302
    messages = ds.unsign(response.cookies["ds_messages"], "messages")
    assert len(messages) == 1
    assert messages[0][0] == expected_message
    db = sqlite_utils.Database(db_path)
    assert db[table].foreign_keys == expected_fks
    assert db[table].pks == expected_pk


def get_options(soup, name):
    select = soup.find("select", attrs={"name": name})
    return [
        {
            "value": o.get("value") or o.text,
            "text": o.text,
            "selected": bool(o.get("selected")),
        }
        for o in select.find_all("option")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "post_data,expected_message,expected_schema",
    (
        (
            {"primary_key_name": "id", "primary_key_type": "INTEGER"},
            "Table name is required",
            None,
        ),
        (
            {
                "primary_key_name": "id",
                "primary_key_type": "INTEGER",
                "table_name": "museums",
            },
            "Table already exists",
            None,
        ),
        (
            {
                "primary_key_name": "id",
                "primary_key_type": "INTEGER",
                "table_name": "foo",
            },
            "Table has been created",
            {"id": int},
        ),
        (
            {
                "primary_key_name": "my_pk",
                "primary_key_type": "TEXT",
                "table_name": "foo",
                "column-name.0": "col1_text",
                "column-type.0": "TEXT",
                "column-sort.0": "2",
                "column-name.1": "col2_int",
                "column-type.1": "INTEGER",
                "column-sort.1": "1",
                "column-name.2": "col3_real",
                "column-type.2": "REAL",
                "column-sort.2": "3",
                "column-name.3": "col4_blob",
                "column-type.3": "BLOB",
                "column-sort.3": "4",
            },
            "Table has been created",
            {
                "my_pk": str,
                "col2_int": int,
                "col1_text": str,
                "col3_real": float,
                "col4_blob": bytes,
            },
        ),
    ),
)
async def test_create_table(db_path, post_data, expected_message, expected_schema):
    ds = Datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    csrftoken_r = await ds.client.get("/-/edit-schema/data/-/create", cookies=cookies)
    csrftoken = csrftoken_r.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    post_data["csrftoken"] = csrftoken
    response = await ds.client.post(
        "/-/edit-schema/data/-/create",
        data=post_data,
        cookies=cookies,
    )
    assert response.status_code == 302
    messages = ds.unsign(response.cookies["ds_messages"], "messages")
    assert len(messages) == 1
    assert messages[0][0] == expected_message
    if expected_schema is not None:
        db = sqlite_utils.Database(db_path)
        assert db[post_data["table_name"]].columns_dict == expected_schema
