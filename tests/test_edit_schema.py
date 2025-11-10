from datasette.app import Datasette
from datasette.utils import tilde_encode
from datasette_edit_schema.utils import (
    potential_foreign_keys,
    get_primary_keys,
    examples_for_columns,
    potential_primary_keys,
)
import sqlite_utils
import pytest
import re
from bs4 import BeautifulSoup
from .conftest import Rule

whitespace = re.compile(r"\s+")


def get_last_event(datasette):
    # Returns None of events are not tracked
    events = getattr(datasette, "_tracked_events", [])
    if events:
        return events[-1]


def root_datasette(*args, **kwargs):
    ds = Datasette(*args, **kwargs)
    ds.root_enabled = True
    return ds


@pytest.mark.asyncio
async def test_csrf_required(db_path):
    ds = root_datasette([db_path])
    response = await ds.client.post(
        "/edit-schema/data/creatures",
        data={"drop_table": "1"},
        cookies={"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")},
    )
    assert response.status_code == 403


@pytest.mark.parametrize(
    "actor_id,should_allow",
    (
        (None, False),
        ("user_with_edit_schema", True),
        ("user_with_create_table", False),
        ("user_with_no_perms", False),
    ),
)
@pytest.mark.parametrize("table", ("creatures", "animal.name/with/slashes"))
@pytest.mark.asyncio
async def test_table_actions(permission_plugin, ds, actor_id, should_allow, table):
    ds._rules_allow = [
        Rule(
            actor_id="user_with_edit_schema",
            action="edit-schema",
            database="data",
            resource=None,
        ),
        Rule(
            actor_id="user_with_create_table",
            action="create-table",
            database="data",
            resource=None,
        ),
    ]
    cookies = None
    if actor_id:
        cookies = {"ds_actor": ds.sign({"a": {"id": actor_id}}, "actor")}
    response = await ds.client.get(
        ds.urls.table(database="data", table=table), cookies=cookies
    )
    assert response.status_code == 200
    fragment = '<a href="/-/edit-schema/data/{}">Edit table schema'.format(
        tilde_encode(table)
    )
    if should_allow:
        # Should have table action
        assert fragment in response.text
    else:
        assert fragment not in response.text


@pytest.mark.asyncio
async def test_post_without_operation_raises_error(db_path):
    ds = root_datasette([db_path])
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
@pytest.mark.parametrize(
    "actor_id,should_allow",
    (
        (None, False),
        ("user_with_edit_schema", True),
        ("user_with_just_create_table", False),
        ("user_with_alter_and_drop_table", True),
    ),
)
async def test_drop_table(permission_plugin, db_path, actor_id, should_allow):
    ds = root_datasette([db_path], pdb=True)
    ds._rules_allow = [
        Rule(
            actor_id="user_with_edit_schema",
            action="edit-schema",
            database="data",
            resource=None,
        ),
        Rule(
            actor_id="user_with_alter_and_drop_table",
            action="drop-table",
            database="data",
            resource="creatures",
        ),
        Rule(
            actor_id="user_with_alter_and_drop_table",
            action="alter-table",
            database="data",
            resource="creatures",
        ),
        Rule(
            actor_id="user_with_just_create_table",
            action="create-table",
            database="data",
            resource=None,
        ),
    ]
    db = sqlite_utils.Database(db_path)
    assert "creatures" in db.table_names()
    cookies = {}
    if actor_id:
        cookies = {"ds_actor": ds.sign({"a": {"id": actor_id}}, "actor")}
    # Get a csrftoken
    form_response = await ds.client.get(
        "/-/edit-schema/data/creatures", cookies=cookies
    )
    if actor_id in (None, "user_with_just_create_table"):
        assert form_response.status_code == 403
        return
    assert form_response.status_code == 200
    csrftoken = form_response.cookies["ds_csrftoken"]
    if should_allow:
        assert 'name="drop_table"' in form_response.text
    else:
        assert 'name="drop_table"' not in form_response.text
    # Try submitting form anyway
    response = await ds.client.post(
        "/-/edit-schema/data/creatures",
        data={"drop_table": "1", "csrftoken": csrftoken},
        cookies=dict(cookies, ds_csrftoken=csrftoken),
    )
    if should_allow:
        assert response.status_code == 302
        assert "creatures" not in db.table_names()
        event = get_last_event(ds)
        if event is not None:
            assert event.name == "drop-table"
            # This should have used isolated_fn as well:
            assert getattr(ds, "_datasette_edit_schema_used_execute_isolated_fn", None)
    else:
        assert response.status_code == 403
        assert "creatures" in db.table_names()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "col_type,expected_type",
    [("text", str), ("integer", int), ("real", float), ("blob", bytes)],
)
async def test_add_column(db_path, col_type, expected_type):
    ds = root_datasette([db_path])
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
    ds = root_datasette([db_path])
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
    ds = root_datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    db = sqlite_utils.Database(db_path)
    table = db["creatures"]
    before_schema = table.schema
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
    # Should have tracked an event
    event = get_last_event(ds)
    assert event.name == "alter-table"
    assert event.before_schema == before_schema
    assert event.after_schema == table.schema


@pytest.mark.asyncio
async def test_drop_column_from_table_that_is_part_of_a_view(db_path):
    # https://github.com/simonw/datasette-edit-schema/issues/35
    ds = root_datasette([db_path], pdb=True)
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
    ds = root_datasette([db_path])
    for path in (
        "/-/static-plugins/datasette-edit-schema/draggable.1.0.0-beta.11.bundle.min.js",
    ):
        response = await ds.client.post(path)
        assert response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path", ["/-/edit-schema", "/-/edit-schema/data", "/-/edit-schema/data/creatures"]
)
async def test_permission_edit_schema(db_path, path):
    # root user has edit-schema which allows access to all
    ds = root_datasette([db_path])
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
    "rules_allow,should_work",
    (
        (
            [
                Rule(
                    actor_id="user",
                    action="edit-schema",
                    database="data",
                    resource=None,
                ),
            ],
            True,
        ),
        (
            [
                Rule(
                    actor_id="user2",
                    action="edit-schema",
                    database="data",
                    resource=None,
                ),
            ],
            False,
        ),
        (
            [
                Rule(
                    actor_id="user",
                    action="create-table",
                    database="data",
                    resource=None,
                ),
            ],
            True,
        ),
        (
            [
                Rule(
                    actor_id="user2",
                    action="create-table",
                    database="data",
                    resource=None,
                ),
            ],
            False,
        ),
    ),
)
async def test_permission_create_table(permission_plugin, ds, rules_allow, should_work):
    ds._rules_allow = rules_allow
    cookies = {"ds_actor": ds.sign({"a": {"id": "user"}}, "actor")}
    csrftoken_r = await ds.client.get("/-/edit-schema/data/-/create", cookies=cookies)
    if not should_work:
        assert csrftoken_r.status_code == 403
        return
    assert csrftoken_r.status_code == 200
    csrftoken = csrftoken_r.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    post_data = {
        "primary_key_name": "id",
        "primary_key_type": "INTEGER",
        "table_name": "foo",
        "csrftoken": csrftoken,
    }
    response = await ds.client.post(
        "/-/edit-schema/data/-/create",
        data=post_data,
        cookies=cookies,
    )
    assert response.status_code == 302


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rules_allow,should_work",
    (
        (
            [
                Rule(
                    actor_id="user",
                    action="edit-schema",
                    database="data",
                    resource=None,
                ),
            ],
            True,
        ),
        (
            [
                Rule(
                    actor_id="user2",
                    action="edit-schema",
                    database="data",
                    resource=None,
                ),
            ],
            False,
        ),
        (
            [
                Rule(
                    actor_id="user",
                    action="alter-table",
                    database="data",
                    resource="museums",
                ),
            ],
            True,
        ),
        (
            [
                Rule(
                    actor_id="user2",
                    action="alter-table",
                    database="data",
                    resource="museums",
                ),
            ],
            False,
        ),
    ),
)
async def test_permission_alter_table(permission_plugin, ds, rules_allow, should_work):
    ds._rules_allow = rules_allow
    cookies = {"ds_actor": ds.sign({"a": {"id": "user"}}, "actor")}
    csrftoken_r = await ds.client.get("/-/edit-schema/data/museums", cookies=cookies)
    if not should_work:
        assert csrftoken_r.status_code == 403
        return
    assert csrftoken_r.status_code == 200
    csrftoken = csrftoken_r.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    post_data = {
        "action": "update_primary_key",
        "primary_key": "name",
        "csrftoken": csrftoken,
    }
    response = await ds.client.post(
        "/-/edit-schema/data/museums",
        data=post_data,
        cookies=cookies,
    )
    assert response.status_code == 302


@pytest.mark.asyncio
async def test_table_form_contains_schema(permission_plugin, ds):
    ds._rules_allow = [
        Rule(
            actor_id="user",
            action="edit-schema",
            database="data",
            resource=None,
        ),
    ]
    response = await ds.client.get(
        "/-/edit-schema/data/creatures",
        cookies={"ds_actor": ds.sign({"a": {"id": "user"}}, "actor")},
    )
    assert response.status_code == 200
    assert (
        "CREATE TABLE [creatures]" in response.text
        # In case we remove '[' in the future:
        or "CREATE TABLE creatures" in response.text
    )


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
    ds = root_datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    csrftoken = (
        await ds.client.get("/-/edit-schema/data/creatures", cookies=cookies)
    ).cookies["ds_csrftoken"]
    before_schema = sqlite_utils.Database(db_path)["creatures"].schema
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
        if expected_message != "Table name was the same":
            event = get_last_event(ds)
            if event:
                assert event.name == "alter-table"
                assert event.table == new_name
                assert new_name in event.properties()["after_schema"]
                assert "creatures" in event.properties()["before_schema"]

    else:
        expected_path = "/-/edit-schema/data/creatures"
    assert response.headers["location"] == expected_path
    messages = ds.unsign(response.cookies["ds_messages"], "messages")
    assert len(messages) == 1
    assert messages[0][0] == expected_message
    if should_work:
        # Should have tracked alter-table against the new table name
        event = get_last_event(ds)
        if expected_message == "Table name was the same":
            assert event is None
        else:
            assert event.name == "alter-table"
            assert event.before_schema == before_schema
            assert event.after_schema == sqlite_utils.Database(db_path)[new_name].schema


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
    ds = root_datasette([db_path])
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
    ds = root_datasette([db_path])
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
            "fk.id",
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
    ds = root_datasette([db_path])
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
        # Set the primary key to be a foreign key
        (
            "museums",
            {"action": "update_foreign_keys", "fk.id": "cities.id"},
            [("museums", "id", "cities", "id")],
            ["id"],
            "Foreign keys updated to id → cities.id",
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
        # Same again for tables with weird characters in their names
        (
            "animal.name/with/slashes",
            {
                "action": "update_foreign_keys",
                "fk.species": "table~2Ename~2Fwith~2Fslashes~2Ecategories.id",
            },
            [
                (
                    "animal.name/with/slashes",
                    "species",
                    "table.name/with/slashes.categories",
                    "id",
                )
            ],
            ["id"],
            "Foreign keys updated to species → table.name/with/slashes.categories.id",
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
    ds = root_datasette([db_path])
    # Grab a csrftoken
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    csrftoken_r = await ds.client.get(
        "/-/edit-schema/data/{}".format(tilde_encode(table)), cookies=cookies
    )
    csrftoken = csrftoken_r.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    post_data["csrftoken"] = csrftoken
    response = await ds.client.post(
        "/-/edit-schema/data/{}".format(tilde_encode(table)),
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
    ds = root_datasette([db_path])
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
    # create-table should have been tracked
    event = get_last_event(ds)
    if event:
        assert event.name == "create-table"


def test_examples_for_columns():
    db = sqlite_utils.Database(memory=True)
    db["examples"].insert_all(
        [
            {"id": 1, "name": "Name 1", "age": 15, "weight": None, "photo's": b"Blob"},
            {"id": 2, "name": None, "age": 25, "weight": 2.3, "photo's": b"Blob2"},
            {"id": 3, "name": "", "age": None, "weight": 2.0, "photo's": b"Blob3"},
            {"id": 4, "name": "Name 4", "age": 18, "weight": 1.7, "photo's": b"Blob4"},
            {"id": 5, "name": "Name 5", "age": 21, "weight": None, "photo's": b"Blob5"},
            {"id": 6, "name": "Name 6", "age": 35, "weight": 2.5, "photo's": b"Blob6"},
            {"id": 7, "name": "Name 7", "age": 28, "weight": 1.9, "photo's": b"Blob7"},
            {"id": 8, "name": "Name 8", "age": 22, "weight": 2.1, "photo's": b"Blob8"},
            {"id": 9, "name": "Name 9", "age": 20, "weight": 1.5, "photo's": b"Blob9"},
            {
                "id": 10,
                "name": "Name 10",
                "age": 40,
                "weight": 2.8,
                "photo's": b"Blob10",
            },
        ]
    )
    examples = examples_for_columns(db.conn, "examples")
    assert examples == {
        "age": ["15", "25", "18", "21", "35"],
        "id": ["1", "2", "3", "4", "5"],
        "name": ["Name 1", "Name 4", "Name 5", "Name 6", "Name 7"],
        "weight": ["2.3", "2.0", "1.7", "2.5", "1.9"],
    }


def test_potential_primary_keys():
    db = sqlite_utils.Database(memory=True)
    db["examples"].insert_all(
        [
            {"id": 1, "photo's": b"Blob", "cat": "1"},
            {"id": 2, "photo's": b"Blob2", "cat": "1"},
            {"id": 3, "photo's": b"Blob3", "cat": "2"},
        ]
    )
    potentials = potential_primary_keys(db.conn, "examples", ["id", "photo's", "cat"])
    assert potentials == ["id", "photo's"]


def test_potential_primary_keys_primary_key_only_table():
    # https://github.com/simonw/datasette-edit-schema/issues/51
    db = sqlite_utils.Database(memory=True)
    db["examples"].insert_all(
        [
            {"one_id": 1, "two_id": 2},
            {"one_id": 2, "two_id": 2},
        ],
        pk=("one_id", "two_id"),
    )
    potentials = potential_primary_keys(db.conn, "examples", [])
    assert potentials == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table,post_data,expected_message,expected_indexes",
    (
        (
            "museums",
            {"add_index": "1"},
            "Column name is required",
            [],
        ),
        (
            "museums",
            {"add_index": "1", "add_index_column": "name"},
            "Index added on name",
            [{"name": "idx_museums_name", "columns": ["name"], "unique": 0}],
        ),
        (
            "museums",
            {"add_index": "1", "add_index_column": "name", "add_index_unique": 1},
            "Unique index added on name",
            [{"name": "idx_museums_name", "columns": ["name"], "unique": 1}],
        ),
        (
            "museums",
            {"add_index": "1", "add_index_column": "city", "add_index_unique": 1},
            "no such column: city",
            [],
        ),
        (
            "museums",
            {"add_index": "1", "add_index_column": "city_id", "add_index_unique": 1},
            "UNIQUE constraint failed: museums.city_id",
            [],
        ),
        # Tests for removing an index
        (
            "has_indexes",
            {"drop_index_bad": "1"},
            "no such index: bad",
            [
                {"columns": ["name"], "name": "name_unique_index", "unique": 1},
                {"columns": ["name"], "name": "name_index", "unique": 0},
            ],
        ),
        (
            "has_indexes",
            {"drop_index_name_index": "1"},
            "Index dropped: name_index",
            [{"columns": ["name"], "name": "name_unique_index", "unique": 1}],
        ),
        (
            "has_indexes",
            {"drop_index_name_unique_index": "1"},
            "Index dropped: name_unique_index",
            [{"columns": ["name"], "name": "name_index", "unique": 0}],
        ),
        # Test for table with surprising characters in its name
        (
            "animal.name/with/slashes",
            {"add_index": "1", "add_index_column": "species"},
            "Index added on species",
            [
                {
                    "name": "idx_animal.name/with/slashes_species",
                    "columns": ["species"],
                    "unique": 0,
                }
            ],
        ),
    ),
)
async def test_add_remove_index(
    db_path, table, post_data, expected_message, expected_indexes
):
    ds = root_datasette([db_path])
    cookies = {"ds_actor": ds.sign({"a": {"id": "root"}}, "actor")}
    get_response = await ds.client.get(
        "/-/edit-schema/data/{}".format(tilde_encode(table)), cookies=cookies
    )
    assert get_response.status_code == 200
    csrftoken = get_response.cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    post_data["csrftoken"] = csrftoken
    response = await ds.client.post(
        "/-/edit-schema/data/{}".format(tilde_encode(table)),
        cookies=cookies,
        data=post_data,
    )
    assert response.status_code == 302
    messages = ds.unsign(response.cookies["ds_messages"], "messages")
    assert len(messages) == 1
    assert messages[0][0] == expected_message
    db = sqlite_utils.Database(db_path)
    indexes = db[table].indexes
    assert [
        {"name": index.name, "columns": index.columns, "unique": index.unique}
        for index in indexes
        if "sqlite_autoindex" not in index.name
    ] == expected_indexes


@pytest.mark.asyncio
async def test_database_and_table_level_permissions(tmp_path):
    marketing_path = str(tmp_path / "marketing.db")
    sales_path = str(tmp_path / "sales.db")
    marketing_db = sqlite_utils.Database(marketing_path)
    marketing_db["one"].insert({"id": 1}, pk="id")
    sales_db = sqlite_utils.Database(sales_path)
    sales_db["notes"].insert({"id": 1, "note": "Hello"}, pk="id")
    sales_db["not_allowed"].insert({"id": 1}, pk="id")

    ds = root_datasette(
        [marketing_path, sales_path],
        config={
            "databases": {
                "marketing": {
                    "permissions": {
                        "create-table": {"id": "pelican"},
                        "drop-table": {"id": "pelican"},
                        "alter-table": {"id": "pelican"},
                    }
                },
                "sales": {
                    "tables": {
                        "notes": {"permissions": {"alter-table": {"id": "pelican"}}}
                    }
                },
            }
        },
    )

    pelican_cookies = {"ds_actor": ds.sign({"a": {"id": "pelican"}}, "actor")}
    walrus_cookies = {"ds_actor": ds.sign({"a": {"id": "walrus"}}, "actor")}

    async def pelican_can_see(path):
        response = await ds.client.get(path, cookies=pelican_cookies)
        return response if response.status_code == 200 else None

    async def walrus_can_see(path):
        response = await ds.client.get(path, cookies=walrus_cookies)
        return response if response.status_code == 200 else None

    assert await pelican_can_see("/-/edit-schema/marketing/one")
    assert not await walrus_can_see("/-/edit-schema/marketing/one")

    # pelican cannot edit sales/not_allowed
    assert not await pelican_can_see("/-/edit-schema/sales/not_allowed")
    assert not await walrus_can_see("/-/edit-schema/sales/not_allowed")

    # pelican can edit notes - but not drop or rename it
    response = await pelican_can_see("/-/edit-schema/sales/notes")
    assert response
    assert '<input type="submit" value="Add column">' in response.text
    assert 'value="Drop this table">' not in response.text
    assert ' <input type="submit" value="Rename">' not in response.text

    # But they can drop table or rename table in marketing/one
    response2 = await pelican_can_see("/-/edit-schema/marketing/one")
    assert response2
    assert 'value="Drop this table">' in response2.text
    assert ' <input type="submit" value="Rename">' in response2.text
