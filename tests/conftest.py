from dataclasses import dataclass
from datasette import hookimpl
from datasette.app import Datasette
from datasette.permissions import PermissionSQL
from datasette.plugins import pm
import pytest
import sqlite_utils


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
    db["has_indexes"].insert(
        {
            "id": 1,
            "name": "Cleo",
            "description": "A medium sized dog",
        },
        pk="id",
    )
    db["has_indexes"].create_index(["name"], index_name="name_index")
    db["has_indexes"].create_index(
        ["name"], index_name="name_unique_index", unique=True
    )
    db["animal.name/with/slashes"].insert(
        {
            "id": 1,
            "name": "Sirocco",
            "species": 582,
        },
        pk="id",
    )
    db["table.name/with/slashes.categories"].insert(
        {
            "id": 582,
            "name": "Kākāpō",
        },
        pk="id",
    )

    return db, path


@pytest.fixture
def db_path(db_and_path):
    return db_and_path[1]


@pytest.fixture
def db(db_and_path):
    return db_and_path[0]


@pytest.fixture
def ds(db_path):
    ds = Datasette([db_path])
    ds.root_enabled = True
    return ds


@dataclass
class Rule:
    actor_id: str
    action: str
    database: str = None
    resource: str = None


@pytest.fixture
def rule():
    return Rule


@pytest.fixture
def permission_plugin():
    class PermissionPlugin:
        __name__ = "PermissionPlugin"

        # Use hookimpl and method names to register hooks
        @hookimpl
        def permission_resources_sql(self, datasette, actor, action):
            if not actor:
                return None

            actor_id = actor.get("id")
            if not actor_id:
                return None

            def build_sql(rules, *, allow, prefix):
                selects = []
                params = {}
                for idx, rule in enumerate(rules):
                    if rule.actor_id != actor_id or rule.action != action:
                        continue
                    param = f"{prefix}_{idx}"
                    selects.append(
                        "SELECT "
                        f":{param}_parent AS parent, "
                        f":{param}_child AS child, "
                        f"{1 if allow else 0} AS allow, "
                        f":{param}_reason AS reason"
                    )
                    params[f"{param}_parent"] = rule.database
                    params[f"{param}_child"] = rule.resource
                    params[f"{param}_reason"] = f"{prefix} rule"
                if not selects:
                    return None
                return PermissionSQL(
                    sql=" UNION ALL ".join(selects),
                    params=params,
                )

            results = []
            allow_sql = build_sql(
                getattr(datasette, "_rules_allow", []),
                allow=True,
                prefix="allow",
            )
            if allow_sql:
                results.append(allow_sql)
            deny_sql = build_sql(
                getattr(datasette, "_rules_deny", []),
                allow=False,
                prefix="deny",
            )
            if deny_sql:
                results.append(deny_sql)
            return results or None

    pm.register(PermissionPlugin(), name="undo_permission_plugin")
    yield
    pm.unregister(name="undo_permission_plugin")


class TrackEventPlugin:
    __name__ = "TrackEventPlugin"

    @hookimpl
    def track_event(self, datasette, event):
        datasette._tracked_events = getattr(datasette, "_tracked_events", [])
        datasette._tracked_events.append(event)


@pytest.fixture(scope="session", autouse=True)
def install_event_tracking_plugin():
    from datasette.plugins import pm

    pm.register(TrackEventPlugin(), name="TrackEventPlugin")
