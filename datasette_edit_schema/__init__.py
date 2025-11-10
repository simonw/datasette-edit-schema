from datasette import hookimpl
from datasette.events import CreateTableEvent, AlterTableEvent, DropTableEvent
from datasette.permissions import Action
from datasette.resources import DatabaseResource, TableResource
from datasette.utils.asgi import Response, NotFound, Forbidden
from datasette.utils import sqlite3, tilde_decode, tilde_encode
from urllib.parse import quote_plus, unquote_plus
import sqlite_utils
import textwrap
from .utils import (
    examples_for_columns,
    get_primary_keys,
    potential_foreign_keys,
    potential_primary_keys,
)

try:
    from datasette import events
except ImportError:  # Pre Datasette 1.0a8
    events = None

# Don't attempt to detect foreign keys on tables larger than this:
FOREIGN_KEY_DETECTION_LIMIT = 10_000


@hookimpl
def register_actions(datasette):
    return [
        Action(
            name="edit-schema",
            description="Edit database schemas",
            resource_class=DatabaseResource,
        )
    ]


@hookimpl
def table_actions(datasette, actor, database, table):
    async def inner():
        if not await can_alter_table(datasette, actor, database, table):
            return []
        return [
            {
                "href": datasette.urls.path(
                    "/-/edit-schema/{}/{}".format(database, tilde_encode(table))
                ),
                "label": "Edit table schema",
                "description": "Rename the table, add and remove columns...",
            }
        ]

    return inner


async def can_create_table(datasette, actor, database):
    database_resource = DatabaseResource(database)
    if await datasette.allowed(
        actor=actor,
        action="edit-schema",
        resource=database_resource,
    ):
        return True
    # Or maybe they have create-table
    if await datasette.allowed(
        actor=actor,
        action="create-table",
        resource=database_resource,
    ):
        return True
    return False


async def can_alter_table(datasette, actor, database, table):
    database_resource = DatabaseResource(database)
    if await datasette.allowed(
        actor=actor,
        action="edit-schema",
        resource=database_resource,
    ):
        return True
    table_resource = TableResource(database, table)
    if await datasette.allowed(
        actor=actor,
        action="alter-table",
        resource=table_resource,
    ):
        return True
    return False


async def can_rename_table(datasette, actor, database, table):
    if not await can_drop_table(datasette, actor, database, table):
        return False
    if not await can_create_table(datasette, actor, database):
        return False
    return True


async def can_drop_table(datasette, actor, database, table):
    database_resource = DatabaseResource(database)
    if await datasette.allowed(
        actor=actor,
        action="edit-schema",
        resource=database_resource,
    ):
        return True
    # Or maybe they have drop-table
    table_resource = TableResource(database, table)
    if await datasette.allowed(
        actor=actor,
        action="drop-table",
        resource=table_resource,
    ):
        return True
    return False


@hookimpl
def database_actions(datasette, actor, database):
    async def inner():
        if not await can_create_table(datasette, actor, database):
            return []
        return [
            {
                "href": datasette.urls.path(
                    "/-/edit-schema/{}/-/create".format(database)
                ),
                "label": "Create a table",
                "description": "Define a new table with specified columns",
            }
        ]

    return inner


@hookimpl
def register_routes():
    return [
        (r"^/-/edit-schema$", edit_schema_index),
        (r"^/-/edit-schema/(?P<database>[^/]+)$", edit_schema_database),
        (r"^/-/edit-schema/(?P<database>[^/]+)/-/create$", edit_schema_create_table),
        (r"^/-/edit-schema/(?P<database>[^/]+)/(?P<table>[^/]+)$", edit_schema_table),
    ]


TYPES = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
    bytes: "BLOB",
}
REV_TYPES = {v: k for k, v in TYPES.items()}
TYPE_NAMES = {
    "TEXT": "Text",
    "REAL": "Floating point",
    "INTEGER": "Integer",
    "BLOB": "Binary data",
}


def get_databases(datasette):
    return [
        db
        for db in datasette.databases.values()
        if db.is_mutable and db.name != "_internal"
    ]


async def check_permissions(datasette, request, database):
    if not await datasette.allowed(
        actor=request.actor,
        action="edit-schema",
        resource=DatabaseResource(database),
    ):
        raise Forbidden("Permission denied for edit-schema")


async def edit_schema_index(datasette, request):
    database_names = [db.name for db in get_databases(datasette)]
    # Check permissions for each one
    allowed_databases = [
        name
        for name in database_names
        if await datasette.allowed(
            actor=request.actor,
            action="edit-schema",
            resource=DatabaseResource(name),
        )
    ]
    if not allowed_databases:
        raise Forbidden("Permission denied for edit-schema")

    if len(allowed_databases) == 1:
        return Response.redirect(
            "/-/edit-schema/{}".format(quote_plus(allowed_databases[0]))
        )

    return Response.html(
        await datasette.render_template(
            "edit_schema_index.html",
            {
                "databases": allowed_databases,
            },
            request=request,
        )
    )


async def edit_schema_database(request, datasette):
    databases = get_databases(datasette)
    database_name = request.url_vars["database"]
    await check_permissions(datasette, request, database_name)
    just_these_tables = set(request.args.getlist("table"))
    try:
        database = [db for db in databases if db.name == database_name][0]
    except IndexError:
        raise NotFound("Database not found")
    tables = []
    hidden_tables = set(await database.hidden_table_names())
    for table_name in await database.table_names():
        if just_these_tables and table_name not in just_these_tables:
            continue
        if table_name in hidden_tables:
            continue

        def get_columns(conn):
            return [
                {"name": column, "type": dtype}
                for column, dtype in sqlite_utils.Database(conn)[
                    table_name
                ].columns_dict.items()
            ]

        columns = await database.execute_write_fn(get_columns, block=True)
        tables.append({"name": table_name, "columns": columns})
    return Response.html(
        await datasette.render_template(
            "edit_schema_database.html",
            {
                "database": database,
                "tables": tables,
                "tilde_encode": tilde_encode,
            },
            request=request,
        )
    )


async def edit_schema_create_table(request, datasette):
    database_name = request.url_vars["database"]
    if not await can_create_table(datasette, request.actor, database_name):
        raise Forbidden("Permission denied for create-table")
    try:
        db = datasette.get_database(database_name)
    except KeyError:
        raise NotFound("Database not found")

    if request.method == "POST":
        formdata = await request.post_vars()
        table_name = formdata.get("table_name") or ""
        columns = {}
        for key, value in formdata.items():
            if key.startswith("column-name"):
                idx = key.split(".")[-1]
                columns[idx] = {"name": value}
            elif key.startswith("column-type"):
                idx = key.split(".")[-1]
                columns[idx]["type"] = value
            elif key.startswith("column-sort"):
                idx = key.split(".")[-1]
                columns[idx]["sort"] = int(value)

        # Sort columns based on sort order
        sorted_columns = sorted(columns.values(), key=lambda x: x["sort"])

        # Dictionary to use with .create()
        primary_key_name = formdata["primary_key_name"].strip()
        create = {primary_key_name: REV_TYPES[formdata["primary_key_type"]]}

        for column in sorted_columns:
            if column["name"].strip():
                create[column["name"].strip()] = REV_TYPES[column["type"]]

        def create_the_table(conn):
            db = sqlite_utils.Database(conn)
            if not table_name.strip():
                return None, "Table name is required"
            if db[table_name].exists():
                return None, "Table already exists"
            try:
                db[table_name].create(
                    create, pk=primary_key_name, not_null=(primary_key_name,)
                )
                return db[table_name].schema, None
            except Exception as e:
                return None, str(e)

        schema, error = await db.execute_write_fn(create_the_table, block=True)

        if error:
            datasette.add_message(request, str(error), datasette.ERROR)
            path = request.path
        else:
            datasette.add_message(request, "Table has been created")
            path = datasette.urls.table(database_name, table_name)
            await datasette.track_event(
                CreateTableEvent(
                    actor=request.actor,
                    database=database_name,
                    table=table_name,
                    schema=schema,
                )
            )

        return Response.redirect(path)

    return Response.html(
        await datasette.render_template(
            "edit_schema_create_table.html",
            {
                "database": db,
                "columns": [{"name": "Column {}".format(i)} for i in range(1, 10)],
                "types": [
                    {"name": TYPE_NAMES[value], "value": value}
                    for value in TYPES.values()
                ],
            },
            request=request,
        )
    )


async def edit_schema_table(request, datasette):
    table = tilde_decode(request.url_vars["table"])
    databases = get_databases(datasette)
    database_name = request.url_vars["database"]

    if not await can_alter_table(datasette, request.actor, database_name, table):
        raise Forbidden("Permission denied for alter-table")

    try:
        database = [db for db in databases if db.name == database_name][0]
    except IndexError:
        raise NotFound("Database not found")
    if not await database.table_exists(table):
        raise NotFound("Table not found")

    if request.method == "POST":

        def get_schema(conn):
            table_obj = sqlite_utils.Database(conn)[table]
            if not table_obj.exists():
                return None
            return table_obj.schema

        before_schema = await database.execute_fn(get_schema)

        async def track_analytics():
            after_schema = await database.execute_fn(get_schema)
            # Don't track drop tables, which happen when after_schema is None
            if after_schema is not None and after_schema != before_schema:
                await datasette.track_event(
                    AlterTableEvent(
                        actor=request.actor,
                        database=database_name,
                        table=table,
                        before_schema=before_schema,
                        after_schema=after_schema,
                    )
                )

        formdata = await request.post_vars()
        if formdata.get("action") == "update_columns":
            types = {}
            rename = {}
            drop = set()
            order_pairs = []

            def get_columns(conn):
                return [
                    {"name": column, "type": dtype}
                    for column, dtype in sqlite_utils.Database(conn)[
                        table
                    ].columns_dict.items()
                ]

            existing_columns = await database.execute_fn(get_columns)

            for column_details in existing_columns:
                column = column_details["name"]
                new_name = formdata.get("name.{}".format(column))
                if new_name and new_name != column:
                    rename[column] = new_name
                if formdata.get("delete.{}".format(column)):
                    drop.add(column)
                types[column] = (
                    REV_TYPES.get(formdata.get("type.{}".format(column)))
                    or column_details["type"]
                )
                order_pairs.append((column, formdata.get("sort.{}".format(column), 0)))

            order_pairs.sort(key=lambda p: int(p[1]))

            def transform_the_table(conn):
                # Run this in a transaction:
                with conn:
                    # We have to read all the views first, because we need to drop and recreate them
                    db = sqlite_utils.Database(conn)
                    views = {
                        v.name: v.schema for v in db.views if table.lower() in v.schema
                    }
                    for view in views.keys():
                        db[view].drop()
                    db[table].transform(
                        types=types,
                        rename=rename,
                        drop=drop,
                        column_order=[p[0] for p in order_pairs],
                    )
                    # Now recreate the views
                    for schema in views.values():
                        db.execute(schema)

            await database.execute_write_fn(transform_the_table, block=True)

            datasette.add_message(request, "Changes to table have been saved")
            await track_analytics()
            return Response.redirect(request.path)

        if formdata.get("action") == "update_foreign_keys":
            response = await update_foreign_keys(
                request, datasette, database, table, formdata
            )
        elif formdata.get("action") == "update_primary_key":
            response = await update_primary_key(
                request, datasette, database, table, formdata
            )
        elif "drop_table" in formdata:
            response = await drop_table(request, datasette, database, table)
        elif "add_column" in formdata:
            response = await add_column(request, datasette, database, table, formdata)
        elif "rename_table" in formdata:
            response = await rename_table(request, datasette, database, table, formdata)
        elif "add_index" in formdata:
            column = formdata.get("add_index_column") or ""
            unique = formdata.get("add_index_unique")
            response = await add_index(
                request, datasette, database, table, column, unique
            )
        elif any(key.startswith("drop_index_") for key in formdata.keys()):
            response = await drop_index(request, datasette, database, table, formdata)
        else:
            response = Response.html("Unknown operation", status=400)
        await track_analytics()
        return response

    def get_columns_and_schema_and_fks_and_pks_and_indexes(conn):
        db = sqlite_utils.Database(conn)
        t = db[table]
        pks = set(t.pks)
        columns = [
            {"name": column, "type": dtype, "is_pk": column in pks}
            for column, dtype in t.columns_dict.items()
        ]
        # Include the index declarations in the schema as well
        schema = db.execute(
            textwrap.dedent(
                """
        select group_concat(sql, ';
        ') from sqlite_master where tbl_name = ?
        order by type desc
        """
            ),
            [table],
        ).fetchone()[0]
        return columns, schema, t.foreign_keys, t.pks, t.indexes

    columns, schema, foreign_keys, pks, indexes = await database.execute_fn(
        get_columns_and_schema_and_fks_and_pks_and_indexes
    )
    foreign_keys_by_column = {}
    for fk in foreign_keys:
        foreign_keys_by_column.setdefault(fk.column, []).append(fk)

    # Load example data for the columns - truncated first five non-blank values
    column_examples = await database.execute_fn(
        lambda conn: examples_for_columns(conn, table)
    )

    columns_display = [
        {
            "name": c["name"],
            "type": TYPES[c["type"]],
            "examples": column_examples.get(c["name"]) or {},
        }
        for c in columns
    ]

    # To detect potential foreign keys we need (table, column) for the
    # primary keys on every other table
    other_primary_keys = [
        pair for pair in await database.execute_fn(get_primary_keys) if pair[0] != table
    ]
    integer_primary_keys = [
        (pair[0], pair[1]) for pair in other_primary_keys if pair[2] is int
    ]
    string_primary_keys = [
        (pair[0], pair[1]) for pair in other_primary_keys if pair[2] is str
    ]

    all_columns_to_manage_foreign_keys = [
        {
            "name": column["name"],
            "foreign_key": (
                foreign_keys_by_column.get(column["name"])[0]
                if foreign_keys_by_column.get(column["name"])
                else None
            ),
            "suggestions": [],
            "options": (
                integer_primary_keys if column["type"] is int else string_primary_keys
            ),
        }
        for column in columns
    ]

    # Anything not a float or an existing PK could be the next PK, but
    # for smaller tables we cut those down to just unique columns
    potential_pks = [
        c["name"] for c in columns if c["type"] is not float and not c["is_pk"]
    ]
    potential_fks = []
    # Only scan for potential foreign keys if there are less than 10,000
    # rows - since execute_fn() does not yet support time limits
    limited_count = (
        await database.execute(
            'select count(*) from (select 1 from "{}" limit {})'.format(
                table, FOREIGN_KEY_DETECTION_LIMIT
            )
        )
    ).single_value()
    if limited_count and limited_count < FOREIGN_KEY_DETECTION_LIMIT:
        potential_fks = await database.execute_fn(
            lambda conn: potential_foreign_keys(
                conn,
                table,
                [c["name"] for c in columns if not c["is_pk"]],
                other_primary_keys,
            )
        )
        for info in all_columns_to_manage_foreign_keys:
            info["suggestions"] = potential_fks.get(info["name"], [])
        # Now do potential primary keys against non-float columns
        non_float_columns = [
            c["name"] for c in columns if c["type"] is not float and not c["is_pk"]
        ]
        potential_pks = await database.execute_fn(
            lambda conn: potential_primary_keys(conn, table, non_float_columns)
        )

    # Add 'options' to those
    for info in all_columns_to_manage_foreign_keys:
        options = []
        seen = set()
        info["html_options"] = options
        # Reshuffle so suggestions are at the top
        if info["foreign_key"]:
            options.append(
                {
                    "name": "{}.{} (current)".format(
                        info["foreign_key"].other_table,
                        info["foreign_key"].other_column,
                    ),
                    "value": "{}.{}".format(
                        tilde_encode(info["foreign_key"].other_table),
                        tilde_encode(info["foreign_key"].other_column),
                    ),
                    "selected": True,
                }
            )
            seen.add(
                (info["foreign_key"].other_table, info["foreign_key"].other_column)
            )
        # Now add suggestions
        for suggested_table, suggested_column in info["suggestions"]:
            if not (
                info["foreign_key"]
                and info["foreign_key"].other_column == suggested_column
            ):
                options.append(
                    {
                        "name": "{}.{} (suggested)".format(
                            suggested_table, suggested_column
                        ),
                        "value": "{}.{}".format(
                            tilde_encode(suggested_table),
                            tilde_encode(suggested_column),
                        ),
                        "selected": False,
                    }
                )
                seen.add((suggested_table, suggested_column))
                info["suggested"] = "{}.{}".format(suggested_table, suggested_column)
        # And the rest
        for rest_table, rest_column in info["options"]:
            if (rest_table, rest_column) not in seen:
                options.append(
                    {
                        "name": "{}.{}".format(rest_table, rest_column),
                        "value": "{}.{}".format(
                            tilde_encode(rest_table), tilde_encode(rest_column)
                        ),
                        "selected": False,
                    }
                )

    # Don't let users drop sqlite_autoindex_* indexes
    existing_indexes = [
        index for index in indexes if not index.name.startswith("sqlite_autoindex_")
    ]
    # Only allow index creation on non-primary-key columns
    non_primary_key_columns = [c for c in columns if not c["is_pk"]]

    return Response.html(
        await datasette.render_template(
            "edit_schema_table.html",
            {
                "database": database,
                "table": table,
                "columns": columns_display,
                "schema": schema,
                "types": [
                    {"name": TYPE_NAMES[value], "value": value}
                    for value in TYPES.values()
                ],
                "foreign_keys": foreign_keys,
                "all_columns_to_manage_foreign_keys": all_columns_to_manage_foreign_keys,
                "potential_pks": potential_pks,
                "is_rowid_table": bool(pks == ["rowid"]),
                "current_pk": pks[0] if len(pks) == 1 else None,
                "existing_indexes": existing_indexes,
                "non_primary_key_columns": non_primary_key_columns,
                "can_drop_table": await can_drop_table(
                    datasette, request.actor, database_name, table
                ),
                "can_rename_table": await can_rename_table(
                    datasette, request.actor, database_name, table
                ),
                "tilde_encode": tilde_encode,
            },
            request=request,
        )
    )


async def drop_table(request, datasette, database, table):
    if not await can_drop_table(datasette, request.actor, database.name, table):
        raise Forbidden("Permission denied for drop-table")

    def do_drop_table(conn):
        db = sqlite_utils.Database(conn)
        db[table].disable_fts()
        db[table].drop()
        db.vacuum()

    if hasattr(database, "execute_isolated_fn"):
        await database.execute_isolated_fn(do_drop_table)
        # For the tests
        datasette._datasette_edit_schema_used_execute_isolated_fn = True
    else:
        await database.execute_write_fn(do_drop_table)

    datasette.add_message(request, "Table has been deleted")
    await datasette.track_event(
        DropTableEvent(
            actor=request.actor,
            database=database.name,
            table=table,
        )
    )
    return Response.redirect("/-/edit-schema/" + database.name)


async def add_column(request, datasette, database, table, formdata):
    name = formdata["name"]
    type = formdata["type"]

    redirect = Response.redirect(
        "/-/edit-schema/{}/{}".format(quote_plus(database.name), quote_plus(table))
    )

    if not name:
        datasette.add_message(request, "Column name is required", datasette.ERROR)
        return redirect

    if type.upper() not in REV_TYPES:
        datasette.add_message(request, "Invalid type: {}".format(type), datasette.ERROR)
        return redirect

    def do_add_column(conn):
        db = sqlite_utils.Database(conn)
        db[table].add_column(name, REV_TYPES[type.upper()])

    error = None
    try:
        await datasette.databases[database.name].execute_write_fn(
            do_add_column, block=True
        )
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            error = "A column called '{}' already exists".format(name)
        else:
            error = str(e)

    if error:
        datasette.add_message(request, error, datasette.ERROR)
    else:
        datasette.add_message(request, "Column has been added")
    return redirect


async def rename_table(request, datasette, database, table, formdata):
    new_name = formdata.get("name", "").strip()
    redirect = Response.redirect(
        "/-/edit-schema/{}/{}".format(quote_plus(database.name), quote_plus(table))
    )
    if not new_name:
        datasette.add_message(request, "New table name is required", datasette.ERROR)
        return redirect
    if new_name == table:
        datasette.add_message(request, "Table name was the same", datasette.WARNING)
        return redirect

    existing_tables = await database.table_names()
    if new_name in existing_tables:
        datasette.add_message(
            request,
            "A table called '{}' already exists".format(new_name),
            datasette.ERROR,
        )
        return redirect

    # User must have drop-table permission on old table and create-table on new table
    if not await can_rename_table(datasette, request.actor, database.name, table):
        datasette.add_message(
            request,
            "Permission denied to rename table '{}'".format(table),
            datasette.ERROR,
        )
        return redirect

    try:
        before_schema = await database.execute_fn(
            lambda conn: sqlite_utils.Database(conn)[table].schema
        )
        await database.execute_write(
            """
            ALTER TABLE [{}] RENAME TO [{}];
        """.format(
                table, new_name
            ),
            block=True,
        )
        after_schema = await database.execute_fn(
            lambda conn: sqlite_utils.Database(conn)[new_name].schema
        )
        datasette.add_message(
            request, "Table renamed to '{}'".format(new_name), datasette.INFO
        )
        await datasette.track_event(
            AlterTableEvent(
                actor=request.actor,
                database=database.name,
                table=new_name,
                before_schema=before_schema,
                after_schema=after_schema,
            )
        )

    except Exception as error:
        datasette.add_message(
            request, "Error renaming table: {}".format(str(error)), datasette.ERROR
        )
        return redirect
    return Response.redirect(
        "/-/edit-schema/{}/{}".format(quote_plus(database.name), quote_plus(new_name))
    )


async def update_foreign_keys(request, datasette, database, table, formdata):
    new_fks = {
        key[3:]: value
        for key, value in formdata.items()
        if key.startswith("fk.") and value.strip()
    }
    existing_fks = {
        fk.column: fk.other_table + "." + fk.other_column
        for fk in await database.execute_fn(
            lambda conn: sqlite_utils.Database(conn)[table].foreign_keys
        )
    }
    if new_fks == existing_fks:
        datasette.add_message(request, "No changes to foreign keys", datasette.WARNING)
        return Response.redirect(request.path)

    # Need that in (column, other_table, other_column) format
    fks = []
    for column, other_table_and_column in new_fks.items():
        split = other_table_and_column.split(".")
        fks.append(
            (
                column,
                tilde_decode(split[0]),
                tilde_decode(split[1]),
            )
        )

    # Update foreign keys
    def run(conn):
        db = sqlite_utils.Database(conn)
        with conn:
            db[table].transform(foreign_keys=fks)

    await database.execute_write_fn(run, block=True)
    summary = ", ".join("{} → {}.{}".format(*fk) for fk in fks)
    if summary:
        message = "Foreign keys updated{}".format(
            " to {}".format(summary) if summary else ""
        )
    else:
        message = "Foreign keys removed"
    datasette.add_message(
        request,
        message,
    )
    return Response.redirect(request.path)


async def update_primary_key(request, datasette, database, table, formdata):
    primary_key = formdata["primary_key"]
    if not primary_key:
        datasette.add_message(request, "Primary key is required", datasette.ERROR)
        return Response.redirect(request.path)

    def run(conn):
        db = sqlite_utils.Database(conn)
        with conn:
            if primary_key not in db[table].columns_dict:
                return "Column '{}' does not exist".format(primary_key)
            # Make sure it's unique
            sql = 'select count(*) - count(distinct("{}")) from "{}"'.format(
                primary_key, table
            )
            should_be_zero = db.execute(sql).fetchone()[0]
            if should_be_zero:
                return "Column '{}' is not unique".format(primary_key)
            db[table].transform(pk=primary_key)
            return None

    error = await database.execute_write_fn(run, block=True)
    if error:
        datasette.add_message(request, error, datasette.ERROR)
    else:
        datasette.add_message(
            request,
            "Primary key for '{}' is now '{}'".format(
                table,
                formdata["primary_key"],
            ),
        )
    return Response.redirect(request.path)


async def add_index(request, datasette, database, table, column, unique):
    if not column:
        datasette.add_message(request, "Column name is required", datasette.ERROR)
        return Response.redirect(request.path)

    def run(conn):
        db = sqlite_utils.Database(conn)
        with conn:
            db[table].create_index([column], find_unique_name=True, unique=unique)

    try:
        await database.execute_write_fn(run, block=True)
        message = "Index added on "
        if unique:
            message = "Unique index added on "
        message += column
        datasette.add_message(request, message)
    except Exception as e:
        datasette.add_message(request, str(e), datasette.ERROR)
    return Response.redirect(request.path)


async def drop_index(request, datasette, database, table, formdata):
    to_drops = [
        key[len("drop_index_") :]
        for key in formdata.keys()
        if key.startswith("drop_index_")
    ]
    if to_drops:
        to_drop = to_drops[0]

        def run(conn):
            with conn:
                conn.execute("DROP INDEX [{}]".format(to_drop))

        try:
            await database.execute_write_fn(run, block=True)
            datasette.add_message(request, "Index dropped: {}".format(to_drop))
        except Exception as e:
            datasette.add_message(request, str(e), datasette.ERROR)
    else:
        datasette.add_message(request, "No index name provided", datasette.ERROR)
    return Response.redirect(request.path)
