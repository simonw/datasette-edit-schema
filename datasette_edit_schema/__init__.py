from datasette import hookimpl
from datasette.utils.asgi import Response, NotFound, Forbidden
from datasette.utils import sqlite3
from urllib.parse import quote_plus, unquote_plus
import sqlite_utils


@hookimpl
def permission_allowed(actor, action, resource):
    if (
        action == "edit-schema"
        and actor
        and actor.get("id") == "root"
        and resource != "_internal"
    ):
        return True


@hookimpl
def table_actions(datasette, actor, database, table):
    async def inner():
        if not await datasette.permission_allowed(
            actor, "edit-schema", resource=database, default=False
        ):
            return []
        return [
            {
                "href": datasette.urls.path(
                    "/-/edit-schema/{}/{}".format(database, quote_plus(table))
                ),
                "label": "Edit table schema",
            }
        ]

    return inner


@hookimpl
def register_routes():
    return [
        (r"^/-/edit-schema$", edit_schema_index),
        (r"^/-/edit-schema/(?P<database>[^/]+)$", edit_schema_database),
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
    if not await datasette.permission_allowed(
        request.actor, "edit-schema", resource=database, default=False
    ):
        raise Forbidden("Permission denied for edit-schema")


async def edit_schema_index(datasette, request):
    database_names = [db.name for db in get_databases(datasette)]
    # Check permissions for each one
    allowed_databases = [
        name
        for name in database_names
        if await datasette.permission_allowed(
            request.actor, "edit-schema", resource=name, default=False
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
            },
            request=request,
        )
    )


async def edit_schema_table(request, datasette):
    table = unquote_plus(request.url_vars["table"])
    databases = get_databases(datasette)
    database_name = request.url_vars["database"]
    await check_permissions(datasette, request, database_name)
    try:
        database = [db for db in databases if db.name == database_name][0]
    except IndexError:
        raise NotFound("Database not found")
    if not await database.table_exists(table):
        raise NotFound("Table not found")

    if request.method == "POST":
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
                sqlite_utils.Database(conn)[table].transform(
                    types=types,
                    rename=rename,
                    drop=drop,
                    column_order=[p[0] for p in order_pairs],
                )

            await database.execute_write_fn(transform_the_table, block=True)

            datasette.add_message(request, "Changes to table have been saved")

            return Response.redirect(request.path)

        if "delete_table" in formdata:
            return await delete_table(request, datasette, database, table)
        elif "add_column" in formdata:
            return await add_column(request, datasette, database, table, formdata)
        elif "rename_table" in formdata:
            return await rename_table(request, datasette, database, table, formdata)
        else:
            return Response.html("Unknown operation", status=400)

    def get_columns_and_schema(conn):
        t = sqlite_utils.Database(conn)[table]
        columns = [
            {"name": column, "type": dtype} for column, dtype in t.columns_dict.items()
        ]
        return columns, t.schema

    columns, schema = await database.execute_fn(get_columns_and_schema)

    columns_display = [
        {
            "name": c["name"],
            "type": TYPES[c["type"]],
        }
        for c in columns
    ]

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
            },
            request=request,
        )
    )


async def delete_table(request, datasette, database, table):
    def do_delete_table(conn):
        db = sqlite_utils.Database(conn)
        db[table].disable_fts()
        db[table].drop()
        db.vacuum()

    await datasette.databases[database.name].execute_write_fn(
        do_delete_table, block=True
    )
    datasette.add_message(request, "Table has been deleted")
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

    try:
        await database.execute_write(
            """
            ALTER TABLE [{}] RENAME TO [{}];
        """.format(
                table, new_name
            ),
            block=True,
        )
        datasette.add_message(
            request, "Table renamed to '{}'".format(new_name), datasette.INFO
        )
    except Exception as error:
        datasette.add_message(
            request, "Error renaming table: {}".format(str(error)), datasette.ERROR
        )
        return redirect
    return Response.redirect(
        "/-/edit-schema/{}/{}".format(quote_plus(database.name), quote_plus(new_name))
    )
