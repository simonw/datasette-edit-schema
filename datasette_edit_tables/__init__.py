from datasette import hookimpl
from datasette.utils.asgi import Response, NotFound
from urllib.parse import quote_plus
import sqlite_utils


@hookimpl
def register_routes():
    return [
        (r"^/-/edit-tables$", edit_tables_index),
        (r"^/-/edit-tables/(?P<database>[^/]+)$", edit_tables_database),
        (r"^/-/edit-tables/(?P<database>[^/]+)/(?P<table>[^/]+)$", edit_tables_table),
    ]


def get_databases(datasette):
    return [db for db in datasette.databases.values() if db.is_mutable]


async def edit_tables_index(datasette, request):
    databases = get_databases(datasette)
    if 1 == len(databases):
        return Response.redirect(
            "/-/edit-tables/{}".format(quote_plus(databases[0].name))
        )
    return Response.html(
        await datasette.render_template(
            "edit_tables_index.html", {"databases": databases}, request=request
        )
    )


async def edit_tables_database(request, datasette):
    databases = get_databases(datasette)
    database_name = request.url_vars["database"]
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
        tables.append(
            {"name": table_name, "columns": columns,}
        )
    return Response.html(
        await datasette.render_template(
            "edit_tables_database.html",
            {"database": database, "tables": tables,},
            request=request,
        )
    )


async def edit_tables_table(request, datasette):
    table = request.url_vars["table"]
    databases = get_databases(datasette)
    database_name = request.url_vars["database"]
    try:
        database = [db for db in databases if db.name == database_name][0]
    except IndexError:
        raise NotFound("Database not found")
    if not await database.table_exists(table):
        raise NotFound("Table not found")

    if request.method == "POST":
        formdata = await request.post_vars()
        if "delete_table" in formdata:
            return await delete_table(datasette, database, table)
        elif "add_column" in formdata:
            return await add_column(datasette, database, table, formdata)
        else:
            return Response.html("Unknown operation", status=400)

    def get_columns(conn):
        return [
            {"name": column, "type": dtype}
            for column, dtype in sqlite_utils.Database(conn)[table].columns_dict.items()
        ]

    columns = await database.execute_fn(get_columns)

    return Response.html(
        await datasette.render_template(
            "edit_tables_table.html",
            {"database": database, "table": table, "columns": columns,},
            request=request,
        )
    )


async def delete_table(datasette, database, table):
    def do_delete_table(conn):
        db = sqlite_utils.Database(conn)
        db[table].disable_fts()
        db[table].drop()
        db.vacuum()

    await datasette.databases[database.name].execute_write_fn(
        do_delete_table, block=True
    )

    return Response.redirect("/{}".format(quote_plus(database.name)))


async def add_column(datasette, database, table, formdata):
    name = formdata["name"]
    type = formdata["type"]

    def do_add_column(conn):
        db = sqlite_utils.Database(conn)
        db[table].add_column(name, type)

    await datasette.databases[database.name].execute_write_fn(do_add_column, block=True)

    return Response.redirect(
        "/{}/{}".format(quote_plus(database.name), quote_plus(table))
    )
