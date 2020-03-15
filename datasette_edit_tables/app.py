from starlette.responses import HTMLResponse, RedirectResponse
from starlette.routing import Router, Route
from starlette.endpoints import HTTPEndpoint
from starlette.exceptions import HTTPException
from urllib.parse import quote_plus
from asgi_csrf import asgi_csrf
import sqlite_utils


def edit_tables_app(datasette):
    EditTablesIndex, EditTablesDatabase, EditTablesTable = get_classes(datasette)
    return asgi_csrf(
        Router(
            routes=[
                Route("/-/edit-tables", endpoint=EditTablesIndex),
                Route("/-/edit-tables/{database}", endpoint=EditTablesDatabase),
                Route(
                    "/-/edit-tables/{database}/{table:path}", endpoint=EditTablesTable
                ),
            ]
        )
    )


def get_classes(datasette):
    class EditTablesIndex(HTTPEndpoint):
        def get_databases(self):
            return [db for db in datasette.databases.values() if db.is_mutable]

        async def get(self, request):
            databases = self.get_databases()
            if 1 == len(databases):
                return RedirectResponse(
                    url="/-/edit-tables/{}".format(quote_plus(databases[0].name)),
                    status_code=302,
                )
            return HTMLResponse(
                await datasette.render_template(
                    "edit_tables_index.html", {"databases": databases}
                )
            )

    class EditTablesDatabase(EditTablesIndex):
        async def get(self, request):
            databases = self.get_databases()
            database_name = request.path_params["database"]
            just_these_tables = set(request.query_params.getlist("table"))
            try:
                database = [db for db in databases if db.name == database_name][0]
            except IndexError:
                raise HTTPException(status_code=404, detail="Database not found")
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
            return HTMLResponse(
                await datasette.render_template(
                    "edit_tables_database.html",
                    {
                        "database": database,
                        "tables": tables,
                        "csrftoken": request.scope.get("csrftoken", ""),
                    },
                )
            )

        async def post(self, request):
            formdata = await request.form()
            database_name = request.path_params["database"]
            columns = [
                c.split(".", 1)[1] for c in formdata.keys() if c.startswith("column.")
            ]
            table = formdata["table"]
            # TODO: Create table
            raise NotImplementedError

    class EditTablesTable(EditTablesIndex):
        async def get(self, request):
            # Starlette will have decoded %2F to / for us already thanks to :path
            table = request.path_params["table"]
            databases = self.get_databases()
            database_name = request.path_params["database"]
            try:
                database = [db for db in databases if db.name == database_name][0]
            except IndexError:
                raise HTTPException(status_code=404, detail="Database not found")

            def get_columns(conn):
                return [
                    {"name": column, "type": dtype}
                    for column, dtype in sqlite_utils.Database(conn)[
                        table
                    ].columns_dict.items()
                ]

            columns = await database.execute_write_fn(get_columns, block=True)

            return HTMLResponse(
                await datasette.render_template(
                    "edit_tables_table.html",
                    {
                        "database": database,
                        "table": table,
                        "columns": columns,
                        "csrftoken": request.scope.get("csrftoken", ""),
                    },
                )
            )

        async def post(self, request):
            table = request.path_params["table"]
            databases = self.get_databases()
            database_name = request.path_params["database"]
            try:
                database = [db for db in databases if db.name == database_name][0]
            except IndexError:
                raise HTTPException(status_code=404, detail="Database not found")

            if not await database.table_exists(table):
                return HTMLResponse("Table not found", status_code=404)

            def delete_table(conn):
                db = sqlite_utils.Database(conn)
                db[table].disable_fts()
                db[table].drop()
                db.vacuum()

            await datasette.databases[database_name].execute_write_fn(
                delete_table, block=True
            )

            return RedirectResponse(
                "/{}".format(quote_plus(database_name)), status_code=302,
            )

    return EditTablesIndex, EditTablesDatabase, EditTablesTable
