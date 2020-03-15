from datasette import hookimpl
from .app import edit_tables_app


@hookimpl
def asgi_wrapper(datasette):
    def wrap_with_configure_fts(app):
        async def wrapped_app(scope, receive, send):
            path = scope["path"]
            if path == "/-/edit-tables" or path.startswith("/-/edit-tables/"):
                await (edit_tables_app(datasette))(scope, receive, send)
            else:
                await app(scope, receive, send)

        return wrapped_app

    return wrap_with_configure_fts
