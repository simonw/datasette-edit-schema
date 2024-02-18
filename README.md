# datasette-edit-schema

[![PyPI](https://img.shields.io/pypi/v/datasette-edit-schema.svg)](https://pypi.org/project/datasette-edit-schema/)
[![Changelog](https://img.shields.io/github/v/release/simonw/datasette-edit-schema?include_prereleases&label=changelog)](https://github.com/simonw/datasette-edit-schema/releases)
[![Tests](https://github.com/simonw/datasette-edit-schema/workflows/Test/badge.svg)](https://github.com/simonw/datasette-edit-schema/actions?query=workflow%3ATest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/datasette-edit-schema/blob/master/LICENSE)

Datasette plugin for modifying table schemas

> :warning: The latest alpha release depends on Datasette 1.09a. Use [version 0.7.1](https://github.com/simonw/datasette-edit-schema/blob/0.7.1/README.md) with older releases of Datasette.

## Features

* Add new columns to a table
* Rename columns in a table
* Modify the type of columns in a table
* Re-order the columns in a table
* Rename a table
* Delete a table
* Change the primary key of a table to another column containing unique values
* Update the foreign key constraints on a table
* Add an index (or unique index) to a column on a table
* Drop an index from a table

## Installation

Install this plugin in the same environment as Datasette.
```bash
pip install datasette-edit-schema
```
## Usage

Navigate to `/-/edit-schema/dbname/tablename` on your Datasette instance to edit a specific table.

Use `/-/edit-schema/dbname` to create a new table in a specific database.

By default only [the root actor](https://datasette.readthedocs.io/en/stable/authentication.html#using-the-root-actor) can access the page - so you'll need to run Datasette with the `--root` option and click on the link shown in the terminal to sign in and access the page.

## Permissions

The `edit-schema` permission provides access to all functionality.

You can use permission plugins such as [datasette-permissions-sql](https://github.com/simonw/datasette-permissions-sql) to grant additional access to the write interface.

These permission checks will call the `permission_allowed()` plugin hook with three arguments:

- `action` will be the string `"edit-schema"`
- `actor` will be the currently authenticated actor - usually a dictionary
- `resource` will be the string name of the database

You can instead use more finely-grained permissions from the default Datasette permissions collection:

- `create-table` allows users to create a new table. The `resource` will be the name of the database.
- `drop-table` allows users to drop a table. The `resource` will be a tuple of `(database_name, table_name)`.
- `alter-table` allows users to alter a table. The `resource` will be a tuple of `(database_name, table_name)`.

To rename a table a user must have both `drop-table` permission for that table and `create-table` permission for that database.

For example, to configure Datasette to allow the user with ID `pelican` to create, alter and drop tables in the `marketing` database and to alter just the `notes` table in the `sales` database, you could use the following configuration:

```yaml
databases:
  marketing:
    permissions:
      create-table:
        id: pelican
      drop-table:
        id: pelican
      alter-table:
        id: pelican
  sales:
    tables:
      notes:
        permissions:
          alter-table:
            id: pelican
```

## Events

This plugin fires `create-table`, `alter-table` and `drop-table` events when tables are modified, using the [Datasette Events](https://docs.datasette.io/en/latest/events.html) system introduced in [Datasette 1.0a8](https://docs.datasette.io/en/latest/changelog.html#a8-2024-02-07).

## Screenshot

![datasette-edit-schema interface](https://raw.githubusercontent.com/simonw/datasette-edit-schema/main/datasette-edit-schema.png)

## Development

To set up this plugin locally, first checkout the code. Then create a new virtual environment:
```bash
cd datasette-edit-schema
python3 -mvenv venv
source venv/bin/activate
```
Or if you are using `pipenv`:
```bash
pipenv shell
```
Now install the dependencies and tests:
```bash
pip install -e '.[test]'
```
To run the tests:
```bash
pytest
```