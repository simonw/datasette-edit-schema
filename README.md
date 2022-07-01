# datasette-edit-schema

[![PyPI](https://img.shields.io/pypi/v/datasette-edit-schema.svg)](https://pypi.org/project/datasette-edit-schema/)
[![Changelog](https://img.shields.io/github/v/release/simonw/datasette-edit-schema?include_prereleases&label=changelog)](https://github.com/simonw/datasette-edit-schema/releases)
[![Tests](https://github.com/simonw/datasette-edit-schema/workflows/Test/badge.svg)](https://github.com/simonw/datasette-edit-schema/actions?query=workflow%3ATest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/datasette-edit-schema/blob/master/LICENSE)

Datasette plugin for modifying table schemas

## Features

* Add new columns to a table
* Rename columns in a table
* Modify the type of columns in a table
* Re-order the columns in a table
* Rename a table
* Delete a table

## Installation

Install this plugin in the same environment as Datasette.

    $ pip install datasette-edit-schema

## Usage

Navigate to `/-/edit-schema/dbname/tablename` on your Datasette instance to edit a specific table.

Use `/-/edit-schema/dbname` to create a new table in a specific database.

By default only [the root actor](https://datasette.readthedocs.io/en/stable/authentication.html#using-the-root-actor) can access the page - so you'll need to run Datasette with the `--root` option and click on the link shown in the terminal to sign in and access the page.

## Permissions

The `edit-schema` permission governs access. You can use permission plugins such as [datasette-permissions-sql](https://github.com/simonw/datasette-permissions-sql) to grant additional access to the write interface.

These permission checks will call the `permission_allowed()` plugin hook with three arguments:

- `action` will be the string `"edit-schema"`
- `actor` will be the currently authenticated actor - usually a dictionary
- `resource` will be the string name of the database

## Screenshot

![datasette-edit-schema interface](https://raw.githubusercontent.com/simonw/datasette-edit-schema/main/datasette-edit-schema.png)

## Development

To set up this plugin locally, first checkout the code. Then create a new virtual environment:

    cd datasette-edit-schema
    python3 -mvenv venv
    source venv/bin/activate

Or if you are using `pipenv`:

    pipenv shell

Now install the dependencies and tests:

    pip install -e '.[test]'

To run the tests:

    pytest
