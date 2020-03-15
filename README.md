# datasette-edit-tables

[![PyPI](https://img.shields.io/pypi/v/datasette-edit-tables.svg)](https://pypi.org/project/datasette-edit-tables/)
[![CircleCI](https://circleci.com/gh/simonw/datasette-edit-tables.svg?style=svg)](https://circleci.com/gh/simonw/datasette-edit-tables)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/datasette-edit-tables/blob/master/LICENSE)

Datasette plugin for renaming, deleting and modifying tables.

## Features (planned)

* Rename a table
* Delete a table
* Modify the type of columns in a table
* Add new columns to a table
* Create a new empty table from scratch

## Installation

Install this plugin in the same environment as Datasette.

    $ pip install datasette-edit-tables

## Usage

Navigate to `/-/edit-tables/dbname/tablename` on your Datasette instance to edit a specific table.

Use `/-/edit-tables/dbname` to create a new table in a specific database.
