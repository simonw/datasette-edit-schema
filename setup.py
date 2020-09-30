from setuptools import setup
import os

VERSION = "0.3a"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="datasette-edit-tables",
    description="Datasette plugin for renaming and deleting tables and columns and changing column types",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    url="https://github.com/simonw/datasette-edit-tables",
    license="Apache License, Version 2.0",
    version=VERSION,
    packages=["datasette_edit_tables"],
    entry_points={"datasette": ["edit_tables = datasette_edit_tables"]},
    install_requires=[
        "datasette>=0.44",
        "sqlite-utils>=2.21",
    ],
    extras_require={"test": ["pytest", "pytest-asyncio", "httpx"]},
    tests_require=["datasette-edit-tables[test]"],
    package_data={"datasette_edit_tables": ["templates/*.html"]},
)
