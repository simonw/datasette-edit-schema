from setuptools import setup
import os

VERSION = "0.4a0"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="datasette-edit-schema",
    description="Datasette plugin for modifying table schemas",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    url="https://github.com/simonw/datasette-edit-schema",
    license="Apache License, Version 2.0",
    version=VERSION,
    packages=["datasette_edit_schema"],
    entry_points={"datasette": ["edit_schema = datasette_edit_schema"]},
    install_requires=[
        "datasette>=0.51a1",
        "sqlite-utils>=2.21",
    ],
    extras_require={"test": ["pytest", "pytest-asyncio", "httpx"]},
    tests_require=["datasette-edit-schema[test]"],
    package_data={"datasette_edit_schema": ["templates/*.html", "static/*.js"]},
)
