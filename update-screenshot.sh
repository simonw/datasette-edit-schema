#!/bin/bash

# Grab the database
wget https://datasette.io/content.db

# Delete the triggers on the licenses table
sqlite3 content.db "DROP TRIGGER IF EXISTS licenses_ai"
sqlite3 content.db "DROP TRIGGER IF EXISTS licenses_ad"
sqlite3 content.db "DROP TRIGGER IF EXISTS licenses_au"

# Setup the root plugin
mkdir shot-plugins
cat > shot-plugins/root.py <<EOL
from datasette import hookimpl, Response

@hookimpl
def register_routes():
    def login_as_root(datasette, request):
        response = Response.redirect("/-/edit-schema/content/licenses")
        response.set_cookie(
            "ds_actor", datasette.sign({"a": {"id": "root"}}, "actor")
        )
        return response
    return (
        ("/-/root", login_as_root),
    )
EOL

# Start the server in the background and capture its PID
datasette -p 8045 content.db --plugins-dir shot-plugins &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Take the screenshot
shot-scraper http://localhost:8045/-/root -w 800 -o datasette-edit-schema.png

# Kill the server
kill $SERVER_PID

# Cleanup
rm content.db
rm -rf shot-plugins
