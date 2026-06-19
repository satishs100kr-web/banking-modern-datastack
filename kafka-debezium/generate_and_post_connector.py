# =============================================================================
#  generate_and_post_connector.py
#  WHAT THIS FILE DOES (in one line):
#     It does NOT move data. It "registers" a Debezium connector by sending a
#     JSON config to Kafka Connect's REST API. After this runs, Debezium starts
#     watching Postgres and streaming every change into Kafka.
#
#  WHERE IT SITS IN THE PIPELINE:
#     Postgres --> [Debezium/Kafka Connect] --> Kafka topics --> consumer --> ...
#                          ^ this script turns that arrow ON
# =============================================================================

import os          # read environment variables (os.getenv) — to keep secrets out of the code
import json        # turn a Python dict into a JSON text string (json.dumps)
import requests    # send an HTTP POST request to the Kafka Connect REST API
from dotenv import load_dotenv   # load values from the .env file into the environment

# -----------------------------------------------------------------------------
# Load environment variables.
# load_dotenv() reads the .env file sitting NEXT TO this script (kafka-debezium/.env)
# and puts POSTGRES_HOST, POSTGRES_USER, etc. into the environment so os.getenv()
# can read them below. Why? So we never hard-code passwords inside the code.
# -----------------------------------------------------------------------------
load_dotenv()

# -----------------------------------------------------------------------------
# Build the connector configuration as a normal Python dictionary.
# Kafka Connect expects this exact shape: a "name" + a "config" object.
# Every key below is a Debezium setting. (Full explanation of each one is in
# docs/PROJECT_GUIDE.md section 28.)
# -----------------------------------------------------------------------------
connector_config = {
    "name": "postgres-connector",   # the connector's unique id (used to check status / delete it)
    "config": {
        # WHICH Debezium plugin to load. This is a Java class path:
        # package "io.debezium.connector.postgresql" + class "PostgresConnector".
        # It tells the generic Kafka Connect runtime: "do POSTGRES change-capture".
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # HOW to reach the source database. These come from .env (no hard-coding).
        # IMPORTANT: hostname is the docker SERVICE NAME "postgres" (set in .env),
        # NOT "localhost" — because Debezium runs inside Docker and reaches the DB
        # over the docker network by its service name.
        "database.hostname": os.getenv("POSTGRES_HOST"),   # e.g. "postgres"
        "database.port": os.getenv("POSTGRES_PORT"),       # e.g. "5432"
        "database.user": os.getenv("POSTGRES_USER"),       # needs REPLICATION rights to read the WAL
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),        # e.g. "banking"

        # NAMING of the Kafka topics Debezium will create. With prefix "banking_server",
        # the customers table streams into topic "banking_server.public.customers", etc.
        "topic.prefix": "banking_server",

        # WHICH tables to capture. We list only our 3 tables, so Debezium ignores
        # everything else (less noise, less load).
        "table.include.list": "public.customers,public.accounts,public.transactions",

        # HOW Postgres turns its internal change-log (WAL) into readable events.
        # "pgoutput" is built into Postgres 10+, so nothing extra to install.
        # (Requires the DB to run with wal_level=logical — set in docker-compose.)
        "plugin.name": "pgoutput",

        # The REPLICATION SLOT name = Postgres' bookmark of "how far Debezium has read".
        # This is what makes restarts safe: Debezium resumes exactly where it stopped.
        # (Gotcha: an unused slot keeps WAL forever and can fill the disk.)
        "slot.name": "banking_slot",

        # How Debezium creates the Postgres "publication" (the official list of tables
        # whose changes are streamed). "filtered" = publish ONLY the included tables.
        "publication.autocreate.mode": "filtered",

        # After a DELETE, Kafka can also emit a second "null" message (a tombstone)
        # used for log compaction. We set false to keep the stream simple for our consumer.
        "tombstones.on.delete": "false",

        # How NUMERIC/DECIMAL columns (money: amount, balance) are encoded.
        # "double" = send them as plain numbers. (The default would send base64 bytes,
        # which are unreadable — so this keeps amounts human-readable downstream.)
        "decimal.handling.mode": "double",
    },
}

# -----------------------------------------------------------------------------
# Send the config to Kafka Connect.
# Kafka Connect's REST API lives at port 8083. POSTing to /connectors with this
# JSON tells it "create and start this connector".
# -----------------------------------------------------------------------------
url = "http://localhost:8083/connectors"          # the Kafka Connect REST endpoint
headers = {"Content-Type": "application/json"}     # tell the server we're sending JSON

# requests.post(...) actually sends the HTTP request.
# json.dumps(connector_config) converts our Python dict into a JSON text string.
response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# -----------------------------------------------------------------------------
# Report the result by reading the HTTP status code the server sent back:
#   201 = Created  -> the connector was registered and started (success)
#   409 = Conflict -> a connector with this name already exists (also fine)
#   anything else  -> something went wrong; print the body so we can debug
# -----------------------------------------------------------------------------
if response.status_code == 201:
    print("✅ Connector created successfully!")
elif response.status_code == 409:
    print("⚠️ Connector already exists.")
else:
    print(f"❌ Failed to create connector ({response.status_code}): {response.text}")
