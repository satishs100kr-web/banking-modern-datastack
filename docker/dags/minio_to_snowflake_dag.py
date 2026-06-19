# =============================================================================
#  minio_to_snowflake_dag.py  —  AIRFLOW DAG  (MinIO data lake --> Snowflake RAW)
#  WHAT IT DOES: every minute, Airflow runs 2 tasks:
#     task1 download_minio  : pull the Parquet files down from MinIO to local disk
#     task2 load_snowflake  : PUT them into Snowflake and COPY INTO the RAW tables
#
#  PIPELINE POSITION:  ... MinIO --> [THIS DAG] --> Snowflake RAW --> dbt ...
#  This file is a "DAG definition": Airflow imports it and shows it in the UI.
# =============================================================================

import os
import boto3                                       # to read files from MinIO (S3 API)
import snowflake.connector                         # to connect + load into Snowflake
from airflow import DAG                            # the DAG (pipeline) object
from airflow.operators.python import PythonOperator# runs a Python function as a task
from datetime import datetime, timedelta          # start date + retry delay
from dotenv import load_dotenv                     # load credentials from .env
from cryptography.hazmat.primitives import serialization  # to load the Snowflake key

load_dotenv()   # read the .env mounted next to this DAG (docker/dags/.env)

# -------- MinIO settings (where to read the lake files from) -----------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")                              # "raw"
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")# temp download folder

# -------- Snowflake settings (key-pair / JWT auth) ---------------------------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
# The ROLE matters: the default PUBLIC role can't write to the RAW tables, so we
# explicitly use the role that owns them. (Default falls back to SVC_AIRFLOW_ROLE.)
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SVC_AIRFLOW_ROLE")
# The RSA private key. May be the bare base64 body (no BEGIN/END lines) OR a full
# PEM — the loader below handles both shapes.
SNOWFLAKE_PRIVATE_KEY = os.getenv("SNOWFLAKE_PRIVATE_KEY")

TABLES = ["customers", "accounts", "transactions"]   # the 3 RAW tables to load

# =============================================================================
#  TASK 1 — download every Parquet file from MinIO to local disk.
# =============================================================================
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)            # make the temp folder if missing
    s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY)
    local_files = {}                                 # we'll return {table: [paths]}
    for table in TABLES:
        prefix = f"{table}/"                         # e.g. "customers/" — that folder in the bucket
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)  # list its files
        objects = resp.get("Contents", [])           # the file entries (empty if none)
        local_files[table] = []
        for obj in objects:
            key = obj["Key"]                         # the file's path inside the bucket
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))  # where to save it
            s3.download_file(BUCKET, key, local_file)# download it
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)
    return local_files   # returned value is stored in XCom -> task2 reads it

# =============================================================================
#  TASK 2 — load the downloaded files into Snowflake RAW tables.
# =============================================================================
def load_to_snowflake(**kwargs):
    # XCom = how Airflow passes data between tasks. We pull task1's return value.
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print("No files found in MinIO.")
        return

    # ---- Prepare the Snowflake private key --------------------------------
    # The connector's `private_key` wants DER BYTES, and the PEM needs BEGIN/END
    # framing. This block: (a) adds framing if the .env value is a bare body,
    # (b) parses the PEM, (c) converts it to DER bytes.
    import textwrap
    raw = SNOWFLAKE_PRIVATE_KEY.strip()
    if "BEGIN" in raw:                                       # already framed
        pem = raw.replace("\\n", "\n").encode()
    else:                                                   # bare base64 -> wrap it
        body = raw.replace("\\n", "").replace("\n", "").replace(" ", "")
        wrapped = "\n".join(textwrap.wrap(body, 64))        # 64-char PEM lines
        pem = ("-----BEGIN RSA PRIVATE KEY-----\n" + wrapped +
               "\n-----END RSA PRIVATE KEY-----\n").encode()
    p_key = serialization.load_pem_private_key(pem, password=None)   # parse it
    pkb = p_key.private_bytes(                               # -> DER bytes
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    # ---- Connect to Snowflake using key-pair (JWT) auth -------------------
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        authenticator="SNOWFLAKE_JWT",   # key-pair auth (no password)
        private_key=pkb,                 # the DER-encoded key from above
        role=SNOWFLAKE_ROLE,             # so we can WRITE to RAW
        warehouse=SNOWFLAKE_WAREHOUSE,   # the compute to run on
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    # ---- For each table: stage the files, then bulk-load them -------------
    for table, files in local_files.items():
        if not files:
            print(f"No files for {table}, skipping.")
            continue
        # PUT uploads each local Parquet into the table's INTERNAL STAGE (@%table).
        for f in files:
            cur.execute(f"PUT file://{f} @%{table}")
            print(f"Uploaded {f} -> @{table} stage")
        # COPY INTO bulk-loads the staged Parquet files into the table.
        # ON_ERROR='CONTINUE' skips a bad file instead of failing the whole load.
        copy_sql = f"""
        COPY INTO {table}
        FROM @%{table}
        FILE_FORMAT=(TYPE=PARQUET)
        ON_ERROR='CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {table}")

    cur.close()
    conn.close()

# =============================================================================
#  THE DAG DEFINITION — ties the 2 tasks together on a schedule.
# =============================================================================
default_args = {
    "owner": "airflow",
    "retries": 1,                          # if a task fails, retry it once
    "retry_delay": timedelta(minutes=1),   # wait 1 minute before retrying
}

with DAG(
    dag_id="minio_to_snowflake_banking",   # the name shown in the Airflow UI
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables",
    schedule_interval="*/1 * * * *",       # cron: run every 1 minute
    start_date=datetime(2025, 1, 1),       # the schedule's anchor date
    catchup=False,                         # DON'T back-run every missed minute since 2025
) as dag:

    task1 = PythonOperator(                # task 1 runs download_from_minio()
        task_id="download_minio",
        python_callable=download_from_minio,
    )
    task2 = PythonOperator(                # task 2 runs load_to_snowflake()
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,              # pass the Airflow context (so XCom works)
    )

    task1 >> task2   # DEPENDENCY: task2 only starts AFTER task1 succeeds
