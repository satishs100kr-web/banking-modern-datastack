# 🏦 Banking Modern Data Stack — The Complete Guide

> **One place for everything.** Architecture, every folder, every line of code explained,
> the concepts behind it, the commands we ran, the bugs we fixed, and interview questions.
> Written for a complete beginner aiming to become a **Senior Data Engineer**.

**Pipeline:** `Postgres → Debezium/Kafka (CDC) → MinIO (lake) → Airflow → Snowflake → dbt → Power BI`

📄 *Companion file:* [`COMMANDS_JOURNEY.md`](COMMANDS_JOURNEY.md) — the pure command/troubleshooting log.
🌐 *Interactive view:* open [`index.html`](index.html) in a browser.

---

## 📑 Contents
1. [What & Why](#what)
2. [Architecture & Data Flow](#arch)
3. [Tech Stack — and why each tool](#stack)
4. [Folder-by-Folder](#folders)
5. [Stage 1–2 · Source (Postgres + Faker)](#source)
6. [Stage 3–4 · CDC (Debezium + Kafka + Consumer)](#cdc)
7. [Stage 5 · Airflow (the DAGs)](#airflow)
8. [Stage 5 · dbt (staging → snapshots → marts)](#dbt)
9. [Stage 6 · Snowflake objects](#snowflake)
10. [Infrastructure & CI/CD](#infra)
11. [Concepts Glossary](#glossary)
12. [Interview Questions](#interview)
13. [Top 10 Lessons](#lessons)

---

<a name="what"></a>
## 1. What & Why

A bank has two opposite needs:

| Need | Example | System | Tech |
|---|---|---|---|
| **Run the bank** (fast writes) | "record this transfer NOW" | **OLTP** (Online Transaction Processing) | PostgreSQL |
| **Understand the bank** (heavy reads) | "total spend per customer this month" | **OLAP** (Online Analytics Processing) | Snowflake |

You **cannot** run heavy analytics on the live transactional DB — it would slow real banking. So this project builds a **pipeline** that continuously copies changes from OLTP → OLAP, **cleans + models** them into a star schema **with history (SCD2)**, and serves them to dashboards — **automatically and in near real-time**.

> **Analogy:** OLTP is the **busy kitchen**; OLAP is the **manager's office**. You don't do paperwork in the kitchen — you copy receipts to the office. This pipeline is the **conveyor belt** between them.

---

<a name="arch"></a>
## 2. Architecture & Data Flow

```
 data-geneator/faker_generator.py        ← generates fake banking data
        │  INSERT
        ▼
 PostgreSQL  (banking, OLTP, wal_level=logical)
        │  Debezium captures every INSERT/UPDATE/DELETE  (CDC)
        ▼
 Kafka topics  banking_server.public.{customers,accounts,transactions}
        │  consumer/kafka_to_minio.py  (batches 50 → Parquet)
        ▼
 MinIO  (S3-compatible "raw" bucket)        ← the data lake / landing zone
        │  Airflow DAG: minio_to_snowflake_dag.py  (PUT + COPY INTO)
        ▼
 Snowflake  BANKING.RAW.v  (variant column) ← bronze tier (OLAP)
        │  dbt: banking_dbt/
        ▼
 staging (views) → snapshots (SCD2) → dim_/fact_ (star schema)  in BANKING.ANALYTICS
        │  direct query
        ▼
 Power BI dashboard
```
Cross-cutting: **Airflow** orchestrates recurring jobs; **CI/CD** (`.github/workflows`) tests & deploys on push.

**The 6 stages:**

| # | Stage | Does | Tool |
|---|---|---|---|
| 1 | Generate | create fake data | Python + Faker |
| 2 | Store (OLTP) | structured, ACID storage | PostgreSQL |
| 3 | Stream (CDC) | capture & stream changes live | Debezium + Kafka |
| 4 | Land (lake) | save as Parquet | MinIO (S3) |
| 5 | Load + Transform | files → warehouse → star schema + history | Airflow + Snowflake + dbt |
| 6 | Visualize | dashboards | Power BI |

---

<a name="stack"></a>
## 3. Tech Stack — and why each tool

| Tool | Role | Why chosen (interview-ready) |
|---|---|---|
| **PostgreSQL** | source OLTP DB | Structure + **ACID** (Atomicity, Consistency, Isolation, Durability). Money can't be "eventually consistent." |
| **Debezium** | CDC | Reads the **write-ahead log (WAL)** — captures changes with **zero load** on the live DB. |
| **Kafka** | streaming buffer | Decouples producers/consumers; durable; survives restarts via offsets. |
| **MinIO** | data lake | Free, local, **S3-compatible** API → cloud-portable code. |
| **Snowflake** | warehouse (OLAP) | **Separates storage from compute**; scales analytics independently. |
| **dbt** | transformations | SQL as version-controlled, tested, documented models; built-in **SCD2** snapshots. |
| **Airflow** | orchestration | Schedules + retries + monitors recurring jobs. |
| **Parquet** | file format | **Columnar**, typed, compressed → ideal for analytics (vs CSV/JSON). |
| **Docker** | packaging | Every tool in a container; one `docker compose up`. |
| **Power BI** | BI | **Direct Query** = live dashboards that change with the data. |

---

<a name="folders"></a>
## 4. Folder-by-Folder

| Path | Purpose | Critical? |
|---|---|---|
| `data-geneator/faker_generator.py` | generate + insert fake data into Postgres | ✅ |
| `postgres/schema.sql` | DDL: create the 3 source tables (PK/FK/constraints) | ✅ |
| `kafka-debezium/generate_and_post_connector.py` | register the Debezium CDC connector via REST | ✅ |
| `consumer/kafka_to_minio.py` | Kafka → batch → Parquet → MinIO | ✅ |
| `docker/dags/minio_to_snowflake_dag.py` | Airflow DAG: MinIO → Snowflake RAW | ✅ |
| `docker/dags/scd_snapshots.py` | Airflow DAG: daily `dbt snapshot` + `dbt run --select marts` | ✅ |
| `banking_dbt/models/staging/` | parse the `variant` column, dedup (views) | ✅ |
| `banking_dbt/snapshots/` | SCD2 history tables | ✅ |
| `banking_dbt/models/marts/` | `dim_customers`, `dim_accounts`, `fact_transactions` | ✅ |
| `banking_dbt/models/sources.yml` | declare where RAW tables live | ✅ |
| `banking_dbt/dbt_project.yml` | dbt project config | ✅ |
| `docker-compose.yml` | defines every container | ✅ |
| `dockerfile-airflow.dockerfile` | custom Airflow image w/ `dbt-snowflake` | ✅ |
| `requirements.txt` | Python dependencies | ✅ |
| `.github/workflows/{ci,cd}.yml` | CI tests + CD deploy | ⚠️ important |
| `.env` (×5) + `keys/rsa_key.p8` | secrets — **never committed** | ✅ security |
| `.gitignore` | keep secrets + data dirs out of git | ✅ |

> **Quirks in this repo:** folder is misspelled `data-geneator`; CI folder is `.github/workflow` (singular — GitHub needs **`workflows`**); `READE.md` → should be `README.md`.

---

<a name="source"></a>
## 5. Stage 1–2 · Source (Postgres + Faker)

### `postgres/schema.sql` — the OLTP tables
```sql
CREATE TABLE customers (
  id SERIAL PRIMARY KEY,                              -- auto 1,2,3… unique id (indexed)
  first_name VARCHAR(100) NOT NULL,                  -- value required (quality at source)
  last_name  VARCHAR(100) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,                -- no two customers share an email
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()  -- auto insert-time (used for SCD2 ordering)
);

CREATE TABLE accounts (
  id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,  -- FK + auto-cleanup
  account_type VARCHAR(50) NOT NULL,
  balance NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),        -- exact money, never < 0
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE transactions (
  id BIGSERIAL PRIMARY KEY,                           -- BIG: millions of rows
  account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  txn_type VARCHAR(50) NOT NULL,                      -- DEPOSIT | WITHDRAWAL | TRANSFER
  amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
  related_account_id INT NULL,                        -- only for transfers (sender→receiver)
  status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX idx_transactions_account_created ON transactions(account_id, created_at);
```

| Concept | Plain meaning |
|---|---|
| `SERIAL` / `BIGSERIAL` | auto-incrementing id (BIG = 64-bit for huge tables) |
| `PRIMARY KEY` | unique + indexed row identifier |
| `NOT NULL` / `UNIQUE` / `CHECK` | **constraints** — the DB rejects bad data |
| `REFERENCES … ON DELETE CASCADE` | **foreign key**; delete parent → children auto-deleted (no orphans) |
| `NUMERIC(18,2)` | exact decimal — **never use FLOAT for money** |
| `INDEX` | makes "transactions for account X, newest first" fast |

**Schema shape:** customers → accounts → transactions is a **snowflake schema** (dimensions chained), not a pure star.

### `faker_generator.py` — the generator
- `Faker()` builds realistic fake names/emails; `fake.unique.email()` matches the `UNIQUE` constraint.
- Each loop: **10 customers → 20 accounts → 50 transactions**, every 2s.
- `RETURNING id` grabs the new primary key so the next table can reference it.
- `conn.autocommit = True` → each insert commits instantly → Debezium sees it immediately.
- `try/except KeyboardInterrupt` → Ctrl+C stops cleanly.

**Common mistake:** inserting accounts before their customer exists → FK violation. The code inserts customers first.

---

<a name="cdc"></a>
## 6. Stage 3–4 · CDC (Debezium + Kafka + Consumer)

### `generate_and_post_connector.py` — register the connector
It **doesn't move data** — it POSTs a config to Kafka Connect telling Debezium what to watch.
```python
"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
"database.hostname": os.getenv("POSTGRES_HOST"),   # 'postgres' = docker service name (NOT localhost)
"plugin.name": "pgoutput",                          # Postgres logical-decoding plugin
"slot.name": "banking_slot",                        # replication slot = bookmark in the WAL
"table.include.list": "public.customers,public.accounts,public.transactions",
"topic.prefix": "banking_server",                   # → topic banking_server.public.customers …
"decimal.handling.mode": "double",                  # NUMERIC as double, not base64 bytes
```

| Concept | Plain words |
|---|---|
| **CDC** | read the DB's **write-ahead log** instead of querying tables → no load, no locks |
| **Replication slot** | a bookmark so Debezium resumes exactly where it stopped |
| **`pgoutput`** | decoder turning binary WAL → readable change events (needs `wal_level=logical`) |
| **topic per table** | one Kafka topic per source table |

Response codes: **201** created · **409** already exists (fine) · else = error.

### `kafka_to_minio.py` — Kafka → data lake
```python
consumer = KafkaConsumer(
    'banking_server.public.customers', '...accounts', '...transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),  # host.docker.internal:29092
    auto_offset_reset='earliest',     # new group reads from the OLDEST message (capture history)
    enable_auto_commit=True,          # remember our position automatically
    group_id=os.getenv("KAFKA_GROUP"),# consumer GROUP = where we paused; survives restart
    max_partition_fetch_bytes=262144, # ← FIX: kafka-python 3.0.0 aborts fetches >= 1MB
    fetch_max_bytes=262144,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # bytes → JSON → dict
)
for message in consumer:
    record = message.value.get("payload", {}).get("after")  # Debezium wraps the new row in payload.after
    buffer[topic].append(record)
    if len(buffer[topic]) >= 50:        # batch 50 → one Parquet file (avoids the small-files problem)
        write_to_minio(...)             # df.to_parquet(...) → s3.upload_file(...)
```

| Setting | Why |
|---|---|
| `auto_offset_reset='earliest'` | capture all history on a fresh group |
| `group_id` | offset (position) stored per group → restart resumes, no dup/gap |
| `value_deserializer` | Kafka sends bytes → decode to a Python dict |
| `max_partition_fetch_bytes` | **the live bug**: cap fetch under 1MB so kafka-python 3.0.0 doesn't abort |
| batch of 50 | one Parquet per 50 rows, not one tiny file per row |

---

<a name="airflow"></a>
## 7. Stage 5 · Airflow (the DAGs)

Airflow = the **manager** running jobs on schedule, in order, with retries + logs.

### `minio_to_snowflake_dag.py` — lake → warehouse
```python
with DAG(
    dag_id="minio_to_snowflake_banking",
    schedule_interval="*/1 * * * *",   # cron: every minute
    start_date=datetime(2025,1,1),
    catchup=False,                      # don't back-run every missed interval since start_date
) as dag:
    task1 = PythonOperator(task_id="download_minio", python_callable=download_from_minio)
    task2 = PythonOperator(task_id="load_snowflake", python_callable=load_to_snowflake)
    task1 >> task2                      # task2 waits for task1
```
- **DAG** = Directed Acyclic Graph (tasks + order, no loops).
- **`catchup=False`** = critical — else Airflow queues every missed minute since 2025.
- **XCom** passes data between tasks: `kwargs["ti"].xcom_pull(task_ids="download_minio")`.
- Load logic: `PUT file://… @%table` (stage the Parquet) → `COPY INTO table … FILE_FORMAT=(TYPE=PARQUET)` (bulk load into the `variant` column). `ON_ERROR='CONTINUE'` skips bad rows.

### `scd_snapshots.py` — keep the warehouse fresh (ends the `dbt run` pain)
```python
with DAG(dag_id="SCD2_snapshots", schedule_interval="@daily", catchup=False) as dag:
    dbt_snapshot  = BashOperator(task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/banking_dbt && dbt snapshot --profiles-dir /home/airflow/.dbt")
    dbt_run_marts = BashOperator(task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/banking_dbt && dbt run --select marts --profiles-dir /home/airflow/.dbt")
    dbt_snapshot >> dbt_run_marts        # ← snapshots THEN marts (reference repo forgot this wiring!)
```
This automates the exact ordering that fixes "dim can't find snapshot": **snapshot first, then marts, daily.**

**Debugging in the UI (http://localhost:8080):** Grid view → click a red square → Logs (scroll to the bottom for the real error) → Graph view (task order).

---

<a name="dbt"></a>
## 8. Stage 5 · dbt (staging → snapshots → marts)

Flow: **source → staging (views) → snapshots (SCD2) → marts (dims + facts)**.

### `sources.yml`
```yaml
sources:
  - name: raw
    database: BANKING
    schema: RAW
    tables: [customers, accounts, transactions]
```
Lets you write `{{ source('raw','customers') }}` → resolves to `BANKING.RAW.CUSTOMERS` + tracks lineage.

### Staging — parse `variant` + dedup (view)
```sql
-- stg_customers.sql
with ranked as (
  select
    v:id::string            as customer_id,   -- v:field = pull a key out of the variant JSON + cast
    v:first_name::string    as first_name,
    v:email::string         as email,
    v:created_at::timestamp as created_at,
    row_number() over (partition by v:id::string order by v:created_at desc) as rn  -- newest per id
  from {{ source('raw','customers') }}
)
select customer_id, first_name, last_name, email, created_at
from ranked where rn = 1                       -- keep only the latest version (dedup the CDC stream)
```
`stg_transactions.sql` has **no dedup** — transactions are immutable, never updated.

### Snapshots — SCD2 history
```sql
{% snapshot customers_snapshot %}
{{ config(target_schema='ANALYTICS', unique_key='customer_id',
          strategy='check', check_cols=['first_name','last_name','email']) }}
SELECT * FROM {{ ref('stg_customers') }}
{% endsnapshot %}
```
- `strategy='check'` + `check_cols`: if any listed column changes for a `customer_id`, dbt **closes the old row** (`dbt_valid_to = now`) and **inserts a new one** (`dbt_valid_to = NULL`).
- dbt auto-adds `dbt_valid_from`, `dbt_valid_to`, `dbt_updated_at`. That's SCD2 — full history, no overwrites.

### Marts — the star schema
```sql
-- dim_customers.sql  (TABLE)
WITH latest AS (
  SELECT customer_id, first_name, last_name, email, created_at,
         dbt_valid_from AS effective_from,
         dbt_valid_to   AS effective_to,
         CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current  -- active flag
  FROM {{ ref('customers_snapshot') }}
)
SELECT * FROM latest

-- fact_transactions.sql  (INCREMENTAL)
{{ config(materialized='incremental', unique_key='transaction_id') }}
SELECT t.transaction_id, t.account_id, a.customer_id, t.amount, t.status,
       t.transaction_type, t.transaction_time, CURRENT_TIMESTAMP AS load_timestamp
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_accounts') }} a ON t.account_id = a.account_id   -- enrich with customer_id
```

| Materialization | Used for | Why |
|---|---|---|
| **view** | staging | light, always fresh, no storage |
| **table** | dims | queried often → store for speed |
| **incremental** | fact | huge → only insert NEW rows |
| **snapshot** | SCD2 | track history over time |

**Commands:**
```bash
cd banking_dbt
dbt debug                      # check connection
dbt build                      # models + snapshots + tests (USE THIS)
dbt run --select marts         # just the marts folder
dbt build --select stg_customers+   # a model + everything downstream
```
> **`dbt run` skips snapshots → dims fail.** With snapshots, **always `dbt build`.**

---

<a name="snowflake"></a>
## 9. Stage 6 · Snowflake objects

| Object | What it is | This project | Inspect |
|---|---|---|---|
| **Database** | top container | `BANKING` | `SHOW DATABASES;` |
| **Schema** | folder of tables | `RAW` (bronze), `ANALYTICS` (silver/gold) | `SHOW SCHEMAS IN BANKING;` |
| **Warehouse** | the **compute** (separate from storage) | `COMPUTE_WH` | `SHOW WAREHOUSES;` |
| **Role** | bundle of privileges (RBAC) | `SVC_AIRFLOW_ROLE` | `SHOW GRANTS TO ROLE …;` |
| **User** | login (key-pair auth) | `svc_airflow_user` | `DESC USER …;` |
| **Variant** | column holding any JSON | `RAW.*.v` | — |
| **Internal stage** | file landing area for loads | `@%customers` | `LIST @%customers;` |

**Key ideas:** storage/compute are separate (scale analytics without touching OLTP); privileges go to **roles**; the cleanest setup is for the transformer role to **own** the schema it writes to.

```sql
-- grants (run as ACCOUNTADMIN)
GRANT USAGE, CREATE VIEW, CREATE TABLE ON SCHEMA BANKING.ANALYTICS TO ROLE SVC_AIRFLOW_ROLE;
GRANT CREATE SCHEMA ON DATABASE BANKING TO ROLE SVC_AIRFLOW_ROLE;   -- the permanent fix
-- analytics
SELECT customer_id, SUM(amount) FROM BANKING.ANALYTICS.FACT_TRANSACTIONS GROUP BY 1 ORDER BY 2 DESC;
```

---

<a name="infra"></a>
## 10. Infrastructure & CI/CD

### `docker-compose.yml`
Defines: `zookeeper` → `kafka` → `connect` (Debezium) → `postgres` (`wal_level=logical`) → `minio` → `airflow-init/scheduler/webserver` + `airflow-postgres`. `${VAR}` pulls from `.env`; `volumes:` persist data; `up -d` starts all, `down -v` wipes.

### Secrets
- `.env` holds credentials → `${VAR}` (compose) / `os.getenv()` (Python). **Never committed.**
- `.env.example` = same keys, fake values → committed as documentation.
- GitHub Actions secrets hold Snowflake creds for CI: `${{ secrets.SNOWFLAKE_PASSWORD }}`.

### CI/CD
- **CI** (on push/PR): clean Ubuntu VM → install deps → linters + `dbt compile` → if it fails, code **can't merge**.
- **CD** (on merge to `main`): auto-run `dbt run`/tests to deploy.
- **Branch flow:** `dev` → push → CI → PR → merge `main` → CD.
- ⚠️ folder must be **`.github/workflows`** (plural) or Actions never run.

---

<a name="glossary"></a>
## 11. Concepts Glossary

| Term | Simple definition |
|---|---|
| **OLTP** | system for fast transactional writes (Postgres) |
| **OLAP** | system for heavy analytical reads (Snowflake) |
| **CDC** | Change Data Capture — stream only what changed, via the DB log |
| **WAL** | Write-Ahead Log — every change written here first (durability) |
| **ACID** | Atomicity, Consistency, Isolation, Durability — DB transaction guarantees |
| **SCD2** | Slowly Changing Dimension type 2 — keep history; mark old rows inactive |
| **Star schema** | central fact table + surrounding dimension tables |
| **Snowflake schema** | star schema where dimensions are further normalized/chained |
| **Fact table** | measurable events (transactions) |
| **Dimension table** | descriptive context (customers, accounts) |
| **Bronze/Silver/Gold** | raw → cleaned → business-ready tiers |
| **Idempotent** | running it twice = same result (e.g. incremental loads) |
| **Parquet** | columnar, compressed, typed file format for analytics |
| **DAG** | Directed Acyclic Graph — Airflow's tasks + order |
| **Materialization** | how dbt persists a model (view / table / incremental) |

---

<a name="interview"></a>
## 12. Interview Questions

**Architecture**
- *Why SQL not NoSQL for banking?* → Structure + ACID; money needs strong consistency.
- *Why separate OLTP and OLAP?* → Analytics scans would slow live transactions; different workloads, different engines.

**CDC / Kafka**
- *How does CDC avoid loading the source DB?* → Reads the WAL (written anyway), not the tables — no extra queries/locks.
- *Producer/consumer at different speeds — what prevents loss?* → Kafka is a durable buffer; the consumer **group offset** tracks position across restarts.

**dbt**
- *`dbt run` vs `dbt build`?* → `run` = models only; `build` = models + snapshots + tests in DAG order. With snapshots, use `build`.
- *How is SCD2 implemented?* → dbt snapshots with `strategy='check'`; old rows get `dbt_valid_to`, new rows are inserted with NULL.
- *view vs table vs incremental?* → light/always-fresh; queried-often/stored; huge/append-only.

**Snowflake**
- *Why good for analytics vs Postgres?* → Storage/compute separation, columnar storage, elastic warehouses.
- *You keep hitting permission walls — root cause?* → Ownership: let the transformer role **own** its schema; ownership > per-object grants.

**Airflow**
- *What is `catchup=False`?* → Stops back-filling every missed schedule since `start_date`.
- *How do you debug a failed task?* → Grid view → failed run → Logs (bottom = real error) → Graph view.

**Docker / Infra**
- *App works locally but not in Docker — why?* → `localhost` ≠ service name; containers reach each other by service name.
- *What does CI give a data team?* → Quality gates: broken SQL/Python never reaches `main`.

---

<a name="lessons"></a>
## 13. Top 10 Lessons

1. **M-series Macs need multi-arch Docker images** (or `platform: linux/amd64`).
2. **Airflow needs `db migrate` + a created user** — automate with an `airflow-init` service.
3. **Activate the venv, then use plain `python`** — never the full system path.
4. **One port, one server** — `lsof -iTCP:<port>` finds conflicts.
5. **`localhost` ≠ service name** — containers connect by service name.
6. **`load_dotenv()` loads the `.env` next to the script** — beware multiple `.env` files.
7. **Silent consumer? Turn on logging** — it revealed the kafka-python 1 MB fetch bug.
8. **Snowflake `private_key` needs DER bytes** + proper PEM framing.
9. **`dbt build`, not `dbt run`**, whenever snapshots/tests exist.
10. **`.gitignore` only works on untracked files** — `git rm --cached` to fix leaks.

---

*The complete guide to your banking modern data stack.* 🏦
*Postgres → Debezium/Kafka → MinIO → Airflow → Snowflake → dbt → Power BI*
