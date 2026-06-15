# 🏦 Banking Modern Data Stack — Command Journey & Troubleshooting Log

> A complete, beginner-friendly record of **every command** we ran to build this real-time
> banking data pipeline, **what each one does**, **why we ran it**, and the **lesson** learned
> when something broke. Read top-to-bottom like a story, or jump to a phase.

**Pipeline:** `Postgres → Debezium/Kafka (CDC) → MinIO (data lake) → Airflow → Snowflake → dbt → Power BI`

---

## 📑 Table of Contents
1. [Phase 1 — Docker & the Stack](#phase-1)
2. [Phase 2 — Airflow Bring-up](#phase-2)
3. [Phase 3 — Python Environment (uv + venv)](#phase-3)
4. [Phase 4 — Postgres & DBeaver](#phase-4)
5. [Phase 5 — Debezium CDC Connector](#phase-5)
6. [Phase 6 — Kafka → MinIO Consumer](#phase-6)
7. [Phase 7 — Snowflake Key-Pair Auth](#phase-7)
8. [Phase 8 — dbt (staging → snapshots → marts)](#phase-8)
9. [Phase 9 — Snowflake Permissions (RBAC)](#phase-9)
10. [Phase 10 — Git & GitHub](#phase-10)
11. [🎓 Top 10 Lessons](#lessons)

---

<a name="phase-1"></a>
## 🐳 Phase 1 — Docker & the Stack

### Start all containers
```bash
docker compose -f docker-compose.yml up        # foreground (see all logs, Ctrl+C stops)
docker compose up -d                            # detached / background (recommended)
```
**What:** reads `docker-compose.yml` and starts every service (Kafka, Zookeeper, Debezium, Postgres, MinIO, Airflow). `-d` runs them in the background so your terminal is free.

### Other lifecycle commands
```bash
docker compose ps                               # list containers + status
docker compose logs -f airflow-webserver        # follow one service's logs live
docker compose down                             # stop + remove containers (KEEPS data)
docker compose down -v                           # also DELETE volumes (wipes the databases)
docker compose restart airflow-scheduler airflow-webserver
```

### 🐞 Problem: `no matching manifest for linux/arm64/v8`
**Cause:** the `debezium/connect:2.2` image had **no ARM64 build** for the Apple-Silicon (M-series) Mac.
**Fix:** bumped to a multi-arch tag in `docker-compose.yml`:
```yaml
image: debezium/connect:2.7.3.Final     # has a native ARM64 build
```
**Lesson:** on M-series Macs, always use **multi-arch** images, or pin `platform: linux/amd64` to run the Intel image via emulation (slower).

---

<a name="phase-2"></a>
## 🌬️ Phase 2 — Airflow Bring-up

### 🐞 Problem: Airflow crash-loop — `relation "log" does not exist`
**Cause:** the Airflow **metadata database was never initialized** (no tables), so the webserver and scheduler crashed and restarted forever.

### Fix — initialize the DB and create a login
```bash
# create the Airflow metadata tables (modern command; 'db init' is deprecated)
docker compose run --rm airflow-scheduler airflow db migrate

# create the admin user for the web UI (http://localhost:8080)
docker compose run --rm airflow-scheduler airflow users create \
  --username admin --password admin \
  --firstname Satish --lastname Kumar \
  --role Admin --email satishs100kr@gmail.com

# restart the crashing services so they pick up the new tables
docker compose restart airflow-scheduler airflow-webserver
```
- `run --rm` = start a **throwaway** container just to run one command, then delete it.
- We also added a permanent **`airflow-init`** service so this runs automatically on a fresh `up`.

### 🐞 Problem: "Invalid login"
```bash
docker compose run --rm airflow-scheduler airflow users list          # confirm the user exists
docker compose run --rm airflow-scheduler airflow users reset-password \
  --username admin --password admin                                   # reset it
```
**Lesson:** the **`.env` database password** (`AIRFLOW_DB_PASSWORD`) is **NOT** the web-UI login. The web login is the `admin` user you create separately.

### `airflow-init` shows "Exited" — is that bad?
**No — that's correct.** An init/one-shot container does its job (create tables + user) and **exits**. Only the long-running services (scheduler, webserver) stay green.

---

<a name="phase-3"></a>
## 🐍 Phase 3 — Python Environment (uv + venv)

### Install `uv` (the fast, modern package manager)
```bash
brew install uv
```
**Why `uv`?** Rust-based, 10–100× faster than plain `pip`; the 2025–2026 standard.

### Create an isolated environment and install deps
```bash
uv venv --python 3.12                 # download + create a Python 3.12 virtual env (.venv/)
source .venv/bin/activate             # ACTIVATE it (prompt shows (BANK_DE_PROJECT))
uv pip install -r requirements.txt    # install all project packages into .venv
```
**Why Python 3.12, not 3.14?** Several packages (`dbt-snowflake`, `fastparquet`) had **no 3.14 builds yet**. `uv` fetched a clean 3.12 just for this project.

### 🐞 Problem: `ModuleNotFoundError: No module named 'psycopg2'`
**Cause:** ran the script with the **system** Python (`/opt/homebrew/bin/python3`), which is empty — the packages live in `.venv`.
```bash
# ❌ wrong — uses empty system Python
/opt/homebrew/bin/python3 script.py
# ✅ right — activate first, then use plain `python`
source .venv/bin/activate
python data-geneator/faker_generator.py --once
```
**Lesson:** after `source .venv/bin/activate`, **always type just `python`**. The full `/opt/homebrew/...` path bypasses your venv.

> **`source .venv/bin/activate`** needs spaces and the `.venv` part. `source/bin/activate` fails.

---

<a name="phase-4"></a>
## 🐘 Phase 4 — Postgres & DBeaver

### 🐞 Problem: DBeaver says `role "postgres" does not exist`
**Cause:** **two** Postgres servers were both fighting over port **5432** — a Homebrew one and the Docker one. DBeaver was hitting the Homebrew one (which had no `postgres` role).

### Diagnose & fix
```bash
brew services list | grep postgres                 # found Homebrew postgresql@16 running
lsof -nP -iTCP:5432 -sTCP:LISTEN                    # see what is listening on 5432
brew services stop postgresql@16                   # stop it so Docker owns the port
```
**Lesson:** when a DB "won't connect" but settings look right, check **what is actually on the port**:
```bash
lsof -nP -iTCP:5432 -sTCP:LISTEN
```

### ⚠️ Wrong file warning
We almost edited `/opt/homebrew/.../python3.14/os.py` — that's **Python's own source code**, not the project. **Never edit it.** Credentials belong in the project's `.env`, never in Python internals.

---

<a name="phase-5"></a>
## 🔌 Phase 5 — Debezium CDC Connector

### Register the connector (Python script POSTs JSON to Kafka Connect)
```bash
python kafka-debezium/generate_and_post_connector.py
```
Response meaning: **201** = created ✅ · **409** = already exists (also fine) ✅ · anything else = error.

### Manually test / inspect the connector with `curl`
```bash
# create the connector directly via REST
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" -d '{ ...connector config... }'

# list connectors / check status
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/postgres-connector/status
```

### 🐞 Problem: `database.hostname value is invalid: A value is required`
**Cause:** the script read `POSTGRES_HOST`, but it was **missing** from the `.env` it loaded.
**Discovery:** the project has **5 separate `.env` files** (one per folder). `load_dotenv()` loads the one **next to the script** (`kafka-debezium/.env`), not the project root.
**Key insight:** Debezium runs **inside Docker**, so its `database.hostname` must be the **service name** `postgres` — **not** `localhost`.
**Lesson:** `localhost` means "myself." Inside a container it points to the container, not the DB. Containers reach each other by **service name**.

---

<a name="phase-6"></a>
## 📨 Phase 6 — Kafka → MinIO Consumer

### Generate data & verify the CDC flow
```bash
python data-geneator/faker_generator.py --once       # insert fake rows into Postgres

# confirm Debezium created the Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep banking

# peek at the actual CDC messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic banking_server.public.customers --from-beginning --max-messages 1

# check consumer-group lag (how far behind the consumer is)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group minio-landing-group
```

### Run the consumer
```bash
python consumer/kafka_to_minio.py
```

### 🐞 Problem 1: consumer can't connect (`host.docker.internal` not found)
**Cause:** on **Windows** Docker makes `host.docker.internal` resolve on the host automatically; on **Mac** it does **not**.
```bash
echo "127.0.0.1 host.docker.internal" | sudo tee -a /etc/hosts   # make Mac resolve it
ping -c 1 host.docker.internal                                   # verify → 127.0.0.1
```

### 🐞 Problem 2: connects but pulls **0 messages** ("stuck")
**Real error (found via debug logging):** `InvalidReceiveError: Invalid frame length: 1048666`.
**Cause:** `kafka-python 3.0.0` **rejects any fetch ≥ 1 MB**, and the default fetch size is exactly 1 MB. Debezium messages (with embedded schema) tip it over.
**Fix (in `kafka_to_minio.py`):**
```python
max_partition_fetch_bytes=262144,   # 256 KB, safely under 1 MB
fetch_max_bytes=262144,
```
**Lesson:** when a consumer connects but never advances, **turn on logging** — silent failures hide the real cause.
```python
import logging; logging.basicConfig(level=logging.WARNING)
```

---

<a name="phase-7"></a>
## ❄️ Phase 7 — Snowflake Key-Pair (JWT) Auth

### 🐞 Problem: `Unable to load PEM file ... MalformedFraming`
**Cause:** the private key in `.env` was just the **base64 body** with **no** `-----BEGIN/END-----` lines, and the code passed a raw string where DER bytes were needed.
**Fix:** rebuild a valid key file and convert to DER bytes:
```bash
# (the code now) wraps the base64 with proper PEM armor, then:
#   load_pem_private_key(pem) -> .private_bytes(DER, PKCS8, NoEncryption())
#   snowflake.connector.connect(authenticator="SNOWFLAKE_JWT", private_key=<DER bytes>, ...)
```
Generate a proper key pair (reference):
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```
Then register the **public** key in Snowflake:
```sql
ALTER USER svc_airflow_user SET RSA_PUBLIC_KEY='MIIBIj...';
```
**Lesson:** the Snowflake connector's `private_key` wants **DER bytes**, not a PEM string — and the PEM needs its `BEGIN/END` framing.

---

<a name="phase-8"></a>
## 🔧 Phase 8 — dbt (staging → snapshots → marts)

### Core commands
```bash
cd banking_dbt                         # ALWAYS run dbt from inside the project folder
dbt debug                              # check connection + config (look for "All checks passed!")
dbt build                              # run models + snapshots + tests, in dependency order ✅
dbt run                                # models ONLY (skips snapshots) — caused repeated failures ❌
dbt snapshot                           # build the SCD2 snapshot tables only
dbt run --select stg_customers         # build just one model
dbt run --select marts                 # build everything in the marts/ folder
dbt build --select stg_customers+      # this model AND everything downstream of it
```

### 🐞 Problem: `dbt init banking_dbt` → "project already exists"
**Not an error** — you only run `dbt init` once. The project was already there; `dbt` refuses to overwrite it.

### 🐞 Problem: `'BANKING.ANALYTICS.CUSTOMERS_SNAPSHOT' does not exist`
**Cause:** ran `dbt run` (which **skips snapshots**), but the `dim_` models read from snapshots.
**Fix:** use **`dbt build`** (or `dbt snapshot` first).
**Lesson (the big one):**

| Command | Builds | Snapshots? |
|---|---|---|
| `dbt run` | models only | ❌ no |
| **`dbt build`** | models **+ snapshots + tests** | ✅ yes |

> If your project has snapshots, **always `dbt build`.**

### Moving models from `RAW` to `ANALYTICS`
Changed `schema:` in `~/.dbt/profiles.yml` from `raw` to `ANALYTICS` → all models now build into the `ANALYTICS` schema (sources still read `RAW`).

---

<a name="phase-9"></a>
## 🔐 Phase 9 — Snowflake Permissions (RBAC)

Run these in the Snowflake UI as `ACCOUNTADMIN`. Each new error was a **missing grant** on a schema your role didn't own.

```sql
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS SVC_AIRFLOW_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH         TO ROLE SVC_AIRFLOW_ROLE;
GRANT USAGE ON DATABASE  BANKING            TO ROLE SVC_AIRFLOW_ROLE;
GRANT USAGE ON SCHEMA    BANKING.RAW        TO ROLE SVC_AIRFLOW_ROLE;
GRANT CREATE VIEW, CREATE TABLE ON SCHEMA BANKING.RAW       TO ROLE SVC_AIRFLOW_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA BANKING.RAW            TO ROLE SVC_AIRFLOW_ROLE;

-- snapshots build into ANALYTICS, so grant there too
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA BANKING.ANALYTICS TO ROLE SVC_AIRFLOW_ROLE;

-- THE permanent fix: let the role create + own schemas (no more per-object grants)
GRANT CREATE SCHEMA ON DATABASE BANKING    TO ROLE SVC_AIRFLOW_ROLE;

GRANT ROLE SVC_AIRFLOW_ROLE TO USER svc_airflow_user;
ALTER USER svc_airflow_user SET DEFAULT_ROLE = SVC_AIRFLOW_ROLE;
```

Inspect what exists / what the role can do:
```sql
SHOW SCHEMAS IN DATABASE BANKING;
SHOW TABLES IN SCHEMA BANKING.ANALYTICS;
SHOW GRANTS TO ROLE SVC_AIRFLOW_ROLE;
```
**Lesson:** in Snowflake, privileges go to **roles**, and the cleanest setup is for the transformer role to **own** the schema it writes to. Ownership beats chasing individual grants.

---

<a name="phase-10"></a>
## 🌿 Phase 10 — Git & GitHub

### 🐞 Problem: `git push` → "No configured push destination" + repo in wrong folder
The repo was initialized **inside `banking_dbt/`** instead of the project root.

### Fix — one clean repo at the root, no secrets
```bash
rm -rf banking_dbt/.git                         # remove the misplaced repo
cd /Users/satishkumar/DE/BANK_DE_PROJECT
git init && git branch -M main                  # init at the project root, branch 'main'
git add -A                                       # stage everything (gitignore excludes secrets)

# 🔒 verify NO secrets / data dirs are staged
git ls-files | grep -iE "\.env$|\.p8|rsa_key"   # should print nothing

# gitignore can't untrack already-staged files → remove them from the index
git rm -r --cached -f docker/postgres docker/minio banking_dbt/target

git commit -m "Initial commit: banking modern data stack"
```

### Create the GitHub repo and push (one command, using `gh`)
```bash
gh repo create banking-modern-datastack --public --source=. --remote=origin --push
# or manually:
git remote add origin https://github.com/<user>/<repo>.git
git push -u origin main
```

### Branch workflow for CI/CD
```bash
git checkout -b dev          # work on a dev branch
git push origin dev          # push → CI runs → open PR → merge to main → CD runs
```
**Lesson:** `.gitignore` is **ignored for files already tracked**. If a secret/data dir got staged, `git rm --cached <path>` to untrack it first — adding it to `.gitignore` alone does nothing.

---

<a name="lessons"></a>
## 🎓 Top 10 Lessons From This Build

1. **M-series Macs need multi-arch Docker images** (or `platform: linux/amd64`).
2. **Airflow needs `db migrate` + a created user** before it runs — automate with an `airflow-init` service.
3. **Activate the venv** (`source .venv/bin/activate`), then use plain **`python`** — never the full system path.
4. **One port, one server** — `lsof -iTCP:<port>` finds conflicts (Homebrew vs Docker Postgres).
5. **`localhost` ≠ service name** — inside Docker, containers reach each other by **service name**.
6. **`load_dotenv()` loads the `.env` next to the script** — beware multiple `.env` files.
7. **Silent consumer? Turn on logging** — it revealed the kafka-python 1 MB fetch bug.
8. **Snowflake `private_key` needs DER bytes** + proper PEM framing.
9. **`dbt build`, not `dbt run`**, whenever snapshots/tests exist.
10. **`.gitignore` only works on untracked files** — `git rm --cached` to fix leaks.

---

*Generated from a hands-on debugging session. Pipeline: Postgres → Debezium/Kafka → MinIO → Airflow → Snowflake → dbt → Power BI.* 🏦
