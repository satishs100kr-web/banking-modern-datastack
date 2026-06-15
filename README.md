# 🏦 Banking Modern Data Stack

A **real-time, enterprise-grade data engineering pipeline** for banking — built end to end with
Change Data Capture, a data lake, a cloud warehouse, SCD2 history, orchestration, and CI/CD.

> **Pipeline:** `PostgreSQL → Debezium/Kafka (CDC) → MinIO (lake) → Airflow → Snowflake → dbt → Power BI`

---

## 🏗️ Architecture

![Architecture diagram](docs/architecture.svg)

```
 faker_generator.py  →  PostgreSQL (OLTP)
        │  Debezium captures every change (CDC) via the write-ahead log
        ▼
 Kafka topics  →  consumer  →  MinIO (S3-compatible data lake, Parquet)
        │  Apache Airflow orchestrates the load
        ▼
 Snowflake  RAW (bronze)  →  dbt: staging → snapshots (SCD2) → marts (star schema)  →  ANALYTICS
        │  direct query
        ▼
 Power BI dashboards
```

## ✨ What this project demonstrates

- **Change Data Capture (CDC)** — stream `INSERT/UPDATE/DELETE` live with Debezium (zero load on the source).
- **Data lake** — land raw changes as partitioned **Parquet** in MinIO (S3-compatible).
- **Cloud warehouse** — load into **Snowflake** and model with **dbt**.
- **SCD2 history** — track changing customer/account data with dbt **snapshots**.
- **Star schema** — `dim_customers`, `dim_accounts`, `fact_transactions`.
- **Orchestration** — **Airflow** DAGs schedule the load + nightly snapshots.
- **CI/CD** — **GitHub Actions** lint + `dbt compile` on every push.
- **Security** — secrets in `.env` (never committed), Snowflake **key-pair (JWT)** auth, RBAC roles.

## 🧰 Tech Stack

`PostgreSQL` · `Debezium` · `Apache Kafka` · `MinIO` · `Apache Airflow` · `Snowflake` · `dbt` ·
`Docker Compose` · `Power BI` · `Python` · `GitHub Actions`

## 📚 Documentation

The full, beginner-friendly guide lives in [`docs/`](docs/):

| Doc | What it covers |
|---|---|
| **[docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)** | 📖 Complete guide — architecture, every file line-by-line, ports, **click-by-click UI tours**, glossary, interview Q&A |
| **[docs/COMMANDS_JOURNEY.md](docs/COMMANDS_JOURNEY.md)** | ⌨️ Every command + bug + fix, by phase |
| **[docs/index.html](docs/index.html)** | 🌐 Interactive web view (open in a browser) |

## 🚀 Quick Start

```bash
# 1. Start the whole stack
docker compose up -d

# 2. Python env + dependencies
uv venv --python 3.12 && source .venv/bin/activate
uv pip install -r requirements.txt

# 3. Generate data → register CDC → consume to the lake
python data-geneator/faker_generator.py --once
python kafka-debezium/generate_and_post_connector.py
python consumer/kafka_to_minio.py

# 4. Transform in Snowflake with dbt
cd banking_dbt && dbt build
```

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Debezium | http://localhost:8083/connectors | — |

> ⚙️ Copy `.env.example` → `.env` and fill in your own credentials before running.

---

*Built as a hands-on data engineering portfolio project.* 🏦
