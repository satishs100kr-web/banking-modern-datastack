"""
Daily SCD2 refresh DAG.

Runs the dbt snapshots first, THEN rebuilds the marts that read from them.
The `>>` dependency is the important fix the reference repo missed — without it
the marts could run before the snapshots exist (the "dim can't find snapshot" error).

dbt runs INSIDE the Airflow container:
  - the dbt project is mounted at /opt/airflow/banking_dbt
  - the profiles.yml is mounted at /home/airflow/.dbt/profiles.yml
  - the Snowflake key is mounted at /opt/airflow/keys/rsa_key.p8
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

DBT = "cd /opt/airflow/banking_dbt && dbt {cmd} --profiles-dir /home/airflow/.dbt"

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Daily SCD2 refresh: dbt snapshot then rebuild marts",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "snapshots", "scd2"],
) as dag:

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=DBT.format(cmd="snapshot"),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=DBT.format(cmd="run --select marts"),
    )

    # snapshots MUST finish before the marts that read from them
    dbt_snapshot >> dbt_run_marts
