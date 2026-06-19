# =============================================================================
#  dockerfile-airflow.dockerfile  —  builds a CUSTOM Airflow image.
#  WHY: the stock Airflow image doesn't have dbt. The SCD2_snapshots DAG runs
#  `dbt snapshot` / `dbt run` INSIDE the Airflow container, so dbt must be
#  installed there. This Dockerfile = stock Airflow + dbt.
#  (docker-compose's `build:` lines tell the airflow services to use this.)
# =============================================================================

# Start FROM the official Apache Airflow 2.9.3 image (our base).
FROM apache/airflow:2.9.3

# Run the next commands as the non-root 'airflow' user (pip installs go into the
# airflow user's environment, and Airflow refuses to run as root).
USER airflow

# Install dbt + the Snowflake adapter into the image. --no-cache-dir keeps the
# image smaller. After this, `dbt` is available inside the Airflow containers.
RUN pip install --no-cache-dir dbt-core dbt-snowflake
