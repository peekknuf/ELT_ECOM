FROM quay.io/astronomer/astro-runtime:10.5.0

VOLUME /usr/local/airflow/data_pipeline/seeds
RUN chmod -R 777 /usr/local/airflow/data_pipeline/seeds

RUN python3 -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-duckdb faker && deactivate

