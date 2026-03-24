FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
