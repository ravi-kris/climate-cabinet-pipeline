FROM apache/airflow:2.10.5-python3.11

USER root
COPY pyproject.toml README.md /opt/project/
COPY src /opt/project/src
RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}" /opt/project
USER airflow
