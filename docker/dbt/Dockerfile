FROM python:3.11-slim

WORKDIR /usr/app
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir dbt-duckdb==1.8.2 \
    && mkdir -p /root/.dbt

COPY dbt /usr/app/dbt
COPY docker/dbt/profiles.yml /root/.dbt/profiles.yml

WORKDIR /usr/app/dbt
ENTRYPOINT ["dbt"]
CMD ["parse"]
