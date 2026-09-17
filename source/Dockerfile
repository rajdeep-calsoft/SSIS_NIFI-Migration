FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir faker psycopg2-binary "psycopg[binary]" PyYAML pytest

WORKDIR /app