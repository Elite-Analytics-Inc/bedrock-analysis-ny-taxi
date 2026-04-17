FROM python:3.12-slim

WORKDIR /app

# System deps for DuckDB native extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python deps: duckdb (includes iceberg + httpfs extensions), fastavro
RUN pip install --no-cache-dir \
    duckdb==1.5.1 \
    fastavro==1.9.7

# Bedrock SDK — copied in from the monorepo base image layer at build time.
# In production the SDK is injected via a shared base image or init-container.
# For this dev image we vendor it directly.
COPY bedrock_sdk/ /bedrock_sdk/

COPY analysis.py .
COPY dashboard/ dashboard/

CMD ["python", "analysis.py"]
# duckdb 1.2.2, fastavro 1.9.7
