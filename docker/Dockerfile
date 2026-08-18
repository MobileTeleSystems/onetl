FROM python:3.13-slim-bookworm AS base
LABEL maintainer="MWS Data Bridge"

ENV LANGUAGE=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        procps \
        openjdk-17-jdk \
        libsasl2-modules \
        libsasl2-dev \
        libaio1 \
        libkrb5-dev \
        gcc \
        zip \
        unzip && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir /app/ \
    && useradd -ms /bin/bash onetl \
    && chown -R onetl:onetl /app/

RUN wget -P /tmp https://download.oracle.com/otn_software/linux/instantclient/2110000/instantclient-basic-linux.x64-21.10.0.0.0dbru.zip && \
    mkdir -p /opt/oracle && \
    unzip /tmp/instantclient-basic-linux.x64-21.10.0.0.0dbru.zip -d /opt/oracle && \
    rm -rf /tmp/*

ENV ONETL_ORA_CLIENT_PATH=/opt/oracle/instantclient_21_10
ENV LD_LIBRARY_PATH=${ONETL_ORA_CLIENT_PATH}:${LD_LIBRARY_PATH} \
    PATH=${ONETL_ORA_CLIENT_PATH}:${PATH}

WORKDIR /app
USER onetl
ENV PATH=/app/.venv/bin:${PATH}

COPY --chown=onetl:onetl Makefile /app/

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install uv

ARG SPARK_VERSION=3.5
ARG ETL_ENTITIES_VERSION=3
COPY --chown=onetl:onetl pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync \
    --all-extras \
    --group dev \
    --group docs \
    --group test \
    --group test-clickhouse \
    --group test-mongodb \
    --group test-mssql \
    --group test-mysql \
    --group test-oracle \
    --group test-postgres \
    --group test-etl-entities-${ETL_ENTITIES_VERSION} \
    --group test-spark-${SPARK_VERSION}

FROM base AS full

COPY . .
