FROM python:3.11-slim as base
LABEL maintainer="DataOps.ETL"

ARG ONETL_USER_HOME=/usr/local/onetl

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
    && useradd -ms /bin/bash -d ${ONETL_USER_HOME} onetl \
    && chown -R onetl:onetl ${ONETL_USER_HOME} \
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
ENV PATH=${ONETL_USER_HOME}/.local/bin:${PATH}

COPY --chown=onetl:onetl ./run_tests.sh ./pytest_runner.sh ./combine_coverage.sh /app/
COPY --chown=onetl:onetl ./docker/wait-for-it.sh /app/docker/wait-for-it.sh
RUN chmod +x /app/run_tests.sh /app/pytest_runner.sh /app/combine_coverage.sh /app/docker/wait-for-it.sh

ARG SPARK_VERSION=3.4.1
# Spark is heavy, and version change is quite rare
COPY --chown=onetl:onetl ./requirements/tests/spark-${SPARK_VERSION}.txt /app/requirements/tests/
RUN pip install -r /app/requirements/tests/spark-${SPARK_VERSION}.txt

COPY --chown=onetl:onetl ./requirements/ /app/requirements/
RUN pip install \
    -r /app/requirements/core.txt \
    -r /app/requirements/ftp.txt \
    -r /app/requirements/hdfs.txt \
    -r /app/requirements/s3.txt \
    -r /app/requirements/sftp.txt \
    -r /app/requirements/webdav.txt \
    -r /app/requirements/kerberos.txt \
    -r /app/requirements/docs.txt \
    -r /app/requirements/tests/base.txt \
    -r /app/requirements/tests/clickhouse.txt \
    -r /app/requirements/tests/mongodb.txt \
    -r /app/requirements/tests/mssql.txt \
    -r /app/requirements/tests/mysql.txt \
    -r /app/requirements/tests/oracle.txt \
    -r /app/requirements/tests/postgres.txt \
    -r /app/requirements/tests/kafka.txt

FROM base AS full

COPY . .
