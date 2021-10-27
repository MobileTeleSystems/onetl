FROM docker.rep.msk.mts.ru/platform/python:3.7-slim as wheel_builder

RUN unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy \
    && yum -y install make wheel gcc gcc-c++ \
    && pip3 install -U pip setuptools

COPY ./requirements.txt \
    ./README.rst \
    ./setup.py \
    ./version.py \
    /opt/build_dir/
COPY ./onetl /opt/build_dir/onetl

ARG BUILD_NUMBER
ARG BRANCH_NAME

# Build /opt/build_dir/dist/oneloader*.whl
RUN cd /opt/build_dir && python3 setup.py bdist_wheel

FROM docker.rep.msk.mts.ru/platform/python:3.7-slim
LABEL maintainer="oneTools team"
# http://docker.rep.msk.mts.ru/artifactory/webapp/#/packages/docker/platform~2Fpython

ARG ONETL_USER_HOME=/usr/local/onetl
ARG TZ=Europe/Moscow

ENV TZ=${TZ} \
    LANGUAGE=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    HADOOP_CONF_DIR=/opt/project/docker/hive2/hadoop/conf/ \
    ONETL_PG_CONN_USER=onetl \
    ONETL_PG_CONN_HOST=postgres \
    ONETL_PG_CONN_PORT=5432 \
    ONETL_PG_CONN_PASSWORD=onetl \
    ONETL_PG_CONN_DATABASE=onetl \
    ONETL_ORA_CONN_USER=onetl \
    ONETL_ORA_CONN_HOST=oracle \
    ONETL_ORA_CONN_PORT=1521 \
    ONETL_ORA_CONN_PASSWORD=onetl \
    ONETL_ORA_CONN_DATABASE=onetl \
    ONETL_ORA_CLIENT_PATH=/usr/lib/oracle/21/client64/lib \
    ONETL_ORA_CONN_SID=XE \
    ONETL_CH_CONN_USER=default \
    ONETL_CH_CONN_HOST=clickhouse \
    ONETL_CH_CONN_PASSWORD="" \
    ONETL_CH_CONN_PORT=9000 \
    ONETL_CH_CONN_DATABASE=default \
    MTSPARK_CUSTOM_CONFIG_PATH=/opt/project/tests/config.yml

COPY --from=wheel_builder /opt/build_dir/dist/ /opt/build_dir/dist/

COPY ./requirements.txt \
    ./requirements-test.txt \
    ./docker/entrypoint.sh \
    ${ONETL_USER_HOME}/

COPY . /opt/project/

RUN unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy \
    && wget -P /tmp http://docker.rep.msk.mts.ru:80/artifactory/common-rpm/oracle-instantclient-basic-21.3.0.0.0-1.el8.x86_64.rpm \
    && yum -y install /tmp/oracle-instantclient-basic-21.3.0.0.0-1.el8.x86_64.rpm \
    && yum -y install \
        java \
        krb5-devel \
        cyrus-sasl-devel \
        zlib-devel \
        bzip2-devel \
        make \
        gcc \
        gcc-c++ \
        # for oracle client
        libaio1\
        libaio\
        # hive
        libsasl2-modules\
        libsasl2-dev\
        cyrus-sasl-plain\
    && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && pip install -U pip setuptools \
    && pip install -i http://rep.msk.mts.ru/artifactory/api/pypi/pypi-dev-local/simple --extra-index-url=http://rep.msk.mts.ru/artifactory/api/pypi/pypi/simple --trusted-host=rep.msk.mts.ru --retries 15 --timeout 30 -r ${ONETL_USER_HOME}/requirements-test.txt \
    && yum -y autoremove gcc make \
    && yum -y erase krb5-devel cyrus-sasl-devel zlib-devel bzip2-devel \
    && yum clean all \
    && rm -rf /tmp/* /var/tmp/* /root/.cache/* \
    && echo $(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)") > ${ONETL_USER_HOME}/fernet.key \
    && mv ${ONETL_USER_HOME}/entrypoint.sh / \
    && useradd -ms /bin/bash -d ${ONETL_USER_HOME} onetl \
    && chown -R onetl:onetl ${ONETL_USER_HOME} \
    && chown -R onetl:onetl /opt/project/

USER onetl

WORKDIR /opt/project/

ENTRYPOINT ["/entrypoint.sh"]
