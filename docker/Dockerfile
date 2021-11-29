FROM docker.rep.msk.mts.ru/platform/python:3.7.7
LABEL maintainer="oneTools team"
# http://docker.rep.msk.mts.ru/artifactory/webapp/#/packages/docker/platform~2Fpython

ARG ONETL_USER_HOME=/usr/local/onetl

ENV LANGUAGE=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HADOOP_CONF_DIR=/opt/project/docker/hive2/hadoop/conf/ \
    MTSPARK_CUSTOM_CONFIG_PATH=/opt/project/tests/config.yml


RUN wget -P /tmp http://docker.rep.msk.mts.ru:80/artifactory/common-rpm/oracle-instantclient-basic-21.3.0.0.0-1.el8.x86_64.rpm \
    && yum -y install /tmp/oracle-instantclient-basic-21.3.0.0.0-1.el8.x86_64.rpm \
    && yum -y install \
        java \
        unixODBC-devel \
        # hive
        libsasl2-modules\
        libsasl2-dev\
    && yum clean all \
    && rm -rf /tmp/* /var/tmp/* /root/.cache/* \
    && mkdir /opt/project/ \
    && useradd -ms /bin/bash -d ${ONETL_USER_HOME} onetl \
    && chown -R onetl:onetl ${ONETL_USER_HOME} \
    && chown -R onetl:onetl /opt/project/

COPY ./requirements.txt \
     ./requirements-test.txt \
     ./wait-for-it.sh \
     ./docker/entrypoint.sh /

RUN chmod +x /entrypoint.sh /wait-for-it.sh

ARG BUILD_NUMBER
ARG BRANCH_NAME

RUN pip install -r /requirements-test.txt

USER onetl

COPY . /opt/project/

WORKDIR /opt/project/

# Build /opt/build_dir/dist/oneloader*.whl
RUN python3 setup.py bdist_wheel

ENTRYPOINT ["/entrypoint.sh"]