# -*- coding: utf-8 -*-
import os
import sysconfig
import logging

# noinspection PyPackageRequirements
import pytest
import pyspark

# noinspection PyPackageRequirements
from pyhive import hive
from hdfs import InsecureClient

LOG = logging.getLogger(__name__)

# ****************************Environment Variables*****************************
SELF_PATH = os.path.abspath(os.path.dirname(__file__))

HDFS_PORT = '50070'
DOCKER_HOST = 'host.docker.internal'


def debug_executor_change_state_crutch(self, key, state):
    self.log.debug(f'Popping {key} from executor task queue.')
    self.running.pop(key, None)
    self.event_buffer[key] = state


@pytest.fixture(scope='session', name='spark_conf')
def setup_spark_conf():
    # FIXME: the conf leaks
    jars = ','.join((
        (
            'http://rep.msk.mts.ru/artifactory/SBT-maven.org-cache/org/postgresql/'
            'postgresql/42.2.4/postgresql-42.2.4.jar'
        ),
        (
            'http://rep.msk.mts.ru/artifactory/jcenter/ru/yandex/clickhouse/clickhouse-jdbc/0.2.4/'
            'clickhouse-jdbc-0.2.4.jar'
        ),
        (
            'http://rep.msk.mts.ru:80/artifactory/files/oracle/ojdbc7.jar'
        ),
    ))
    conf = pyspark.SparkConf().setAppName('onetl').setMaster('local[*]')
    conf.set('spark.jars', jars)
    return conf


@pytest.fixture(scope='session')
def hive_client():
    client = hive.connect(host='hive2')
    yield client
    client.close()


@pytest.fixture(scope='session')
def hdfs_client():
    client = InsecureClient(f'http://{DOCKER_HOST}:{HDFS_PORT}')
    yield client


if __name__ == '__main__':
    pytest.main([
        '--verbose',
        '-s',
        '-c',
        'pytest.ini',
        os.environ.get('X-TEST-TARGET', 'tests'),
    ])
