import os
import logging

# noinspection PyPackageRequirements
import pytest

# noinspection PyPackageRequirements
from pyhive import hive
from hdfs import InsecureClient
from mtspark import get_spark

LOG = logging.getLogger(__name__)

# ****************************Environment Variables*****************************
SELF_PATH = os.path.abspath(os.path.dirname(__file__))

HDFS_PORT = "50070"
DOCKER_HOST = "host.docker.internal"


def debug_executor_change_state_crutch(self, key, state):
    self.log.debug(f"Popping {key} from executor task queue.")
    self.running.pop(key, None)
    self.event_buffer[key] = state


@pytest.fixture(scope="session", name="spark")
def get_mtspark_session():
    spark = get_spark(
        config={
            "appName": "onetl",
            "spark.jars.packages": True,
        },
        fix_pyspark=False,
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def hive_client():
    client = hive.connect(host="hive2")
    yield client
    client.close()


@pytest.fixture(scope="session")
def hdfs_client():
    client = InsecureClient(f"http://{DOCKER_HOST}:{HDFS_PORT}")  # NOSONAR
    yield client


if __name__ == "__main__":
    pytest.main(
        [
            "--verbose",
            "-s",
            "-c",
            "pytest.ini",
            os.environ.get("X-TEST-TARGET", "tests"),
        ],
    )
