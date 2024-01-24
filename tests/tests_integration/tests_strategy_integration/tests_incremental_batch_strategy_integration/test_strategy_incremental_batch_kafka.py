import re
import secrets

import pytest

from onetl.connection import Kafka
from onetl.db import DBReader
from onetl.strategy import IncrementalBatchStrategy, SnapshotBatchStrategy

pytestmark = [pytest.mark.kafka, pytest.mark.db_connection, pytest.mark.connection]


@pytest.mark.parametrize(
    "strategy",
    [
        IncrementalBatchStrategy,
        SnapshotBatchStrategy,
    ],
)
def test_strategy_kafka_with_batch_strategy_error(strategy, spark):
    from tests.fixtures.processing.kafka import KafkaProcessing

    processing = KafkaProcessing()

    with strategy(step=10) as batches:
        reader = DBReader(
            connection=Kafka(
                addresses=[f"{processing.host}:{processing.port}"],
                cluster="cluster",
                spark=spark,
            ),
            table="topic",
            hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="offset"),
        )
        # raises as at current version there is no way to distribute step size between kafka partitions
        with pytest.raises(TypeError, match=re.escape("unsupported operand type(s) for +")):
            for _ in batches:
                reader.run()
