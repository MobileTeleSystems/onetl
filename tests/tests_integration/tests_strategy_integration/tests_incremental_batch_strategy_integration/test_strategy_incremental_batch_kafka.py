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

    with strategy(step=10):
        reader = DBReader(
            connection=Kafka(
                addresses=[f"{processing.host}:{processing.port}"],
                cluster="cluster",
                spark=spark,
            ),
            table="topic",
            hwm_column="offset",
        )
        with pytest.raises(ValueError, match="connection does not support batch strategies"):
            reader.run()
