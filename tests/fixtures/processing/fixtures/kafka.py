import secrets

import pytest


@pytest.fixture
def kafka_topic(processing, request):
    topic = secrets.token_hex(6)
    processing.create_topic(topic, num_partitions=1)

    def delete_topic():
        processing.delete_topic([topic])

    request.addfinalizer(delete_topic)
    return topic


@pytest.fixture
def kafka_dataframe_schema():
    from pyspark.sql.types import (
        FloatType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("id_int", LongType(), nullable=True),
            StructField("text_string", StringType(), nullable=True),
            StructField("hwm_int", LongType(), nullable=True),
            StructField("float_value", FloatType(), nullable=True),
        ],
    )
