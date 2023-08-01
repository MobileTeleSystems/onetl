from __future__ import annotations

import os
from logging import getLogger
from typing import TYPE_CHECKING

import pandas

from tests.fixtures.processing.base_processing import BaseProcessing

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql.types import StructType


logger = getLogger(__name__)


class KafkaProcessing(BaseProcessing):
    column_names: list[str] = ["id_int", "text_string", "hwm_int", "float_value"]

    @property
    def producer(self):
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": f"{self.host}:{self.port}"})

    @property
    def admin(self):
        from confluent_kafka.admin import AdminClient

        return AdminClient({"bootstrap.servers": f"{self.host}:{self.port}"})

    @property
    def user(self) -> str:
        return os.environ["ONETL_KAFKA_USER"]

    @property
    def password(self) -> str:
        return os.environ["ONETL_KAFKA_PASSWORD"]

    @property
    def host(self) -> str:
        return os.environ["ONETL_KAFKA_HOST"]

    @property
    def port(self) -> int:
        return int(os.environ["ONETL_KAFKA_PORT"])

    def create_schema(self, schema: str) -> None:
        pass

    def create_table(self, table: str, fields: dict[str, str], schema: str) -> None:
        pass

    def drop_database(self, schema: str) -> None:
        pass

    def drop_table(self, table: str, schema: str) -> None:
        pass

    @staticmethod
    def delivery_report(err, msg):
        """Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
        if err is not None:
            logger.debug("Message delivery failed: %s", err)
        else:
            pass

    def send_message(self, topic, message):
        producer = self.producer
        producer.produce(topic, message, callback=self.delivery_report)
        producer.flush()

    def read_data_earliest(self, topic, num_messages=1, timeout=1.0):
        from confluent_kafka import Consumer, KafkaException

        conf = {
            "bootstrap.servers": f"{self.host}:{self.port}",
            "group.id": "mygroup",
            "auto.offset.reset": "earliest",
        }

        consumer = Consumer(conf)
        consumer.subscribe([topic])

        messages = consumer.consume(num_messages=num_messages, timeout=timeout)

        result = []
        for msg in messages:
            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key()
            value = msg.value()

            result.append((key.decode("utf-8"), value.decode("utf-8")))

        consumer.close()
        return result

    def insert_data(self, schema: str, table: str, values: list) -> None:
        pass

    def delete_topic(self, topics: list[str]):
        admin = self.admin
        # https://github.com/confluentinc/confluent-kafka-python/issues/813
        admin.delete_topics(topics, request_timeout=3)

    def get_expected_dataframe(  # noqa: WPS463
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        pass

    def json_serialize(
        self,
        df: SparkDataFrame,
        df_schema: StructType,
    ) -> SparkDataFrame:
        """Serializes dataframe to JSON"""
        from pyspark.sql.functions import col, from_json

        df = df.select(
            from_json(col=col("value").cast("string"), schema=df_schema).alias("value"),
        ).select("value.*")

        return df  # noqa:  WPS331
