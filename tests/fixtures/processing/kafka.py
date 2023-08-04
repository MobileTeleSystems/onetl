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

    def get_consumer(self):
        from confluent_kafka import Consumer

        return Consumer(
            {
                "bootstrap.servers": f"{self.host}:{self.port}",
                "group.id": "mygroup",
                "auto.offset.reset": "earliest",
            },
        )

    def get_producer(self):
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": f"{self.host}:{self.port}"})

    def get_admin_client(self):
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
        return int(os.environ["ONETL_KAFKA_PLAINTEXT_ANONYMOUS_PORT"])

    @property
    def basic_auth_port(self) -> int:
        return int(os.environ["ONETL_KAFKA_PLAINTEXT_BASIC_PORT"])

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
        producer = self.get_producer()
        producer.produce(topic, message, callback=self.delivery_report)
        producer.flush()

    def get_expected_df(self, topic: str, num_messages: int = 1, timeout: float = 1.0) -> pandas.DataFrame:
        from confluent_kafka import KafkaException

        consumer = self.get_consumer()

        consumer.subscribe([topic])

        messages = consumer.consume(num_messages=num_messages, timeout=timeout)

        result = []
        for msg in messages:
            if msg.error():
                raise KafkaException(msg.error())
            else:
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                partition = msg.partition()
                headers = msg.headers()
                result.append((key, value, partition, headers, topic))

        consumer.close()
        return pandas.DataFrame(result, columns=["key", "value", "partition", "headers", "topic"])

    def insert_data(self, schema: str, table: str, values: list) -> None:
        pass

    def delete_topic(self, topics: list[str]):
        admin = self.get_admin_client()
        # https://github.com/confluentinc/confluent-kafka-python/issues/813
        admin.delete_topics(topics, request_timeout=5)

    def topic_exists(self, topic: str) -> bool:
        admin = self.get_admin_client()
        topic_metadata = admin.list_topics(timeout=5)
        return topic in topic_metadata.topics

    def get_num_partitions(self, topic: str) -> int:
        admin = self.get_admin_client()
        metadata = admin.list_topics(topic, timeout=5)
        topic_metadata = metadata.topics[topic]

        # Return the number of partitions
        return len(topic_metadata.partitions)

    def get_expected_dataframe(  # noqa: WPS463
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        pass

    def json_deserialize(
        self,
        df: SparkDataFrame,
        df_schema: StructType,
    ) -> SparkDataFrame:
        """Deserializes dataframe's "value" column from JSON to struct"""
        from pyspark.sql.functions import col, from_json

        df = df.select(
            from_json(col=col("value").cast("string"), schema=df_schema).alias("value"),
        ).select("value.*")

        return df  # noqa:  WPS331

    def json_serialize(self, df: SparkDataFrame) -> SparkDataFrame:
        """Serializes dataframe's columns into JSON "value" field"""
        from pyspark.sql.functions import col, struct, to_json

        df = df.select(struct(*df.columns).alias("value"))
        df = df.select(to_json(col("value")).alias("value"))

        return df  # noqa:  WPS331
