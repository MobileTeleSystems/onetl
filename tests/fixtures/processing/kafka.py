from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, ClassVar

import pandas

from tests.fixtures.processing.base_processing import BaseProcessing

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql.types import StructType

DEFAULT_TIMEOUT = 10.0


class KafkaProcessing(BaseProcessing):
    column_names: ClassVar[list[str]] = ["id_int", "text_string", "hwm_int", "float_value"]

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        return False

    @property
    def schema(self) -> str:
        # Kafka does not support schemas
        return ""

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
    def sasl_port(self) -> int:
        return int(os.environ["ONETL_KAFKA_PLAINTEXT_SASL_PORT"])

    @property
    def oauth_host(self) -> str:
        return os.environ["ONETL_KAFKA_OAUTH_HOST"]

    @property
    def oauth_port(self) -> int:
        return int(os.environ["ONETL_KAFKA_OAUTH_PORT"])

    @property
    def oauth_client_id(self) -> str:
        return os.environ["ONETL_KAFKA_OAUTH_CLIENT_ID"]

    @property
    def oauth_client_secret(self) -> str:
        return os.environ["ONETL_KAFKA_OAUTH_CLIENT_SECRET"]

    @property
    def oauth_token_endpoint(self) -> str:
        return os.environ["ONETL_KAFKA_OAUTH_TOKEN_ENDPOINT"]

    @property
    def oauth_scopes(self) -> list[str]:
        return os.environ["ONETL_KAFKA_OAUTH_SCOPE"].split()

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
        from confluent_kafka import KafkaException

        if err is not None:
            msg = f"Message {msg} delivery failed: {err}"
            raise KafkaException(msg)

    def send_message(self, topic, message, timeout: float = DEFAULT_TIMEOUT):
        from confluent_kafka import KafkaException

        producer = self.get_producer()
        producer.produce(topic, message, callback=self.delivery_report)
        messages_left = producer.flush(timeout)
        if messages_left:
            msg = f"{messages_left} messages were not delivered"
            raise KafkaException(msg)

    def get_expected_df(self, topic: str, num_messages: int = 1, timeout: float = DEFAULT_TIMEOUT) -> pandas.DataFrame:
        from confluent_kafka import KafkaException

        consumer = self.get_consumer()
        consumer.list_topics(timeout=timeout)  # if Kafka is not accessible, raise exception
        consumer.subscribe([topic])
        messages = consumer.consume(num_messages=num_messages, timeout=timeout)

        result = []
        for msg in messages:
            if msg.error():
                raise KafkaException(msg.error())
            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8") if msg.value() else None
            partition = msg.partition()
            headers = msg.headers()
            result.append((key, value, partition, headers, topic))

        consumer.close()
        return pandas.DataFrame(result, columns=["key", "value", "partition", "headers", "topic"])

    def insert_data(self, schema: str, table: str, values: list) -> None:
        pass

    def change_topic_partitions(self, topic: str, num_partitions: int, timeout: float = DEFAULT_TIMEOUT):
        from confluent_kafka.admin import NewPartitions

        admin_client = self.get_admin_client()

        if not self.topic_exists(topic):
            self.create_topic(topic, num_partitions)
        else:
            new_partitions = [NewPartitions(topic, num_partitions)]
            # change the number of partitions
            fs = admin_client.create_partitions(new_partitions, request_timeout=timeout)

            for topic_name, f in fs.items():
                try:
                    f.result()
                except Exception as e:
                    msg = f"Failed to update number of partitions for topic '{topic_name}': {e}"
                    raise RuntimeError(msg) from e

    def create_topic(self, topic: str, num_partitions: int, timeout: float = DEFAULT_TIMEOUT):
        from confluent_kafka.admin import KafkaException, NewTopic

        admin_client = self.get_admin_client()
        topic_config = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        fs = admin_client.create_topics([topic_config], request_timeout=timeout)

        for topic_name, f in fs.items():
            try:
                f.result()
            except Exception as e:
                msg = f"Error creating topic '{topic_name}': {e}"
                raise KafkaException(msg) from e

    def delete_topic(self, topics: list[str], timeout: float = DEFAULT_TIMEOUT):
        admin = self.get_admin_client()
        # https://github.com/confluentinc/confluent-kafka-python/issues/813
        admin.delete_topics(topics, request_timeout=timeout)

    def insert_pandas_df_into_topic(self, df: pandas.DataFrame, topic: str):
        for _, row in df.iterrows():
            message = json.dumps(row.to_dict())
            self.send_message(topic, message.encode("utf-8"))

    def topic_exists(self, topic: str, timeout: float = DEFAULT_TIMEOUT) -> bool:
        admin = self.get_admin_client()
        topic_metadata = admin.list_topics(timeout=timeout)
        return topic in topic_metadata.topics

    def get_num_partitions(self, topic: str, timeout: float = DEFAULT_TIMEOUT) -> int:
        admin = self.get_admin_client()
        metadata = admin.list_topics(topic, timeout=timeout)
        topic_metadata = metadata.topics[topic]

        # Return the number of partitions
        return len(topic_metadata.partitions)

    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        raise NotImplementedError

    def fix_pandas_df(
        self,
        df: pandas.DataFrame,
    ) -> pandas.DataFrame:
        df = super().fix_pandas_df(df)

        for column in df.columns:
            if "headers" in column:

                def convert(headers):
                    if not headers:
                        return headers

                    # Spark 4.2 df.collect() uses pyarrow which convert tuples to dicts
                    result = []
                    for header in headers:
                        if isinstance(header, dict):
                            result.append((header["key"], header["value"]))
                        else:
                            result.append(header)
                    return result

                df[column] = df[column].apply(convert)

        return df

    def json_deserialize(
        self,
        df: SparkDataFrame,
        df_schema: StructType,
    ) -> SparkDataFrame:
        """Deserializes dataframe's "value" column from JSON to struct"""
        from pyspark.sql.functions import col, from_json

        return df.select(
            from_json(col=col("value").cast("string"), schema=df_schema).alias("value"),
        ).select("value.*")

    def json_serialize(self, df: SparkDataFrame) -> SparkDataFrame:
        """Serializes dataframe's columns into JSON "value" field"""
        from pyspark.sql.functions import col, struct, to_json

        intermediate_df = df.select(struct(*df.columns).alias("value"))
        return intermediate_df.select(to_json(col("value")).alias("value"))
