import secrets

import pytest

from onetl.connection import Kafka
from onetl.db import DBReader, DBWriter

pytestmark = [pytest.mark.kafka_oauth, pytest.mark.db_connection, pytest.mark.connection]


def test_kafka_oauth2_client_credentials_read_write(spark, processing):
    topic = f"oauth_{secrets.token_hex(5)}"
    kafka = Kafka(
        addresses=[f"{processing.oauth_host}:{processing.oauth_port}"],
        cluster="cluster",
        spark=spark,
        auth=Kafka.OAuth2ClientCredentials(
            client_id=processing.oauth_client_id,
            client_secret=processing.oauth_client_secret,
            oauth2_token_endpoint=processing.oauth_token_endpoint,
            scopes=processing.oauth_scopes,
        ),
    )
    source_df = spark.createDataFrame([(bytearray(b"oauth-message"),)], ["value"])
    writer = DBWriter(connection=kafka, target=topic)

    writer.run(source_df)

    reader = DBReader(connection=kafka, source=topic)
    result = reader.run().select("value").collect()
    assert [bytes(row.value) for row in result] == [b"oauth-message"]


def test_kafka_oauth2_client_credentials_invalid_secret(spark, processing):
    kafka = Kafka(
        addresses=[f"{processing.oauth_host}:{processing.oauth_port}"],
        cluster="cluster",
        spark=spark,
        auth=Kafka.OAuth2ClientCredentials(
            client_id=processing.oauth_client_id,
            client_secret="invalid-secret",
            oauth2_token_endpoint=processing.oauth_token_endpoint,
            scopes=processing.oauth_scopes,
        ),
    )

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        kafka.check()
