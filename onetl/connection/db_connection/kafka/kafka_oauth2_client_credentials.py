# SPDX-FileCopyrightText: 2026-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, Annotated

from pydantic import Field, HttpUrl, SecretStr, UrlConstraints

from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from onetl.connection.db_connection.kafka.connection import Kafka


class KafkaOAuth2ClientCredentials(KafkaAuth, GenericOptions):
    """Connect to Kafka using OAuth 2.0 Client Credentials Flow and `sasl.mechanism="OAUTHBEARER"`.

    The Kafka client fetches access tokens from the OAuth2 server and refreshes them automatically.

    For more details see [Kafka Documentation](https://kafka.apache.org/documentation/#security_sasl_oauthbearer).

    !!! success "Added in 0.17.0"

    !!! warning

        This authentication method requires Spark 3.4 or higher.

    Parameters
    ----------
    client_id : str
        OAuth2 client ID.

    client_secret : str
        OAuth2 client secret.

    oauth2_token_endpoint : str
        OAuth2 endpoint used to fetch access tokens.

    scopes : list[str], default: []
        OAuth2 scopes to request.

    Examples
    --------

    ```python
    from onetl.connection import Kafka

    auth = Kafka.OAuth2ClientCredentials(
        client_id="my-client",
        client_secret="my-secret",
        oauth2_token_endpoint="https://keycloak.example.com/realms/my-realm/protocol/openid-connect/token",
        scopes=["kafka"],
    )
    ```
    """

    client_id: str
    client_secret: SecretStr
    oauth2_token_endpoint: Annotated[HttpUrl, UrlConstraints(host_required=True, preserve_empty_path=True)]
    scopes: list[str] = Field(default_factory=list)

    @staticmethod
    def _escape_jaas_value(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def get_jaas_conf(self) -> str:
        client_id = self._escape_jaas_value(self.client_id)
        client_secret = self._escape_jaas_value(self.client_secret.get_secret_value())
        options = [
            f'clientId="{client_id}"',
            f'clientSecret="{client_secret}"',
        ]

        if self.scopes:
            scope = self._escape_jaas_value(" ".join(self.scopes))
            options.append(f'scope="{scope}"')

        return "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " + " ".join(options) + ";"

    def get_options(self, kafka: "Kafka") -> dict:
        spark_version = get_spark_version(kafka.spark)
        if spark_version < Version("3.4"):
            msg = f"Kafka OAuth2 Client Credentials authentication requires Spark 3.4 or higher, got {spark_version}."
            raise ValueError(msg)

        if spark_version < Version("3.5"):
            callback_handler = "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler"
        else:
            callback_handler = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"

        return {
            "sasl.mechanism": "OAUTHBEARER",
            "sasl.login.callback.handler.class": callback_handler,
            "sasl.oauthbearer.token.endpoint.url": str(self.oauth2_token_endpoint),
            "sasl.jaas.config": self.get_jaas_conf(),
        }

    def cleanup(self, kafka: "Kafka") -> None:
        # nothing to cleanup
        pass
