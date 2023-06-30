#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, List, Optional

from etl_entities.instance import Cluster
from pydantic import SecretStr, root_validator, validator

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.hwm import Statement
from onetl.impl import LocalPath, path_repr

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


class Kafka(DBConnection):
    """
    This connector is designed to read and write from Kafka in batch mode.

    Based on `official Kafka 0.10+ Source For Spark
    <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_.

    .. note::

        This connector is for batch download from kafka and not streaming.

    .. dropdown:: Version compatibility

        * Apache Kafka versions: 0.10 or higher
        * Spark versions: 2.3.x - 3.4.x

    Parameters
    ----------

    addresses : list[str]
        A list of broker addresses, for example ``[192.168.1.10:9092, 192.168.1.11:9092]``.
        The list cannot be empty.

    cluster : Cluster
        Cluster name. Used for HWM and lineage. A cluster field cannot be empty.

    user : str, default  ``None``
        User, which have proper access to the Apache Kafka. For example: ``some_user``.

    password : SecretStr, default  ``None``
        Password for Kafka connection.

    .. warning::

        When creating a connector, when specifying `user` parameter, either `password` or `keytab` can be specified. Or
        these parameters for anonymous connection are not specified at all.

    keytab : LocalPath, default  ``None``
        A path to the keytab file. A keytab is a file containing pairs of Kerberos principals and encrypted keys that
        are derived from the Kerberos password. You can use this file to log on to Kerberos without being prompted for
        a password.

    .. warning::

        When creating a connector, when specifying `user` parameter, either `password` or `keytab` can be specified. Or
        these parameters for anonymous connection are not specified at all.

    Examples
    --------

    .. code:: python

        kafka = Kafka(
            addresses=["mybroker:9020", "anotherbroker:9020"],
            cluster="mycluster",
            user="me",
            keytab="/etc/security/keytabs/me.keytab",
            spark=spark,
        )

    .. code:: python

        kafka = Kafka(
            addresses=["mybroker:9020", "anotherbroker:9020"],
            cluster="mycluster",
            user="me",
            password="pwd",
            spark=spark,
        )

    """

    def read_df(  # type: ignore
        self,
        source: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
    ) -> DataFrame:
        pass

    def write_df(self, df: DataFrame, target: str) -> None:
        pass

    addresses: List[str]
    cluster: Cluster
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    keytab: Optional[LocalPath] = None

    @classmethod
    def get_package_spark(
        cls,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        if not scala_version:
            scala_version = "2.11" if spark_version.startswith("2") else "2.12"
        return [f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"]

    @property
    def instance_url(self):
        return "kafka://" + self.cluster

    def check(self):
        ...

    def read_table(  # type: ignore
        self,
        table: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
    ) -> DataFrame:
        ...

    def save_df(self, df: DataFrame, table: str) -> None:
        ...

    def get_min_max_bounds(  # type: ignore
        self,
        table: str,
        column: str,
        expression: str | None = None,
        hint: Any | None = None,
        where: Any | None = None,
    ) -> tuple[Any, Any]:
        ...

    @validator("keytab")
    def _validate_keytab(cls, value):  # noqa: N805
        if not os.path.exists(value):
            raise ValueError(
                f"File '{os.fspath(value)}' is missing",
            )

        if not os.access(value, os.R_OK):
            raise ValueError(
                f"No access to file {path_repr(value)}",
            )

        return value

    @validator("addresses")
    def _validate_addresses(cls, value):  # noqa: N805
        if not value:
            raise ValueError("Passed empty parameter 'addresses'")
        return value

    @root_validator()
    def _validate_auth(cls, values: dict) -> dict:  # noqa: N805
        user = values.get("user", None)
        password = values.get("password", None)
        keytab = values.get("keytab", None)

        passed_user_and_keytab = user and keytab
        passed_user_and_pass = user and password

        if not user and not password and not keytab:
            # anonymous access
            return values

        if passed_user_and_keytab and not password:
            # valid credentials
            return values

        if passed_user_and_pass and not keytab:
            # valid credentials
            return values

        raise ValueError(
            "Please provide either `keytab` and `user`, or `password` and "
            "`user` for Kerberos auth, or none of parameters for anonymous auth",
        )
