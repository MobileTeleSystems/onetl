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

PARTITION_OVERWRITE_MODE_PARAM = "spark.sql.sources.partitionOverwriteMode"
log = logging.getLogger(__name__)


class Kafka(DBConnection):
    """
    This connector is designed to read and write from Kafka in batch mode.

    Based on Maven package ``spark-sql-kafka-0-10_2.11-2.3.0.jar``
    (`official Kafka 0.10+ Source For Structured Streaming
    driver <https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.3.0/spark-sql-kafka-0-10_2.11-2.3.0.jar>`_).

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

    def read_df(
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
    ) -> str:
        if not scala_version:
            scala_version = "2.11" if spark_version.startswith("2") else "2.12"
        return f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

    @validator("keytab")
    def validate_keytab(cls, value):  # noqa: N805, U100
        if not os.path.exists(value):
            raise ValueError(
                f"The file does not exists. File  properties:  {path_repr(value)} ",
            )

        if not os.access(value, os.R_OK):
            raise ValueError(
                f"Keytab file permission denied. File properties: {path_repr(value)}",
            )

        log.info(
            "The keytab file exists and the user has read permissions",
        )

        return value

    @validator("addresses")
    def validate_addresses(cls, value):  # noqa: N805, U100
        if not value:
            raise ValueError("Passed empty parameter 'addresses'")
        return value

    @root_validator()  # noqa: WPS231
    def validate_auth(cls, values: dict) -> dict:  # type: ignore # noqa: N805, U100
        user = values.get("user", None)
        password = values.get("password", None)
        keytab = values.get("keytab", None)

        if user is None and password is None and keytab is None:
            return values

        passed_pass_and_user = user is not None and password is not None
        passed_user_pass_keytab = keytab is not None and passed_pass_and_user
        passed_keytab_and_user = keytab is not None and user is not None

        if passed_user_pass_keytab:
            raise ValueError(
                "If you passed the `user` parameter please provide either `keytab` or `password` for auth, "
                "not both. Or do not specify `user`, "
                "`keytab` and `password` parameters for anonymous authorization.",
            )

        if passed_pass_and_user or passed_keytab_and_user:
            return values

        if user is None and password is not None and keytab is not None:
            raise ValueError(
                "`user` parameter not passed. Passed `password` and `keytab` parameters. Passing either `password` or "
                "`keytab` is allowed.",
            )

        if password is not None and user is None:
            raise ValueError("Passed `password` without `user` parameter.")

        if keytab is not None and user is None:
            raise ValueError("Passed `keytab` without `user` parameter.")

        if user is not None and password is None and keytab is None:
            raise ValueError("Passed only `user` parameter without `password` or `keytab`.")

    @property
    def instance_url(self):
        return "kafka://" + self.cluster

    def check(self):
        ...

    def read_table(  # type: ignore
        self,
        table: str,  # noqa: U100
        columns: list[str] | None = None,  # noqa: U100
        hint: Any | None = None,  # noqa: U100
        where: Any | None = None,  # noqa: U100
        df_schema: StructType | None = None,  # noqa: U100
        start_from: Statement | None = None,  # noqa: U100
        end_at: Statement | None = None,  # noqa: U100
    ) -> DataFrame:
        ...

    def save_df(self, df: DataFrame, table: str) -> None:  # noqa: U100
        ...

    def get_min_max_bounds(  # type: ignore
        self,
        table: str,  # noqa: U100
        column: str,  # noqa: U100
        expression: str | None = None,  # noqa: U100
        hint: Any | None = None,  # noqa: U100
        where: Any | None = None,  # noqa: U100
    ) -> tuple[Any, Any]:
        ...
