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
from typing import TYPE_CHECKING, Any, ClassVar, List, Optional

from etl_entities.instance import Cluster
from pydantic import SecretStr, root_validator, validator

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.hwm import Statement
from onetl.impl import LocalPath

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

PARTITION_OVERWRITE_MODE_PARAM = "spark.sql.sources.partitionOverwriteMode"
log = logging.getLogger(__name__)


class Kafka(DBConnection):
    """
    This connector is designed to read and write from Kafka in batch mode.

    .. dropdown:: Version compatibility

        * Apache Kafka versions: 0.10 or higher
        * Spark versions: 2.0.x - 3.3.x

    Parameters
    ----------

    addresses : list[str]
        A list of broker addresses, for example [192.168.1.10:9092, 192.168.1.11:9092].
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

    package_spark_2_0_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.0.2"  # noqa: WPS114
    package_spark_2_1_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0"  # noqa: WPS114
    package_spark_2_1_1: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1"  # noqa: WPS114
    package_spark_2_1_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.2"  # noqa: WPS114
    package_spark_2_1_3: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.3"  # noqa: WPS114
    package_spark_2_2_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0"  # noqa: WPS114
    package_spark_2_2_1: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1"  # noqa: WPS114
    package_spark_2_2_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.2"  # noqa: WPS114
    package_spark_2_2_3: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.3"  # noqa: WPS114
    package_spark_2_3_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0"  # noqa: WPS114
    package_spark_2_3_1: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1"  # noqa: WPS114
    package_spark_2_3_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2"  # noqa: WPS114
    package_spark_2_3_3: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3"  # noqa: WPS114
    package_spark_2_3_4: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4"  # noqa: WPS114
    package_spark_2_4_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0"  # noqa: WPS114
    package_spark_2_4_1: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1"  # noqa: WPS114
    package_spark_2_4_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.2"  # noqa: WPS114
    package_spark_2_4_3: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3"  # noqa: WPS114
    package_spark_2_4_4: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4"  # noqa: WPS114
    package_spark_2_4_5: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5"  # noqa: WPS114
    package_spark_2_4_6: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6"  # noqa: WPS114
    package_spark_2_4_7: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7"  # noqa: WPS114
    package_spark_3_0_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0"  # noqa: WPS114
    package_spark_3_0_1: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"  # noqa: WPS114
    package_spark_3_0_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2"  # noqa: WPS114
    package_spark_3_0_3: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3"  # noqa: WPS114
    package_spark_3_1_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0"  # noqa: WPS114
    package_spark_3_1_1: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1"  # noqa: WPS114
    package_spark_3_1_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"  # noqa: WPS114
    package_spark_3_1_3: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3"  # noqa: WPS114
    package_spark_3_2_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0"  # noqa: WPS114
    package_spark_3_2_1: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1"  # noqa: WPS114
    package_spark_3_2_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2"  # noqa: WPS114
    package_spark_3_2_3: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3"  # noqa: WPS114
    package_spark_3_2_4: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4"  # noqa: WPS114
    package_spark_3_3_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"  # noqa: WPS114
    package_spark_3_3_1: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"  # noqa: WPS114
    package_spark_3_3_2: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"  # noqa: WPS114
    package_spark_3_4_0: ClassVar[str] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"  # noqa: WPS114

    addresses: List[str]
    cluster: Cluster
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    keytab: Optional[LocalPath] = None

    @validator("keytab")
    def validate_keytab(cls, value):  # noqa: N805, U100
        if os.path.exists(value) and os.access(value, os.R_OK):
            log.info("The file exists and the user has read permissions")
        else:
            raise ValueError("The file does not exist or the user does not have read permissions")
        return value

    @validator("addresses")
    def validate_addresses(cls, value):  # noqa: N805, U100
        if not value:
            raise ValueError("Passed empty parameter 'addresses'")
        return value

    @root_validator()
    def validate_auth(cls, values: dict) -> dict:  # noqa: N805, U100
        user = values.get("user", None)
        password = values.get("password", None)
        keytab = values.get("keytab", None)

        passed_pass_and_user = user is not None and password is not None
        passed_user_pass_keytab = keytab is not None and passed_pass_and_user
        passed_keytab_and_user = keytab is not None and user is not None

        if passed_pass_and_user and passed_user_pass_keytab:
            raise ValueError("Authorization parameters passed at the same time, only two must be specified")

        if passed_pass_and_user or passed_keytab_and_user:
            return values

        raise ValueError("Invalid parameters user, password or keytab passed.")

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
