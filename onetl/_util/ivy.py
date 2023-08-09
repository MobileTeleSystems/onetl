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

from typing import TYPE_CHECKING

from onetl._util.spark import get_spark_version

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def inject_ivy_package(
    spark_session: SparkSession,
    package: str,
    transitive: bool = True,
    exclude: list[str] | None = None,
) -> None:
    """
    Inject Maven package into Spark session

    Parameters
    ----------
    package : str
        Maven package name in format ``group:module:version``.

    transitive : bool, default: True
        Whether to include transitive dependencies.

    exclude : list[str] | None, default: None
        List of dependency packages to exclude

    Examples
    --------

    Inject package to Spark session:

    .. code:: python

        inject_ivy_package(spark, "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2")

    All options:

    .. code:: python

        inject_ivy_package(
            spark,
            "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2",
            transitive=True,
            exclude=["some:dependency:1.0.0"],
        )
    """
    spark_version = get_spark_version(spark_session)
    if spark_version < (3, 2):
        # https://issues.apache.org/jira/browse/SPARK-33084
        raise RuntimeError(f"Spark version must be at least 3.2, got {spark_version}")

    resource_loader = spark_session._jsparkSession.sessionState().resourceLoader()  # type: ignore
    url = f"ivy://{package}?transitive={str(transitive).lower()}"
    if exclude:
        url += f"&exclude={','.join(package)}"
    resource_loader.addJar(url)
