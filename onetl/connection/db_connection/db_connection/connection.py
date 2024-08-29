# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

try:
    from pydantic.v1 import Field, validator
except (ImportError, AttributeError):
    from pydantic import Field, validator  # type: ignore[no-redef, assignment]

from onetl._util.spark import try_import_pyspark
from onetl.base import BaseDBConnection
from onetl.connection.db_connection.db_connection.dialect import DBDialect
from onetl.impl import FrozenModel
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = getLogger(__name__)


class DBConnection(BaseDBConnection, FrozenModel):
    spark: SparkSession = Field(repr=False)

    Dialect = DBDialect

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        try_import_pyspark()

        from pyspark.sql import SparkSession  # noqa: WPS442

        # avoid importing pyspark unless user called the constructor,
        # as we allow user to use `Connection.get_packages()` for creating Spark session
        refs = super()._forward_refs()
        refs["SparkSession"] = SparkSession
        return refs

    @validator("spark")
    def _check_spark_session_alive(cls, spark):
        # https://stackoverflow.com/a/36044685
        msg = "Spark session is stopped. Please recreate Spark session."
        try:
            if not spark._jsc.sc().isStopped():
                return spark
        except Exception as e:
            # None has no attribute "something"
            raise ValueError(msg) from e

        raise ValueError(msg)

    def _log_parameters(self):
        log.info("|%s| Using connection parameters:", self.__class__.__name__)
        parameters = self.dict(exclude_none=True, exclude={"spark"})
        for attr, value in parameters.items():
            log_with_indent(log, "%s = %r", attr, value)
