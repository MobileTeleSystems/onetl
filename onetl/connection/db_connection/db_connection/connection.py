# SPDX-FileCopyrightText: 2021-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from logging import getLogger
from typing import TYPE_CHECKING, ClassVar

from pydantic import Field, field_validator

from onetl._util.spark import try_import_pyspark
from onetl.base import BaseDBConnection
from onetl.connection.db_connection.db_connection.dialect import DBDialect
from onetl.impl import FrozenModel
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = getLogger(__name__)


class DBConnection(BaseDBConnection, FrozenModel):
    spark: "SparkSession" = Field(repr=False)

    Dialect: ClassVar = DBDialect

    def __new__(cls, *args, **kwargs):
        try_import_pyspark()

        from pyspark.sql import SparkSession

        _ = SparkSession

        cls.model_rebuild()
        return super().__new__(cls)

    @field_validator("spark", mode="before")
    @classmethod
    def _check_spark_session_alive(cls, spark):
        # https://stackoverflow.com/a/36044685
        msg = "Spark session is stopped. Please recreate Spark session."
        try:
            if not spark._jsc.sc().isStopped():  # noqa: SLF001
                return spark
        except Exception as e:
            # None has no attribute "something"
            raise ValueError(msg) from e

        raise ValueError(msg)

    def _log_parameters(self):
        log.info("|%s| Using connection parameters:", self.__class__.__name__)
        parameters = self.model_dump(exclude_none=True, exclude={"spark"})
        for attr, value in parameters.items():
            log_with_indent(log, "%s = %r", attr, value)
