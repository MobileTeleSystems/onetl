# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from etl_entities.hwm import HWM, KeyValueIntHWM

from onetl._util.spark import get_spark_version
from onetl.connection.db_connection.db_connection.dialect import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    NotSupportColumns,
    NotSupportDFSchema,
    NotSupportHint,
    NotSupportWhere,
)

if TYPE_CHECKING:
    from pyspark.sql.types import StructField

log = logging.getLogger(__name__)


class KafkaDialect(  # noqa: WPS215
    NotSupportColumns,
    NotSupportDFSchema,
    NotSupportHint,
    NotSupportWhere,
    DBDialect,
):
    SUPPORTED_HWM_COLUMNS = {"offset"}

    def validate_name(self, value: str) -> str:
        if "*" in value or "," in value:
            raise ValueError(
                f"source/target={value} is not supported by {self.connection.__class__.__name__}. "
                f"Provide a singular topic.",
            )
        return value

    def validate_hwm(
        self,
        hwm: HWM | None,
    ) -> HWM | None:
        if not hwm:
            return None

        if hwm.expression not in self.SUPPORTED_HWM_COLUMNS:
            raise ValueError(
                f"hwm.expression={hwm.expression!r} is not supported by {self.connection.__class__.__name__}. "
                f"Valid values are: {self.SUPPORTED_HWM_COLUMNS}",
            )

        if hwm.expression == "timestamp":
            # Spark version less 3.x does not support reading from Kafka with the timestamp parameter
            spark_version = get_spark_version(self.connection.spark)  # type: ignore[attr-defined]
            if spark_version.major < 3:
                raise ValueError(
                    f"Spark version must be 3.x for the timestamp column. Current version is: {spark_version}",
                )
        return hwm

    def detect_hwm_class(self, field: StructField) -> type[KeyValueIntHWM] | None:
        kafka_field_to_hwm_class = {
            "offset": KeyValueIntHWM,
            # add "timestamp" in future
        }
        return kafka_field_to_hwm_class.get(field.name)
