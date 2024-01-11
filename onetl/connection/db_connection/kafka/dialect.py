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
from typing import TYPE_CHECKING

from etl_entities.hwm import HWM, KeyValueIntHWM

from onetl._util.spark import get_spark_version
from onetl.connection.db_connection.db_connection.dialect import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    NotSupportColumns,
    NotSupportDFSchema,
    NotSupportHint,
    NotSupportWhere,
    SupportNameAny,
)

if TYPE_CHECKING:
    from pyspark.sql.types import StructField

log = logging.getLogger(__name__)


class KafkaDialect(  # noqa: WPS215
    NotSupportColumns,
    NotSupportDFSchema,
    NotSupportHint,
    NotSupportWhere,
    SupportNameAny,
    DBDialect,
):
    SUPPORTED_HWM_COLUMNS = {"offset"}

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
