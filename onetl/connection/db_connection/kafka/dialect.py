# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from etl_entities.hwm import HWM, KeyValueIntHWM

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
        return hwm

    def detect_hwm_class(self, field: StructField) -> type[KeyValueIntHWM] | None:
        kafka_field_to_hwm_class = {
            "offset": KeyValueIntHWM,
            # add "timestamp" in future
        }
        return kafka_field_to_hwm_class.get(field.name)
