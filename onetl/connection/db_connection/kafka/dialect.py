from __future__ import annotations

import logging

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsNone,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportTableWithoutDBSchema,
    SupportWhereNone,
)

log = logging.getLogger(__name__)


class KafkaDialect(  # noqa: WPS215
    SupportColumnsNone,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportWhereNone,
    SupportTableWithoutDBSchema,
    DBConnection.Dialect,
):
    pass
