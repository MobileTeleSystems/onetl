# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping

from onetl.connection.db_connection.db_connection.dialect import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    NotSupportColumns,
    RequiresDFSchema,
    SupportHWMExpressionStr,
    SupportNameAny,
)
from onetl.hwm import Edge, Window

_upper_level_operators = frozenset(  # noqa: WPS527
    [
        "$addFields",
        "$bucket",
        "$bucketAuto",
        "$changeStream",
        "$collStats",
        "$count",
        "$currentOp",
        "$densify",
        "$documents",
        "$facet",
        "$fill",
        "$geoNear",
        "$graphLookup",
        "$group",
        "$indexStats",
        "$limit",
        "$listLocalSessions",
        "$listSessions",
        "$lookup",
        "$merge",
        "$out",
        "$planCacheStats",
        "$project",
        "$redact",
        "$replaceRoot",
        "$replaceWith",
        "$sample",
        "$search",
        "$searchMeta",
        "$set",
        "$setWindowFields",
        "$shardedDataDistribution",
        "$skip",
        "$sort",
        "$sortByCount",
        "$unionWith",
        "$unset",
        "$unwind",
    ],
)


class MongoDBDialect(  # noqa: WPS215
    SupportNameAny,
    NotSupportColumns,
    RequiresDFSchema,
    SupportHWMExpressionStr,
    DBDialect,
):
    def validate_where(
        self,
        where: Any,
    ) -> dict | None:
        if where is None:
            return None

        if not isinstance(where, dict):
            raise ValueError(
                f"{self.connection.__class__.__name__} requires 'where' parameter type to be 'dict', "
                f"got {where.__class__.__name__!r}",
            )

        for key in where:
            self._validate_top_level_keys_in_where_parameter(key)
        return where

    def validate_hint(
        self,
        hint: Any,
    ) -> dict | None:
        if hint is None:
            return None

        if not isinstance(hint, dict):
            raise ValueError(
                f"{self.connection.__class__.__name__} requires 'hint' parameter type to be 'dict', "
                f"got {hint.__class__.__name__!r}",
            )
        return hint

    def prepare_pipeline(
        self,
        pipeline: Any,
    ) -> Any:
        """
        Prepares pipeline (list or dict) to MongoDB syntax, but without converting it to string.
        """

        if isinstance(pipeline, Mapping):
            return {self.prepare_pipeline(key): self.prepare_pipeline(value) for key, value in pipeline.items()}

        if isinstance(pipeline, Iterable) and not isinstance(pipeline, str):
            return [self.prepare_pipeline(item) for item in pipeline]

        return self._serialize_value(pipeline)

    def apply_window(
        self,
        condition: Any,
        window: Window | None = None,
    ) -> Any:
        result = super().apply_window(condition, window)
        if not result:
            return {}
        if len(result) == 1:
            return result[0]
        return {"$and": result}

    def _serialize_value(self, value: Any) -> str | int | dict:
        """
        Transform the value into an SQL Dialect-supported form.
        """

        if isinstance(value, datetime):
            return {"$date": value.astimezone().isoformat()}

        return value

    def _edge_to_where(
        self,
        expression: str,
        edge: Edge,
        position: str,
    ) -> Any:
        if not expression or not edge.is_set():
            return None

        operators: dict[tuple[str, bool], str] = {
            ("start", True): "$gte",
            ("start", False): "$gt",
            ("end", True): "$lte",
            ("end", False): "$lt",
        }

        operator = operators[(position, edge.including)]
        value = self._serialize_value(edge.value)
        return {
            expression: {
                operator: value,
            },
        }

    def _validate_top_level_keys_in_where_parameter(self, key: str):
        """
        Checks the 'where' parameter for illegal operators, such as ``$match``, ``$merge`` or ``$changeStream``.

        'where' clause can contain only filtering operators, like ``{"col1" {"$eq": 1}}`` or ``{"$and": [...]}``.
        """
        if key.startswith("$"):
            if key == "$match":
                raise ValueError(
                    "'$match' operator not allowed at the top level of the 'where' parameter dictionary. "
                    "This error most likely occurred due to the fact that you used the MongoDB format for the "
                    "pipeline {'$match': {'column': ...}}. In the onETL paradigm, you do not need to specify the "
                    "'$match' keyword, but write the filtering condition right away, like {'column': ...}",
                )
            if key in _upper_level_operators:  # noqa: WPS220
                raise ValueError(  # noqa: WPS220
                    f"An invalid parameter {key!r} was specified in the 'where' "
                    "field. You cannot use aggregations or 'groupBy' clauses in 'where'",
                )
