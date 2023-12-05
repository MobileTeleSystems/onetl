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

import json
import operator
from datetime import datetime
from typing import Any, Callable, ClassVar, Dict, Iterable, Mapping

from etl_entities.hwm import HWM

from onetl.connection.db_connection.db_connection.dialect import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    NotSupportColumns,
    RequiresDFSchema,
    SupportNameAny,
)

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
    DBDialect,
):
    _compare_statements: ClassVar[Dict[Callable, str]] = {
        operator.ge: "$gte",
        operator.gt: "$gt",
        operator.le: "$lte",
        operator.lt: "$lt",
        operator.eq: "$eq",
        operator.ne: "$ne",
    }

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

    def validate_hwm(self, hwm: HWM | None) -> HWM | None:
        if hwm and hwm.expression is not None:
            raise ValueError(
                f"'hwm.expression' parameter is not supported by {self.connection.__class__.__name__}",
            )
        return hwm

    def prepare_pipeline(
        self,
        pipeline: Any,
    ) -> Any:
        """
        Prepares pipeline (list or dict) to MongoDB syntax, but without converting it to string.
        """

        if isinstance(pipeline, datetime):
            return {"$date": pipeline.astimezone().isoformat()}

        if isinstance(pipeline, Mapping):
            return {self.prepare_pipeline(key): self.prepare_pipeline(value) for key, value in pipeline.items()}

        if isinstance(pipeline, Iterable) and not isinstance(pipeline, str):
            return [self.prepare_pipeline(item) for item in pipeline]

        return pipeline

    def convert_to_str(
        self,
        value: Any,
    ) -> str:
        """
        Converts the given dictionary, list or primitive to a string.
        """

        return json.dumps(self.prepare_pipeline(value))

    def _merge_conditions(self, conditions: list[Any]) -> Any:
        if len(conditions) == 1:
            return conditions[0]

        return {"$and": conditions}

    def _get_compare_statement(self, comparator: Callable, arg1: Any, arg2: Any) -> dict:
        """
        Returns the comparison statement in MongoDB syntax:

        .. code::

            {
                "field": {
                    "$gt": "some_value",
                }
            }
        """
        return {
            arg1: {
                self._compare_statements[comparator]: arg2,
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
