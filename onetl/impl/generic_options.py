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
from fnmatch import fnmatch
from typing import Iterable

from pydantic import root_validator

from onetl.impl.frozen_model import FrozenModel

log = logging.getLogger(__name__)


class GenericOptions(FrozenModel):
    class Config:
        known_options: frozenset[str] | None = None
        prohibited_options: frozenset[str] = frozenset()

    @classmethod
    def parse(
        cls,
        options: GenericOptions | dict | None,
    ):
        """
        If a parameter inherited from the ReadOptions class was passed, then it will be returned unchanged.
        If a Dict object was passed it will be converted to ReadOptions.

        Otherwise, an exception will be raised
        """

        if not options:
            return cls()

        if isinstance(options, dict):
            options = cls.parse_obj(options)

        if not isinstance(options, cls):
            raise TypeError(
                f"{options.__class__.__name__} is not a {cls.__name__} instance",
            )

        return options

    @root_validator
    def check_options_not_prohibited(
        cls,
        values,
    ) -> None:
        prohibited = cls.__config__.prohibited_options  # type: ignore[attr-defined]
        if not prohibited:
            return values

        unknown_options = set(values) - set(cls.__fields__)
        if not unknown_options:
            return values

        matching_options = sorted(cls._get_matching_options(unknown_options, prohibited))
        if matching_options:
            class_name = cls.__name__  # type: ignore[attr-defined]
            raise ValueError(f"Options {matching_options!r} are not allowed to use in a {class_name}")

        return values

    @root_validator
    def warn_unknown_options(
        cls,
        values,
    ) -> None:
        class_name = cls.__name__  # type: ignore[attr-defined]
        known_options = cls.__config__.known_options  # type: ignore[attr-defined]
        # None means do nothing
        # empty set means that check is performed only on class attributes
        if known_options is None:
            return values

        current_options = set(values) - set(cls.__fields__)
        already_known = set(cls._get_matching_options(current_options, known_options))
        unknown_options = sorted(current_options - already_known)
        if not unknown_options:
            return values

        log.warning(
            "|%s| Options %s are not known by %s, are you sure they are valid?",
            class_name,
            unknown_options,
            class_name,
        )
        return values

    @classmethod
    def _get_matching_options(cls, values: Iterable[str], matches: Iterable[str]) -> list[str]:
        result = []
        for item in values:
            for match in matches:
                if item == match or fnmatch(item, match):
                    # item=prefix.realOption, match = prefix.*
                    result.append(item)
                    break

        return result
