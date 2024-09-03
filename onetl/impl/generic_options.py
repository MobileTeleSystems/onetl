# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import re
from fnmatch import fnmatch
from typing import Iterable, TypeVar

try:
    from pydantic.v1 import root_validator
except (ImportError, AttributeError):
    from pydantic import root_validator  # type: ignore[no-redef, assignment]

from onetl.impl.frozen_model import FrozenModel

log = logging.getLogger(__name__)
T = TypeVar("T", bound="GenericOptions")


class GenericOptions(FrozenModel):
    class Config:
        strip_prefixes: list[str | re.Pattern] = []
        known_options: frozenset[str] | None = None
        prohibited_options: frozenset[str] = frozenset()

    @classmethod
    def parse(
        cls: type[T],
        options: GenericOptions | dict | None,
    ) -> T:
        """
        If a parameter inherited from the ReadOptions class was passed, then it will be returned unchanged.
        If a Dict object was passed it will be converted to ReadOptions.

        Otherwise, an exception will be raised
        """

        if not options:
            return cls()

        if isinstance(options, dict):
            return cls.parse_obj(options)

        if not isinstance(options, cls):
            raise TypeError(
                f"{options.__class__.__name__} is not a {cls.__name__} instance",
            )

        return options

    @root_validator(pre=True)
    def _strip_prefixes(cls, values):
        prefixes = cls.__config__.strip_prefixes  # type: ignore[attr-defined]
        if not prefixes:  # type: ignore[attr-defined]
            return values

        for key in list(values.keys()):
            for prefix in prefixes:  # type: ignore[attr-defined]
                new_key, match_prefix = cls._strip_prefix(key, prefix)
                if match_prefix:
                    value = values.pop(key)
                    log.debug(
                        "Stripped prefix %r from %r, new key is %r",
                        match_prefix,
                        key,
                        new_key,
                    )
                    if new_key in values:
                        log.warning("Overwriting existing value of key %r with %r", key, new_key)  # noqa: WPS220
                    values[new_key] = value
                    key = new_key
        return values

    @staticmethod
    def _strip_prefix(key: str, prefix: str | re.Pattern) -> tuple[str, str | None]:
        if isinstance(prefix, str) and key.startswith(prefix):
            return key.replace(prefix, "", 1), prefix
        elif isinstance(prefix, re.Pattern) and prefix.match(key):
            return prefix.sub("", key, 1), prefix.pattern
        return key, None

    @root_validator
    def _check_options_allowed(
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
    def _warn_unknown_options(
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
