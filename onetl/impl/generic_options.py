# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
import re
import sys
from collections.abc import Iterable
from fnmatch import fnmatch
from typing import TypeVar

from pydantic import ConfigDict, model_validator

from onetl.impl.frozen_model import FrozenModel

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

log = logging.getLogger(__name__)
T = TypeVar("T", bound="GenericOptions")


class GenericOptions(FrozenModel):
    model_config = ConfigDict()

    @classmethod
    def parse(
        cls,
        options: "GenericOptions | dict | None",
    ) -> Self:
        """
        If a parameter inherited from the ReadOptions class was passed, then it will be returned unchanged.
        If a Dict object was passed it will be converted to ReadOptions.

        Otherwise, an exception will be raised
        """

        if not options:
            return cls()

        if isinstance(options, dict):
            return cls.model_validate(options)

        if not isinstance(options, cls):
            msg = f"{options.__class__.__name__} is not a {cls.__name__} instance"
            raise TypeError(msg)

        return options

    @model_validator(mode="before")
    @classmethod
    def _strip_prefixes(cls, values):
        prefixes = cls.model_config.get("strip_prefixes")  # type: ignore[attr-defined]
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
                        log.warning("Overwriting existing value of key %r with %r", key, new_key)
                    values[new_key] = value
                key = new_key  # noqa: PLW2901
        return values

    @staticmethod
    def _strip_prefix(key: str, prefix: str | re.Pattern) -> tuple[str, str | None]:
        if isinstance(prefix, str) and key.startswith(prefix):
            return key.replace(prefix, "", 1), prefix
        if isinstance(prefix, re.Pattern) and prefix.match(key):
            return prefix.sub("", key, 1), prefix.pattern
        return key, None

    @model_validator(mode="after")
    def _check_options_allowed(self):
        prohibited = self.model_config.get("prohibited_options")
        if not prohibited:
            return self

        extra_options = set(self.model_extra or {})
        if not extra_options:
            return self

        matching_options = sorted(self._get_matching_options(extra_options, prohibited))
        if matching_options:
            class_name = self.__class__.__name__
            msg = f"Options {matching_options!r} are not allowed to use in a {class_name}"
            raise ValueError(msg)

        return self

    @model_validator(mode="after")
    def _warn_unknown_options(self):
        class_name = self.__class__.__name__
        known_options = self.model_config.get("known_options")
        # None means do nothing
        # empty set means that check is performed only on class attributes
        if known_options is None:
            return self

        extra_options = set(self.model_extra or {})
        already_known = set(self._get_matching_options(extra_options, known_options))
        unknown_options = sorted(extra_options - already_known)
        if not unknown_options:
            return self

        log.warning(
            "|%s| Options %s are not known by %s, are you sure they are valid?",
            class_name,
            unknown_options,
            class_name,
        )
        return self

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
