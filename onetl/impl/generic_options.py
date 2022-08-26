from __future__ import annotations

from pydantic import root_validator

from onetl.impl.frozen_model import FrozenModel


class GenericOptions(FrozenModel):
    class Config:
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
        cls,  # noqa: N805
        values,
    ) -> None:
        prohibited = cls.__config__.prohibited_options  # type: ignore[attr-defined]
        if not prohibited:
            return values

        unknown_options = {key: value for key, value in values.items() if key not in cls.__fields__}  # noqa: WPS609
        if not unknown_options:
            return values

        matching_options = sorted(prohibited & set(unknown_options.keys()))  # noqa: WPS609
        if matching_options:
            class_name = cls.__name__  # type: ignore[attr-defined]

            if len(matching_options) > 1:
                options_str = ", ".join(repr(option) for option in matching_options)
                message = f"Options {options_str} are not allowed to use in a {class_name}"
            else:
                message = f"Option {matching_options[0]!r} is not allowed to use in a {class_name}"

            raise ValueError(message)

        return values
