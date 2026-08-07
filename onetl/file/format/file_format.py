# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, ClassVar, TypeVar

from pydantic import ConfigDict

from onetl.base import BaseReadableFileFormat, BaseWritableFileFormat
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader, DataFrameWriter

T = TypeVar("T")

PROHIBITED_OPTIONS = frozenset(
    (
        "ignoreCorruptFiles",
        "ignoreMissingFiles",
        "modifiedAfter",
        "modifiedBefore",
        "pathGlobFilter",
        "recursiveFileLookup",
    ),
)


@support_hooks
class ReadOnlyFileFormat(BaseReadableFileFormat, GenericOptions):
    name: ClassVar[str]
    model_config = ConfigDict(prohibited_options=PROHIBITED_OPTIONS)  # type: ignore[typeddict-unknown-key]

    @slot
    def apply_to_reader(self, reader: "DataFrameReader") -> "DataFrameReader":
        options = self.model_dump(by_alias=True, exclude_none=True)
        return reader.format(self.name).options(**options)


@support_hooks
class WriteOnlyFileFormat(BaseWritableFileFormat, GenericOptions):
    name: ClassVar[str]
    model_config = ConfigDict(prohibited_options=PROHIBITED_OPTIONS)  # type: ignore[typeddict-unknown-key]

    @slot
    def apply_to_writer(self, writer: "DataFrameWriter") -> "DataFrameWriter":
        options = self.model_dump(by_alias=True, exclude_none=True)
        return writer.format(self.name).options(**options)


@support_hooks
class ReadWriteFileFormat(BaseReadableFileFormat, BaseWritableFileFormat, GenericOptions):
    name: ClassVar[str]
    model_config = ConfigDict(prohibited_options=PROHIBITED_OPTIONS)  # type: ignore[typeddict-unknown-key]

    @slot
    def apply_to_reader(self, reader: "DataFrameReader") -> "DataFrameReader":
        options = self.model_dump(by_alias=True, exclude_none=True)
        return reader.format(self.name).options(**options)

    @slot
    def apply_to_writer(self, writer: "DataFrameWriter") -> "DataFrameWriter":
        options = self.model_dump(by_alias=True, exclude_none=True)
        return writer.format(self.name).options(**options)
