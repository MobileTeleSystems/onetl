# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, TypeVar

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

    class Config:
        prohibited_options = PROHIBITED_OPTIONS

    @slot
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader:
        options = self.dict(by_alias=True)
        return reader.format(self.name).options(**options)


@support_hooks
class WriteOnlyFileFormat(BaseWritableFileFormat, GenericOptions):
    name: ClassVar[str]

    class Config:
        prohibited_options = PROHIBITED_OPTIONS

    @slot
    def apply_to_writer(self, writer: DataFrameWriter) -> DataFrameWriter:
        options = self.dict(by_alias=True)
        return writer.format(self.name).options(**options)


@support_hooks
class ReadWriteFileFormat(BaseReadableFileFormat, BaseWritableFileFormat, GenericOptions):
    name: ClassVar[str]

    class Config:
        prohibited_options = PROHIBITED_OPTIONS

    @slot
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader:
        options = self.dict(by_alias=True)
        return reader.format(self.name).options(**options)

    @slot
    def apply_to_writer(self, writer: DataFrameWriter) -> DataFrameWriter:
        options = self.dict(by_alias=True)
        return writer.format(self.name).options(**options)
