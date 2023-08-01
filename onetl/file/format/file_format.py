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


class ReadWriteFileFormat(ReadOnlyFileFormat, WriteOnlyFileFormat):
    pass
