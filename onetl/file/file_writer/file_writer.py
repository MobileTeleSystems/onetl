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
from typing import TYPE_CHECKING, Optional

from pydantic import validator

from onetl.base import BaseFileDFConnection, BaseFileFormat, PurePathProtocol
from onetl.file.file_writer.options import FileWriterOptions
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel
from onetl.log import (
    entity_boundary_log,
    log_dataframe_schema,
    log_options,
    log_with_indent,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

log = logging.getLogger(__name__)


@support_hooks
class FileWriter(FrozenModel):
    """Allows you to write Spark DataFrame as files in a target path of specified file connection
    with parameters. |support_hooks|

    Parameters
    ----------
    connection : :obj:`BaseFileDFConnection <onetl.base.base_file_df_connection.BaseFileDFConnection>`
        File DataFrame connection. See :ref:`file-df-connections` section.

    format : :obj:`BaseWritableFileFormat <onetl.base.base_file_format.BaseWritableFileFormat>`
        File format to write.

    target_path : os.PathLike or str
        Directory path to write data to.

    options : :obj:`FileWriterOptions <onetl.file.file_writer.options.FileWriterOptions>`, optional
        Common writing options.

    Examples
    --------
    Create writer to parse CSV files in local filesystem:

    .. code:: python

        from onetl.connection import SparkLocalFS
        from onetl.file import FileWriter
        from onetl.file.format import CSV

        local_fs = SparkLocalFS(spark=spark)

        writer = FileWriter(
            connection=local_fs,
            format=CSV(delimiter=","),
            target_path="/path/to/directory",
        )

    All supported options

    .. code:: python

        from onetl.connection import SparkLocalFS
        from onetl.file import FileWriter
        from onetl.file.format import CSV

        csv = CSV(delimiter=",")
        local_fs = SparkLocalFS(spark=spark)

        writer = FileWriter(
            connection=local_fs,
            format=csv,
            target_path="/path/to/directory",
            options=FileWriter.Options(if_exists="replace_entire_directory"),
        )
    """

    Options = FileWriterOptions

    connection: BaseFileDFConnection
    format: BaseFileFormat
    target_path: Optional[PurePathProtocol] = None
    options: FileWriterOptions = FileWriterOptions()

    @slot
    def run(self, df: DataFrame) -> None:
        """
        Method for writing DataFrame as files. |support_hooks|

        Parameters
        ----------

        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Write df to target:

        .. code:: python

            writer.run(df)
        """

        entity_boundary_log(f"{self.__class__.__name__} starts")

        self._log_parameters(df)
        self.connection.check()
        self.connection.write_df_as_files(
            df=df,
            path=self.target_path,
            format=self.format,
            options=self.options,
        )

        entity_boundary_log(f"{self.__class__.__name__} ends", char="-")

    def _log_parameters(self, df: DataFrame) -> None:  # noqa: WPS213
        log.info("|Spark| -> |%s| Writing dataframe using parameters:", self.connection.__class__.__name__)
        log_with_indent("target_path = '%s'", self.target_path)
        log_with_indent("format = %r", self.format)

        options_dict = self.options.dict(exclude_none=True)
        if options_dict:
            log_options(options_dict)

        log_dataframe_schema(df)

    @validator("target_path", pre=True)
    def _validate_target_path(cls, target_path, values):
        connection: BaseFileDFConnection = values["connection"]
        if target_path is None:
            return None
        return connection.path_from_string(target_path)

    @validator("format")
    def _validate_format(cls, format, values):  # noqa: WPS125
        connection: BaseFileDFConnection = values["connection"]
        connection.check_if_format_supported(format)
        return format

    @validator("options")
    def _validate_options(cls, value):
        return cls.Options.parse(value)
