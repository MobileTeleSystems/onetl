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

import warnings
from enum import Enum
from typing import Optional

from pydantic import Field, root_validator

from onetl.connection.db_connection.jdbc_mixin import JDBCOptions

# options from which are populated by Greenplum class methods
GENERIC_PROHIBITED_OPTIONS = frozenset(
    (
        "dbschema",
        "dbtable",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "mode",
        "truncate",
        "distributedBy",
        "iteratorOptimization",
    ),
)

READ_OPTIONS = frozenset(
    (
        "partitions",
        "numPartitions",
        "partitionColumn",
    ),
)


class GreenplumTableExistBehavior(str, Enum):
    APPEND = "append"
    IGNORE = "ignore"
    ERROR = "error"
    REPLACE_ENTIRE_TABLE = "replace_entire_table"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod  # noqa: WPS120
    def _missing_(cls, value: object):  # noqa: WPS120
        if str(value) == "overwrite":
            warnings.warn(
                "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_entire_table` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_ENTIRE_TABLE


class GreenplumReadOptions(JDBCOptions):
    """Pivotal's Greenplum Spark connector reading options.

    .. note ::

        You can pass any value
        `supported by connector <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-read_from_gpdb.html>`_,
        even if it is not mentioned in this documentation.

        The set of supported options depends on connector version. See link above.

    .. warning::

        Some options, like ``url``, ``dbtable``, ``server.*``, ``pool.*``,
        etc are populated from connection attributes, and cannot be set in ``ReadOptions`` class

    Examples
    --------

    Read options initialization

    .. code:: python

        Greenplum.ReadOptions(
            partition_column="reg_id",
            num_partitions=10,
        )
    """

    class Config:
        known_options = READ_OPTIONS
        prohibited_options = JDBCOptions.Config.prohibited_options | GENERIC_PROHIBITED_OPTIONS | WRITE_OPTIONS

    partition_column: Optional[str] = Field(alias="partitionColumn")
    """Column used to parallelize reading from a table.

    .. warning::

        You should not change this option, unless you know what you're doing.

        It's preferable to use default values to read data parallel by number of segments in Greenplum cluster.

    Possible values:
        * ``None`` (default):
            Spark generates N jobs (where N == number of segments in Greenplum cluster),
            each job is reading only data from a specific segment
            (filtering data by ``gp_segment_id`` column).

            This is very effective way to fetch the data from a cluster.

        * table column
            Allocate each executor a range of values from a specific column.

            .. note::
                Column type must be numeric. Other types are not supported.

            Spark generates for each executor an SQL query like:

            Executor 1:

            .. code:: sql

                SELECT ... FROM table
                WHERE (partition_column >= lowerBound
                        OR partition_column IS NULL)
                AND partition_column < (lower_bound + stride)

            Executor 2:

            .. code:: sql

                SELECT ... FROM table
                WHERE partition_column >= (lower_bound + stride)
                AND partition_column < (lower_bound + 2 * stride)

            ...

            Executor N:

            .. code:: sql

                SELECT ... FROM table
                WHERE partition_column >= (lower_bound + (N-1) * stride)
                AND partition_column <= upper_bound

            Where ``stride=(upper_bound - lower_bound) / num_partitions``,
            ``lower_bound=MIN(partition_column)``, ``upper_bound=MAX(partition_column)``.

            .. note::

                :obj:`~num_partitions` is used just to
                calculate the partition stride, **NOT** for filtering the rows in table.
                So all rows in the table will be returned (unlike *Incremental* :ref:`strategy`).

            .. note::

                All queries are executed in parallel. To execute them sequentially, use *Batch* :ref:`strategy`.

    .. warning::

        Both options :obj:`~partition_column` and :obj:`~num_partitions` should have a value,
        or both should be ``None``

    Examples
    --------

    Read data in 10 parallel jobs by range of values in ``id_column`` column:

    .. code:: python

        Greenplum.ReadOptions(
            partition_column="id_column",
            num_partitions=10,
        )
    """

    num_partitions: Optional[int] = Field(alias="partitions")
    """Number of jobs created by Spark to read the table content in parallel.

    See documentation for :obj:`~partition_column` for more details

    .. warning::

        By default connector uses number of segments in the Greenplum cluster.
        You should not change this option, unless you know what you're doing

    .. warning::

        Both options :obj:`~partition_column` and :obj:`~num_partitions` should have a value,
        or both should be ``None``
    """


class GreenplumWriteOptions(JDBCOptions):
    """Pivotal's Greenplum Spark connector writing options.

    .. note ::

        You can pass any value
        `supported by connector <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-write_to_gpdb.html>`_,
        even if it is not mentioned in this documentation.

        The set of supported options depends on connector version. See link above.

    .. warning::

        Some options, like ``url``, ``dbtable``, ``server.*``, ``pool.*``,
        etc are populated from connection attributes, and cannot be set in ``WriteOptions`` class

    Examples
    --------

    Write options initialization

    .. code:: python

        options = Greenplum.WriteOptions(
            if_exists="append",
            truncate="false",
            distributedBy="mycolumn",
        )
    """

    class Config:
        known_options = WRITE_OPTIONS
        prohibited_options = JDBCOptions.Config.prohibited_options | GENERIC_PROHIBITED_OPTIONS | READ_OPTIONS

    if_exists: GreenplumTableExistBehavior = Field(default=GreenplumTableExistBehavior.APPEND, alias="mode")
    """Behavior of writing data into existing table.

    Possible values:
        * ``append`` (default)
            Adds new rows into existing table.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``distributedBy`` and others).

                * Table exists
                    Data is appended to a table. Table has the same DDL as before writing data.

                    .. warning::

                        This mode does not check whether table already contains
                        rows from dataframe, so duplicated rows can be created.

                        Also Spark does not support passing custom options to
                        insert statement, like ``ON CONFLICT``, so don't try to
                        implement deduplication using unique indexes or constraints.

                        Instead, write to staging table and perform deduplication
                        using :obj:`~execute` method.

        * ``replace_entire_table``
            **Table is dropped and then created**.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``distributedBy`` and others).

                * Table exists
                    Table content is replaced with dataframe content.

                    After writing completed, target table could either have the same DDL as
                    before writing data (``truncate=True``), or can be recreated (``truncate=False``).

        * ``ignore``
            Ignores the write operation if the table already exists.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``distributedBy`` and others).

                * Table exists
                    The write operation is ignored, and no data is written to the table.

        * ``error``
            Raises an error if the table already exists.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``distributedBy`` and others).

                * Table exists
                    An error is raised, and no data is written to the table.

    """

    @root_validator(pre=True)
    def _mode_is_deprecated(cls, values):
        if "mode" in values:
            warnings.warn(
                "Option `Greenplum.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `Greenplum.WriteOptions(if_exists=...)` instead",
                category=UserWarning,
                stacklevel=3,
            )
        return values
