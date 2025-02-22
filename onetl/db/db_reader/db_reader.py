# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import textwrap
import warnings
from logging import getLogger
from typing import TYPE_CHECKING, Any, List, Optional, Union

import frozendict
from etl_entities.hwm import HWM, ColumnHWM, KeyValueHWM
from etl_entities.old_hwm import IntHWM as OldColumnHWM
from etl_entities.source import Column, Table

try:
    from pydantic.v1 import Field, PrivateAttr, root_validator, validator
except (ImportError, AttributeError):
    from pydantic import Field, PrivateAttr, root_validator, validator  # type: ignore[no-redef, assignment]

from onetl._util.spark import override_job_description, try_import_pyspark
from onetl.base import (
    BaseDBConnection,
    ContainsGetDFSchemaMethod,
    ContainsGetMinMaxValues,
)
from onetl.exception import NoDataError
from onetl.hooks import slot, support_hooks
from onetl.hwm import AutoDetectHWM, Edge, Window
from onetl.impl import FrozenModel, GenericOptions
from onetl.log import (
    entity_boundary_log,
    log_collection,
    log_dataframe_schema,
    log_hwm,
    log_json,
    log_options,
    log_with_indent,
)
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy
from onetl.strategy.hwm_strategy import HWMStrategy
from onetl.strategy.strategy_manager import StrategyManager

log = getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.types import StructField, StructType


@support_hooks
class DBReader(FrozenModel):
    """Allows you to read data from a table with specified database connection
    and parameters, and return its content as Spark dataframe. |support_hooks|

    .. note::

        DBReader can return different results depending on :ref:`strategy`

    .. note::

        This class operates with only one source at a time. It does NOT support executing queries
        to multiple source, like ``SELECT ... JOIN``.

    .. versionadded:: 0.1.0

    .. versionchanged:: 0.8.0
        Moved ``onetl.core.DBReader`` → ``onetl.db.DBReader``

    Parameters
    ----------
    connection : :obj:`onetl.connection.BaseDBConnection`
        Class which contains DB connection properties. See :ref:`db-connections` section

    source : str
        Table/collection/etc name to read data from.

        If connection has schema support, you need to specify the full name of the source
        including the schema, e.g. ``schema.name``.

        .. versionchanged:: 0.7.0
            Renamed ``table`` → ``source``

    columns : list of str, default: None
        The list of columns to be read.

        If RDBMS supports any kind of expressions, you can pass them too.

        .. code:: python

            columns = [
                "mycolumn",
                "another_column as alias",
                "count(*) over ()",
                "some(function) as alias2",
            ]

        .. note::

            Some sources does not have columns.

        .. note::

            It is recommended to pass column names explicitly to avoid selecting too many columns,
            and to avoid adding unexpected columns to dataframe if source DDL is changed.

        .. deprecated:: 0.10.0

            Syntax ``DBReader(columns="col1, col2")`` (string instead of list) is not supported,
            and will be removed in v1.0.0

    where : Any, default: ``None``
        Custom ``where`` for SQL query or MongoDB pipeline.

        ``where`` syntax depends on the source. For example, SQL sources
        accept ``where`` as a string, but MongoDB sources accept ``where`` as a dictionary.

        .. code:: python

            # SQL database connection
            where = "column_1 > 2"

            # MongoDB connection
            where = {
                "col_1": {"$gt": 1, "$lt": 100},
                "col_2": {"$gt": 2},
                "col_3": {"$eq": "hello"},
            }

        .. note::

            Some sources does not support data filtering.

    hwm : type[HWM] | None, default: ``None``
        HWM class to be used as :etl-entities:`HWM <hwm/index.html>` value.

        .. code:: python

            hwm = DBReader.AutoDetectHWM(
                name="some_unique_hwm_name",
                expression="hwm_column",
            )

        HWM value will be fetched using ``hwm_column`` SQL query.

        If you want to use some SQL expression as HWM value, you can use it as well:

        .. code:: python

            hwm = DBReader.AutoDetectHWM(
                name="some_unique_hwm_name",
                expression="cast(hwm_column_orig as date)",
            )

        .. note::

            Some sources does not support passing expressions and can be used only with column/field
            names which present in the source.

        .. versionchanged:: 0.10.0
            Replaces deprecated ``hwm_column`` and ``hwm_expression``  attributes

    hint : Any, default: ``None``
        Hint expression used for querying the data.

        ``hint`` syntax depends on the source. For example, SQL sources
        accept ``hint`` as a string, but MongoDB sources accept ``hint`` as a dictionary.

        .. code:: python

            # SQL database connection
            hint = "index(myschema.mytable mycolumn)"

            # MongoDB connection
            hint = {
                "mycolumn": 1,
            }

        .. note::

            Some sources does not support hints.

    df_schema : StructType, optional, default: ``None``
        Spark DataFrame schema, used for proper type casting of the rows.

        .. code:: python

            from pyspark.sql.types import (
                DoubleType,
                IntegerType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            df_schema = StructType(
                [
                    StructField("_id", IntegerType()),
                    StructField("text_string", StringType()),
                    StructField("hwm_int", IntegerType()),
                    StructField("hwm_datetime", TimestampType()),
                    StructField("float_value", DoubleType()),
                ],
            )

            reader = DBReader(
                connection=connection,
                source="fiddle.dummy",
                df_schema=df_schema,
            )

        .. note::

            Some sources does not support passing dataframe schema.

    options : dict, :obj:`onetl.connection.BaseDBConnection.ReadOptions`, default: ``None``
        Spark read options, like partitioning mode.

        .. code:: python

            Postgres.ReadOptions(
                partitioningMode="hash",
                partitionColumn="some_column",
                numPartitions=20,
                fetchsize=1000,
            )

        .. note::

            Some sources does not support reading options.

    Examples
    --------

    .. tabs::

        .. code-tab:: py Minimal example

            from onetl.db import DBReader
            from onetl.connection import Postgres

            postgres = Postgres(...)

            # create reader
            reader = DBReader(connection=postgres, source="fiddle.dummy")

            # read data from table "fiddle.dummy"
            df = reader.run()

        .. code-tab:: py With custom reading options

            from onetl.connection import Postgres
            from onetl.db import DBReader

            postgres = Postgres(...)
            options = Postgres.ReadOptions(sessionInitStatement="select 300", fetchsize="100")

            # create reader and pass some options to the underlying connection object
            reader = DBReader(connection=postgres, source="fiddle.dummy", options=options)

            # read data from table "fiddle.dummy"
            df = reader.run()

        .. code-tab:: py Full example

            from onetl.db import DBReader
            from onetl.connection import Postgres

            postgres = Postgres(...)
            options = Postgres.ReadOptions(sessionInitStatement="select 300", fetchsize="100")

            # create reader with specific columns, rows filter
            reader = DBReader(
                connection=postgres,
                source="default.test",
                where="d_id > 100",
                hint="NOWAIT",
                columns=["d_id", "d_name", "d_age"],
                options=options,
            )

            # read data from table "fiddle.dummy"
            df = reader.run()

        .. tab:: Incremental reading

            See :ref:`strategy` for more examples

            .. code:: python

                from onetl.strategy import IncrementalStrategy

                ...

                reader = DBReader(
                    connection=postgres,
                    source="fiddle.dummy",
                    hwm=DBReader.AutoDetectHWM(  # mandatory for IncrementalStrategy
                        name="some_unique_hwm_name",
                        expression="d_age",
                    ),
                )

                # read data from table "fiddle.dummy"
                # but only with new rows (`WHERE d_age > previous_hwm_value`)
                with IncrementalStrategy():
                    df = reader.run()
    """

    connection: BaseDBConnection
    source: str = Field(alias="table")
    columns: Optional[List[str]] = Field(default=None, min_items=1)
    where: Optional[Any] = None
    hint: Optional[Any] = None
    df_schema: Optional[StructType] = None
    hwm_column: Optional[Union[str, tuple]] = None
    hwm_expression: Optional[str] = None
    hwm: Optional[Union[AutoDetectHWM, ColumnHWM, KeyValueHWM]] = None
    options: Optional[GenericOptions] = None

    AutoDetectHWM = AutoDetectHWM

    _connection_checked: bool = PrivateAttr(default=False)

    @validator("source", always=True)
    def validate_source(cls, source, values):
        connection: BaseDBConnection = values["connection"]
        return connection.dialect.validate_name(source)

    @validator("columns", always=True, pre=True)
    def validate_columns(cls, value: str | list[str] | None, values: dict) -> list[str] | None:
        connection: BaseDBConnection = values["connection"]
        return connection.dialect.validate_columns(value)

    @validator("where", always=True)
    def validate_where(cls, where: Any, values: dict) -> Any:
        connection: BaseDBConnection = values["connection"]
        result = connection.dialect.validate_where(where)
        if isinstance(result, dict):
            return frozendict.frozendict(result)  # type: ignore[attr-defined, operator]
        return result

    @validator("hint", always=True)
    def validate_hint(cls, hint: Any, values: dict) -> Any:
        connection: BaseDBConnection = values["connection"]
        result = connection.dialect.validate_hint(hint)
        if isinstance(result, dict):
            return frozendict.frozendict(result)  # type: ignore[attr-defined, operator]
        return result

    @validator("df_schema", always=True)
    def validate_df_schema(cls, df_schema: StructType | None, values: dict) -> StructType | None:
        connection: BaseDBConnection = values["connection"]
        return connection.dialect.validate_df_schema(df_schema)

    @root_validator(skip_on_failure=True)
    def validate_hwm(cls, values: dict) -> dict:  # noqa: WPS231
        connection: BaseDBConnection = values["connection"]
        source: str = values["source"]
        hwm_column: str | tuple[str, str] | None = values.get("hwm_column")
        hwm_expression: str | None = values.get("hwm_expression")
        hwm: HWM | None = values.get("hwm")

        if hwm_column is not None:
            if hwm:
                raise ValueError("Please pass either DBReader(hwm=...) or DBReader(hwm_column=...), not both")

            if not hwm_expression and isinstance(hwm_column, tuple):
                hwm_column, hwm_expression = hwm_column  # noqa: WPS434

                if not hwm_expression:
                    error_message = textwrap.dedent(
                        """
                        When the 'hwm_column' field is a tuple, then it must be
                        specified as tuple('column_name', 'expression').

                        Otherwise, the 'hwm_column' field should be a string.
                        """,
                    )
                    raise ValueError(error_message)

            # convert old parameters to new one
            old_hwm = OldColumnHWM(
                source=Table(name=source, instance=connection.instance_url),  # type: ignore[arg-type]
                column=Column(name=hwm_column),  # type: ignore[arg-type]
            )
            warnings.warn(
                textwrap.dedent(
                    f"""
                    Passing "hwm_column" in DBReader class is deprecated since version 0.10.0,
                    and will be removed in v1.0.0.

                    Instead use:
                        hwm=DBReader.AutoDetectHWM(
                            name={old_hwm.qualified_name!r},
                            expression={hwm_column!r},
                        )
                    """,
                ),
                UserWarning,
                stacklevel=2,
            )
            hwm = AutoDetectHWM(
                name=old_hwm.qualified_name,
                expression=hwm_expression or hwm_column,
            )

        if hwm and not hwm.expression:
            raise ValueError("`hwm.expression` cannot be None")

        if hwm and not hwm.entity:
            hwm = hwm.copy(update={"entity": source})

        if hwm and hwm.entity != source:
            error_message = textwrap.dedent(
                f"""
                Passed `hwm.source` is different from `source`.

                `hwm`:
                    {hwm!r}

                `source`:
                    {source!r}

                This is not allowed.
                """,
            )
            raise ValueError(error_message)

        values["hwm"] = connection.dialect.validate_hwm(hwm)
        values["hwm_column"] = None
        values["hwm_expression"] = None
        return values

    @validator("options", pre=True, always=True)
    def validate_options(cls, options, values):
        connection = values.get("connection")
        read_options_class = getattr(connection, "ReadOptions", None)
        if read_options_class:
            return read_options_class.parse(options)

        if options:
            raise ValueError(
                f"{connection.__class__.__name__} does not implement ReadOptions, but {options!r} is passed",
            )

        return None

    @slot
    def has_data(self) -> bool:
        """Returns ``True`` if there is some data in the source, ``False`` otherwise. |support_hooks|

        .. note::

            This method can return different results depending on :ref:`strategy`

        .. warning::

            If :etl-entities:`hwm <hwm/index.html>` is used, then method should be called inside :ref:`strategy` context. And vise-versa, if HWM is not used, this method should not be called within strategy.

        .. versionadded:: 0.10.0

        Raises
        ------
        RuntimeError
            Current strategy is not compatible with HWM parameter.

        Examples
        --------

        .. code:: python

            reader = DBReader(...)

            # handle situation when there is no data in the source
            if reader.has_data():
                df = reader.run()
            else:
                # implement your handling logic here
                ...
        """

        entity_boundary_log(log, msg=f"{self.__class__.__name__}.has_data() starts")
        self._check_strategy()

        if not self._connection_checked:
            self._log_parameters()
            self.connection.check()
            self._connection_checked = True

        job_description = f"{self.connection} -> {self.__class__.__name__}.has_data({self.source})"
        with override_job_description(self.connection.spark, job_description):
            window, limit = self._calculate_window_and_limit()
            if limit == 0:
                return False

            df = self.connection.read_source_as_df(
                source=str(self.source),
                columns=self.columns,
                hint=self.hint,
                where=self.where,
                df_schema=self.df_schema,
                window=window,
                limit=1,
                **self._get_read_kwargs(),
            )

            entity_boundary_log(log, msg=f"{self.__class__.__name__}.has_data() ends", char="-")
            return bool(df.take(1))

    @slot
    def raise_if_no_data(self) -> None:
        """Raises exception ``NoDataError`` if source does not contain any data. |support_hooks|

        .. note::

            This method can return different results depending on :ref:`strategy`

        .. warning::

            If :etl-entities:`hwm <hwm/index.html>` is used, then method should be called inside :ref:`strategy` context. And vise-versa, if HWM is not used, this method should not be called within strategy.

        .. versionadded:: 0.10.0

        Raises
        ------
        RuntimeError
            Current strategy is not compatible with HWM parameter.

        :obj:`onetl.exception.NoDataError`
            There is no data in source.

        Examples
        --------

        .. code:: python

            reader = DBReader(...)

            # ensure that there is some data in the source before reading it using Spark
            reader.raise_if_no_data()
        """

        if not self.has_data():
            raise NoDataError(f"No data in the source: {self.source}")

    @slot
    def run(self) -> DataFrame:
        """
        Reads data from source table and saves as Spark dataframe. |support_hooks|

        .. note::

            This method can return different results depending on :ref:`strategy`

        .. warning::

            If :etl-entities:`hwm <hwm/index.html>` is used, then method should be called inside :ref:`strategy` context. And vise-versa, if HWM is not used, this method should not be called within strategy.

        .. versionadded:: 0.1.0

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Read data to Spark dataframe:

        .. code:: python

            df = reader.run()
        """

        entity_boundary_log(log, msg=f"{self.__class__.__name__}.run() starts")
        self._check_strategy()

        if not self._connection_checked:
            self._log_parameters()
            self.connection.check()
            self._connection_checked = True

        job_description = f"{self.connection} -> {self.__class__.__name__}.run({self.source})"
        with override_job_description(self.connection.spark, job_description):
            window, limit = self._calculate_window_and_limit()

            # update the HWM with the stop value
            if self.hwm and window:
                strategy: HWMStrategy = StrategyManager.get_current()  # type: ignore[assignment]
                strategy.update_hwm(window.stop_at.value)

            df = self.connection.read_source_as_df(
                source=str(self.source),
                columns=self.columns,
                hint=self.hint,
                where=self.where,
                df_schema=self.df_schema,
                window=window,
                limit=limit,
                **self._get_read_kwargs(),
            )

        entity_boundary_log(log, msg=f"{self.__class__.__name__}.run() ends", char="-")
        return df

    def _check_strategy(self):
        strategy = StrategyManager.get_current()
        class_name = type(self).__name__
        strategy_name = type(strategy).__name__

        if self.hwm:
            if not isinstance(strategy, HWMStrategy):
                raise RuntimeError(
                    f"{class_name}(hwm=...) cannot be used with {strategy_name}. Check documentation DBReader.has_data(): https://onetl.readthedocs.io/en/stable/db/db_reader.html#onetl.db.db_reader.db_reader.DBReader.has_data.",
                )
            self._prepare_hwm(strategy, self.hwm)

        elif isinstance(strategy, HWMStrategy):
            raise RuntimeError(f"{strategy_name} cannot be used without {class_name}(hwm=...)")

    def _prepare_hwm(self, strategy: HWMStrategy, hwm: ColumnHWM):
        if not strategy.hwm:
            # first run within the strategy
            if isinstance(hwm, AutoDetectHWM):
                strategy.hwm = self._autodetect_hwm(hwm)
            else:
                strategy.hwm = hwm
            strategy.fetch_hwm()
            return

        if not isinstance(strategy.hwm, (ColumnHWM, KeyValueHWM)) or strategy.hwm.name != hwm.name:
            # exception raised when inside one strategy >1 processes on the same table but with different hwm columns
            # are executed, example: test_postgres_strategy_incremental_hwm_set_twice
            error_message = textwrap.dedent(
                f"""
                Detected wrong {type(strategy).__name__} usage.

                Previous run:
                    {strategy.hwm!r}
                Current run:
                    {hwm!r}

                Probably you've executed code which looks like this:
                    with {strategy.__class__.__name__}(...):
                        DBReader(hwm=one_hwm, ...).run()
                        DBReader(hwm=another_hwm, ...).run()

                Please change it to:
                    with {strategy.__class__.__name__}(...):
                        DBReader(hwm=one_hwm, ...).run()

                    with {strategy.__class__.__name__}(...):
                        DBReader(hwm=another_hwm, ...).run()
                """,
            )
            raise ValueError(error_message)

        strategy.validate_hwm_attributes(hwm, strategy.hwm, origin=self.__class__.__name__)

    def _autodetect_hwm(self, hwm: HWM) -> HWM:
        field = self._get_hwm_field(hwm)
        field_type = field.dataType
        detected_hwm_type = self.connection.dialect.detect_hwm_class(field)

        if detected_hwm_type:
            log.info(
                "|%s| Detected HWM type: %r",
                self.__class__.__name__,
                detected_hwm_type.__name__,
            )
            return detected_hwm_type.deserialize(hwm.dict())

        error_message = textwrap.dedent(
            f"""
            Cannot detect HWM type for field {hwm.expression!r} of type {field_type!r}

            Check that column or expression type is supported by {self.connection.__class__.__name__}.
            """,
        )
        raise RuntimeError(error_message)

    def _get_hwm_field(self, hwm: HWM) -> StructField:
        log.info(
            "|%s| Getting Spark type for HWM expression: %r",
            self.__class__.__name__,
            hwm.expression,
        )

        result: StructField
        if self.df_schema:
            schema = {field.name.casefold(): field for field in self.df_schema}
            column = hwm.expression.casefold()
            if column not in schema:
                raise ValueError(f"HWM column {column!r} not found in dataframe schema")

            result = schema[column]
        elif isinstance(self.connection, ContainsGetDFSchemaMethod):
            df_schema = self.connection.get_df_schema(
                source=self.source,
                columns=[hwm.expression],
                **self._get_read_kwargs(),
            )
            result = df_schema[0]
        else:
            raise ValueError(
                "You should specify `df_schema` field to use DBReader with "
                f"{self.connection.__class__.__name__} connection",
            )

        log.info("|%s| Got Spark field: %s", self.__class__.__name__, result)
        return result

    def _calculate_window_and_limit(self) -> tuple[Window | None, int | None]:  # noqa: WPS231
        if not self.hwm:
            # SnapshotStrategy - always select all the data from source
            return None, None

        strategy: HWMStrategy = StrategyManager.get_current()  # type: ignore[assignment]

        start_value = strategy.current.value
        stop_value = strategy.stop if isinstance(strategy, BatchHWMStrategy) else None

        if start_value is not None and stop_value is not None:
            # we already have start and stop values, nothing to do
            window = Window(self.hwm.expression, start_from=strategy.current, stop_at=strategy.next)
            return window, None

        if not isinstance(self.connection, ContainsGetMinMaxValues):
            raise ValueError(
                f"{self.connection.__class__.__name__} connection does not support {strategy.__class__.__name__}",
            )

        # strategy does not have start/stop/current value - use min/max values from source to fill them up
        min_value, max_value = self.connection.get_min_max_values(
            source=self.source,
            window=Window(
                self.hwm.expression,
                # always include both edges, > vs >= are applied only to final dataframe
                start_from=Edge(value=start_value),
                stop_at=Edge(value=stop_value),
            ),
            hint=self.hint,
            where=self.where,
            **self._get_read_kwargs(),
        )

        if min_value is None or max_value is None:
            log.warning("|%s| No data in source %r", self.__class__.__name__, self.source)
            # return limit=0 to always return empty dataframe from the source.
            # otherwise dataframe may start returning some data whether HWM is not being set
            return None, 0

        # returned value type may not always be the same type as expected, force cast to HWM type
        hwm = strategy.hwm.copy()  # type: ignore[union-attr]

        try:
            min_value = hwm.set_value(min_value).value
            max_value = hwm.set_value(max_value).value
        except ValueError as e:
            hwm_class_name = type(hwm).__name__
            error_message = textwrap.dedent(
                f"""
                Expression {hwm.expression!r} returned values:
                    min: {min_value!r} of type {type(min_value).__name__!r}
                    max: {max_value!r} of type {type(min_value).__name__!r}
                which are not compatible with {hwm_class_name}.

                Please check if selected combination of HWM class and expression is valid.
                """,
            )
            raise ValueError(error_message) from e

        if isinstance(strategy, BatchHWMStrategy):
            if strategy.start is None:
                strategy.start = min_value

            if strategy.stop is None:
                strategy.stop = max_value

            window = Window(self.hwm.expression, start_from=strategy.current, stop_at=strategy.next)
        else:
            # for IncrementalStrategy fix only max value to avoid difference between real dataframe content and HWM value
            window = Window(
                self.hwm.expression,
                start_from=strategy.current,
                stop_at=Edge(value=max_value),
            )

        return window, None

    def _log_parameters(self) -> None:
        log.info("|%s| -> |Spark| Reading DataFrame from source using parameters:", self.connection.__class__.__name__)
        log_with_indent(log, "source = '%s'", self.source)

        if self.hint:
            log_json(log, self.hint, "hint")

        if self.columns:
            log_collection(log, "columns", self.columns)

        if self.where:
            log_json(log, self.where, "where")

        if self.df_schema:
            empty_df = self.connection.spark.createDataFrame([], self.df_schema)  # type: ignore
            log_dataframe_schema(log, empty_df)

        if self.hwm:
            log_hwm(log, self.hwm)

        options = self.options.dict(by_alias=True, exclude_none=True) if self.options else None
        log_options(log, options)

    def _get_read_kwargs(self) -> dict:
        if self.options:
            return {"options": self.options}

        return {}

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        try_import_pyspark()
        from pyspark.sql.types import StructType  # noqa: WPS442

        # avoid importing pyspark unless user called the constructor,
        # as we allow user to use `Connection.get_packages()` for creating Spark session
        refs = super()._forward_refs()
        refs["StructType"] = StructType
        return refs
