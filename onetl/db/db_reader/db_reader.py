# SPDX-FileCopyrightText: 2021-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import sys
import textwrap
import time
import warnings
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, cast

from etl_entities.hwm import HWM, ColumnHWM, HWMTypeRegistry, KeyValueHWM
from humanize import naturaldelta
from pydantic import Field, PrivateAttr, ValidationInfo, field_validator, model_validator

from onetl._util.alias import avoid_alias
from onetl._util.process import get_process_info
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

if sys.version_info < (3, 15):
    from frozendict import frozendict

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.types import StructField, StructType

log = getLogger(__name__)


@support_hooks
class DBReader(FrozenModel):
    """Allows you to read data from a table with specified database connection
    and parameters, and return its content as Spark dataframe. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    !!! note

        DBReader can return different results depending on [strategy][DBR-onetl-strategy-read-strategies]

    !!! note

        This class operates with only one source at a time. It does NOT support executing queries
        to multiple source, like `SELECT ... JOIN`.

    !!! success "Added in 0.1.0"

    !!! info "Changed in 0.8.0"
        Moved `onetl.core.DBReader` → `onetl.db.DBReader`

    Parameters
    ----------
    connection
        Class which contains DB connection properties. See [DBR-onetl-connection-db-connection-db-connections][] section

    source
        Table/collection/etc name to read data from.

        If connection has schema support, you need to specify the full name of the source
        including the schema, e.g. `schema.name`.

        !!! info "Changed in 0.7.0"
            Renamed `table` → `source`

    columns
        The list of columns to be read.

        If RDBMS supports any kind of expressions, you can pass them too.

        ```python
        columns = [
            "mycolumn",
            "another_column as alias",
            "count(*) over ()",
            "some(function) as alias2",
        ]
        ```
        !!! note

            Some sources does not have columns.

        !!! note

            It is recommended to pass column names explicitly to avoid selecting too many columns,
            and to avoid adding unexpected columns to dataframe if source DDL is changed.

        !!! warning "Deprecated since 0.10.0"

            Syntax `DBReader(columns="col1, col2")` (string instead of list) is not supported,
            and will be removed in v1.0.0

    where
        Custom `where` for SQL query or MongoDB pipeline.

        `where` syntax depends on the source. For example, SQL sources
        accept `where` as a string, but MongoDB sources accept `where` as a dictionary.

        ```python
        # SQL database connection
        where = "column_1 > 2"

        # MongoDB connection
        where = {
            "col_1": {"$gt": 1, "$lt": 100},
            "col_2": {"$gt": 2},
            "col_3": {"$eq": "hello"},
        }
        ```
        !!! note

            Some sources does not support data filtering.

    hwm
        HWM class to be used as [HWM](https://etl-entities.readthedocs.io/en/stable/hwm/index.html) value.

        ```python
        hwm = DBReader.AutoDetectHWM(
            name="some_unique_hwm_name",
            expression="hwm_column",
        )
        ```
        HWM value will be fetched using `hwm_column` SQL query.

        If you want to use some SQL expression as HWM value, you can use it as well:

        ```python
        hwm = DBReader.AutoDetectHWM(
            name="some_unique_hwm_name",
            expression="cast(hwm_column_orig as date)",
        )
        ```
        !!! note

            Some sources does not support passing expressions and can be used only with column/field
            names which present in the source.

        !!! info "Changed in 0.10.0"
            Replaces deprecated `hwm_column` and `hwm_expression`  attributes

    hint
        Hint expression used for querying the data.

        `hint` syntax depends on the source. For example, SQL sources
        accept `hint` as a string, but MongoDB sources accept `hint` as a dictionary.

        ```python
        # SQL database connection
        hint = "index(myschema.mytable mycolumn)"

        # MongoDB connection
        hint = {
            "mycolumn": 1,
        }
        ```
        !!! note

            Some sources does not support hints.

    df_schema
        Spark DataFrame schema, used for proper type casting of the rows.

        ```python
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
        ```
        !!! note

            Some sources does not support passing dataframe schema.

    options
        Spark read options, like partitioning mode.

        ```python
        Postgres.ReadOptions(
            partitioningMode="hash",
            partitionColumn="some_column",
            numPartitions=20,
            fetchsize=1000,
        )
        ```
        !!! note

            Some sources does not support reading options.

    Examples
    --------

    === "Minimal example"

        ```python
        from onetl.db import DBReader
        from onetl.connection import Postgres

        postgres = Postgres(...)

        # create reader
        reader = DBReader(connection=postgres, source="fiddle.dummy")

        # read data from table "fiddle.dummy"
        df = reader.run()
        ```

    === "With custom reading options"

        ```python
        from onetl.connection import Postgres
        from onetl.db import DBReader

        postgres = Postgres(...)
        options = Postgres.ReadOptions(sessionInitStatement="select 300", fetchsize="100")

        # create reader and pass some options to the underlying connection object
        reader = DBReader(connection=postgres, source="fiddle.dummy", options=options)

        # read data from table "fiddle.dummy"
        df = reader.run()
        ```

    === "Full example"

        ```python
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
        ```

    === "Incremental reading"

        See [strategy][DBR-onetl-strategy-read-strategies] for more examples

        ```python
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
        ```
    """

    connection: BaseDBConnection
    source: str = Field(alias=avoid_alias("table"))  # type: ignore[literal-required]
    columns: list[str] | None = Field(default=None, min_length=1, validate_default=True)
    where: Any | None = Field(default=None, validate_default=True)
    hint: Any | None = Field(default=None, validate_default=True)
    df_schema: "StructType | None" = Field(default=None, validate_default=True)
    hwm: AutoDetectHWM | ColumnHWM | KeyValueHWM | None = Field(default=None, validate_default=True)
    options: GenericOptions | None = Field(default=None, validate_default=True)

    AutoDetectHWM: ClassVar = AutoDetectHWM

    _connection_checked: bool = PrivateAttr(default=False)

    def __new__(cls, *args, **kwargs):
        try_import_pyspark()

        from pyspark.sql.types import StructType

        _ = StructType

        cls.model_rebuild()
        return super().__new__(cls)

    @field_validator("source", mode="before")
    @classmethod
    def _validate_source(cls, value, info: ValidationInfo):
        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return value
        return connection.dialect.validate_name(value)

    @field_validator("columns", mode="before")
    @classmethod
    def _validate_columns(cls, value, info: ValidationInfo):
        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return value
        return connection.dialect.validate_columns(value)

    @field_validator("where", mode="before")
    @classmethod
    def _validate_where(cls, value, info: ValidationInfo):
        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return value
        result = connection.dialect.validate_where(value)
        if isinstance(result, dict):
            return frozendict(result)  # type: ignore[attr-defined, operator]
        return result

    @field_validator("hint", mode="before")
    @classmethod
    def _validate_hint(cls, value, info: ValidationInfo):
        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return value
        result = connection.dialect.validate_hint(value)
        if isinstance(result, dict):
            return frozendict(result)  # type: ignore[attr-defined, operator]
        return result

    @field_validator("df_schema", mode="before")
    @classmethod
    def _validate_df_schema(cls, value, info: ValidationInfo):
        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return value
        return connection.dialect.validate_df_schema(value)

    @model_validator(mode="before")
    @classmethod
    def _deprecated_hwm_column_to_hwm(cls, values: dict) -> dict:
        connection: BaseDBConnection | None = values.get("connection")
        if not connection:
            return values

        source = values.get("source")
        if not source:
            return values

        hwm_column: str | tuple[str, str] | None = values.pop("hwm_column", None)
        hwm_expression: str | None = values.pop("hwm_expression", None)
        hwm: HWM | None = values.get("hwm")

        if hwm_column:
            if hwm:
                msg = "Please pass either DBReader(hwm=...) or DBReader(hwm_column=...), not both"
                raise ValueError(msg)

            if not hwm_expression and isinstance(hwm_column, tuple):
                hwm_column, hwm_expression = hwm_column

                if not hwm_expression:
                    error_message = textwrap.dedent(
                        """
                        When the 'hwm_column' field is a tuple, then it must be
                        specified as tuple('column_name', 'expression').

                        Otherwise, the 'hwm_column' field should be a string.
                        """,
                    )
                    raise ValueError(error_message)

            process_name, hostname = get_process_info()
            hwm_column = cast("str", hwm_column)
            hwm_expression = cast("str | None", hwm_expression)
            # backported HWM.qualified_name from etl_entities v1/v2
            qualified_name = f"{hwm_column}#{source}@{connection.instance_url}#{process_name}@{hostname}"
            warnings.warn(
                textwrap.dedent(
                    f"""
                    Passing "hwm_column" in DBReader class is deprecated since version 0.10.0,
                    and will be removed in v1.0.0.

                    Instead use:
                        hwm=DBReader.AutoDetectHWM(
                            name={qualified_name!r},
                            expression={hwm_column!r},
                        )
                    """,
                ),
                UserWarning,
                stacklevel=2,
            )

            hwm = AutoDetectHWM(
                name=qualified_name,
                expression=hwm_expression or hwm_column,
            )

        values["hwm"] = hwm
        return values

    # etl-entities v1 uses pydantic v1 models
    # which are not compatible with pydantic v2.
    # using a plain validator here
    @field_validator("hwm", mode="plain")
    @classmethod
    def _validate_hwm(cls, hwm, info: ValidationInfo):
        if not hwm:
            return None

        if not isinstance(hwm, (ColumnHWM, KeyValueHWM, AutoDetectHWM)):
            hwm = HWMTypeRegistry.parse(hwm)

        if not isinstance(hwm, (ColumnHWM, KeyValueHWM, AutoDetectHWM)):
            msg = f"Expected ColumnHWM or KeyValueHWM, got {hwm.__class__.__name__}"
            raise ValueError(msg)  # noqa: TRY004

        hwm = cast("ColumnHWM | KeyValueHWM | AutoDetectHWM", hwm)
        if not hwm.expression:
            msg = "`hwm.expression` cannot be None"
            raise ValueError(msg)

        source = info.data.get("source")
        if not hwm.entity:
            hwm = hwm.copy(update={"entity": source})

        if hwm.entity != source:
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

        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return hwm

        return connection.dialect.validate_hwm(hwm)

    @field_validator("options", mode="before")
    @classmethod
    def _validate_options(cls, value, info: ValidationInfo):
        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return value

        read_options_class = getattr(connection, "ReadOptions", None)
        if read_options_class:
            return read_options_class.parse(value)

        if value:
            msg = f"{connection.__class__.__name__} does not implement ReadOptions, but {value!r} is passed"
            raise ValueError(msg)

        return None

    @slot
    def has_data(self) -> bool:
        """Returns `True` if there is some data in the source, `False` otherwise. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! note

            This method can return different results depending on [strategy][DBR-onetl-strategy-read-strategies]

        !!! warning

            If [hwm](https://etl-entities.readthedocs.io/en/stable/hwm/index.html) is used,
            then method should be called inside [strategy][DBR-onetl-strategy-read-strategies] context.
            And vise-versa, if HWM is not used, this method should not be called within strategy.

        !!! success "Added in 0.10.0"

        Raises
        ------
        RuntimeError
            Current strategy is not compatible with HWM parameter.

        Examples
        --------

        ```python
        reader = DBReader(...)

        # handle situation when there is no data in the source
        if reader.has_data():
            df = reader.run()
        else:
            # implement your handling logic here
            ...
        ```
        """

        method = f"{self.__class__.__name__}.has_data"
        entity_boundary_log(log, f"{method}() started")
        self._check_strategy()

        if not self._connection_checked:
            self._log_parameters()
            self.connection.check()
            self._connection_checked = True

        with override_job_description(self.connection.spark, f"{self.connection} -> {method}({self.source})"):
            started = time.perf_counter()
            try:
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
                return bool(df.take(1))
            except Exception:
                log.error(  # noqa: TRY400
                    "|%s| Error while reading dataframe",
                    self.__class__.__name__,
                )
                raise
            finally:
                elapsed = naturaldelta(time.perf_counter() - started, minimum_unit="milliseconds")
                entity_boundary_log(log, f"{method}() ended in %s", elapsed, char="-")

    @slot
    def raise_if_no_data(self) -> None:
        """Raises exception `NoDataError` if source does not contain any data. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! note

            This method can return different results depending on [strategy][DBR-onetl-strategy-read-strategies]

        !!! warning

            If [hwm](https://etl-entities.readthedocs.io/en/stable/hwm/index.html) is used,
            then method should be called inside [strategy][DBR-onetl-strategy-read-strategies] context.
            And vise-versa, if HWM is not used, this method should not be called within strategy.

        !!! success "Added in 0.10.0"

        Raises
        ------
        RuntimeError
            Current strategy is not compatible with HWM parameter.

        onetl.exception.NoDataError
            There is no data in source.

        Examples
        --------

        ```python
        reader = DBReader(...)

        # ensure that there is some data in the source before reading it using Spark
        reader.raise_if_no_data()
        ```
        """

        if not self.has_data():
            msg = f"No data in the source: {self.source}"
            raise NoDataError(msg)

    @slot
    def run(self) -> "DataFrame":
        """
        Reads data from source table and saves as Spark dataframe. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! note

            This method can return different results depending on [strategy][DBR-onetl-strategy-read-strategies]

        !!! warning

            If [hwm](https://etl-entities.readthedocs.io/en/stable/index.html) is used,
            then method should be called inside [strategy][DBR-onetl-strategy-read-strategies] context.
            And vise-versa, if HWM is not used, this method should not be called within strategy.

        !!! success "Added in 0.1.0"

        Returns
        -------
        :
            Spark dataframe

        Examples
        --------

        Read data to Spark dataframe:

        ```python
        df = reader.run()
        ```
        """

        method = f"{self.__class__.__name__}.run"
        entity_boundary_log(log, f"{method}() started")
        self._check_strategy()

        if not self._connection_checked:
            self._log_parameters()
            self.connection.check()
            self._connection_checked = True

        with override_job_description(self.connection.spark, f"{self.connection} -> {method}({self.source})"):
            try:
                started = time.perf_counter()
                window, limit = self._calculate_window_and_limit()

                # update the HWM with the stop value
                if self.hwm and window:
                    strategy: HWMStrategy = StrategyManager.get_current()  # type: ignore[assignment]
                    strategy.update_hwm(window.stop_at.value)

                return self.connection.read_source_as_df(
                    source=str(self.source),
                    columns=self.columns,
                    hint=self.hint,
                    where=self.where,
                    df_schema=self.df_schema,
                    window=window,
                    limit=limit,
                    **self._get_read_kwargs(),
                )
            except Exception:
                log.error(  # noqa: TRY400
                    "|%s| Error while reading dataframe",
                    self.__class__.__name__,
                )
                raise
            finally:
                elapsed = naturaldelta(time.perf_counter() - started, minimum_unit="milliseconds")
                entity_boundary_log(log, f"{method}() ended in %s", elapsed, char="-")

    def _check_strategy(self):
        strategy = StrategyManager.get_current()
        class_name = type(self).__name__
        strategy_name = type(strategy).__name__

        if self.hwm:
            if not isinstance(strategy, HWMStrategy):
                msg = (
                    f"{class_name}(hwm=...) cannot be used with {strategy_name}. "
                    "Check documentation DBReader.has_data(): "
                    "https://onetl.readthedocs.io/en/stable/db/db_reader.html#onetl.db.db_reader.db_reader.DBReader.has_data."
                )
                raise RuntimeError(msg)
            self._prepare_hwm(strategy, self.hwm)

        elif isinstance(strategy, HWMStrategy):
            msg = f"{strategy_name} cannot be used without {class_name}(hwm=...)"
            raise RuntimeError(msg)

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

    def _get_hwm_field(self, hwm: HWM) -> "StructField":
        log.info(
            "|%s| Getting Spark type for HWM expression: %r",
            self.__class__.__name__,
            hwm.expression,
        )

        if self.df_schema:
            schema = {field.name.casefold(): field for field in self.df_schema}
            column = hwm.expression.casefold()
            if column not in schema:
                msg = f"HWM column {column!r} not found in dataframe schema"
                raise ValueError(msg)

            result = schema[column]
        elif isinstance(self.connection, ContainsGetDFSchemaMethod):
            df_schema = self.connection.get_df_schema(
                source=self.source,
                columns=[hwm.expression],
                **self._get_read_kwargs(),
            )
            result = df_schema[0]
        else:
            msg = (
                "You should specify `df_schema` field to use DBReader with "
                f"{self.connection.__class__.__name__} connection"
            )
            raise ValueError(msg)

        log.info("|%s| Got Spark field: %s", self.__class__.__name__, result)
        return result

    def _calculate_window_and_limit(self) -> tuple[Window | None, int | None]:
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
            msg = f"{self.connection.__class__.__name__} connection does not support {strategy.__class__.__name__}"
            raise TypeError(msg)

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
            # for IncrementalStrategy fix only max value
            # to avoid difference between real dataframe content and HWM value
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
            log_json(log, self.hint, name="hint")

        if self.columns:
            log_collection(log, "columns", self.columns)

        if self.where:
            log_json(log, self.where, name="where")

        if self.df_schema:
            empty_df = self.connection.spark.createDataFrame([], self.df_schema)
            log_dataframe_schema(log, empty_df)

        if self.hwm:
            log_hwm(log, self.hwm)

        options = self.options.model_dump(by_alias=True, exclude_none=True) if self.options else None
        log_options(log, options)

    def _get_read_kwargs(self) -> dict:
        if self.options:
            return {"options": self.options}

        return {}
