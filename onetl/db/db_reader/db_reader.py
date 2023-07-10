from __future__ import annotations

import textwrap
from logging import getLogger
from typing import TYPE_CHECKING, Any, List, Optional

import frozendict
from etl_entities import Column, Table
from pydantic import Field, root_validator, validator

from onetl._internal import uniq_ignore_case  # noqa: WPS436
from onetl.base import BaseDBConnection
from onetl.base.contains_get_df_schema import ContainsGetDFSchemaMethod
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel, GenericOptions
from onetl.log import (
    entity_boundary_log,
    log_collection,
    log_dataframe_schema,
    log_json,
    log_options,
    log_with_indent,
)

log = getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.types import StructType


@support_hooks
class DBReader(FrozenModel):
    """Allows you to read data from a table with specified database connection
    and parameters, and return its content as Spark dataframe. |support_hooks|

    .. note::

        DBReader can return different results depending on :ref:`strategy`

    .. note::

        This class operates with only one table at a time. It does NOT support executing JOINs.

        To get the JOIN result you can instead:

            1. Use 2 instandes of DBReader with different tables,
               call :obj:`~run` of each one to get a table dataframe,
               and then use ``df1.join(df2)`` syntax (Hive)

            2. Use ``connection.execute("INSERT INTO ... SELECT ... JOIN ...")``
               to execute JOIN on RDBMS side, write the result into a temporary table,
               and then use DBReader to get the data from this temporary table (MPP systems, like Greenplum)

            3. Use ``connection.sql(query)`` method to pass SQL query with a JOIN,
               and fetch the result (other RDBMS)

    Parameters
    ----------
    connection : :obj:`onetl.connection.BaseDBConnection`
        Class which contains DB connection properties. See :ref:`db-connections` section

    source : str
        Table/collection/etc name to read data from.

        If connection has schema support, you need to specify the full name of the source
        including the schema, e.g. ``schema.name``.

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

            Some connections does not have columns.

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

    hwm_column : str or tuple[str, any], default: ``None``
        Column to be used as :ref:`column-hwm` value.

        If you want to use some SQL expression as HWM value, you can pass it as tuple
        ``("column_name", "expression")``, like:

        .. code:: python

            hwm_column = ("hwm_column", "cast(hwm_column_orig as date)")

        HWM value will be fetched using ``max(cast(hwm_column_orig as date)) as hwm_column`` SQL query.

        .. note::

            Some sources does not support ``("column_name", "expression")`` syntax.

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
    Simple Reader creation:

    .. code:: python

        from onetl.db import DBReader
        from onetl.connection import Postgres
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Postgres.package)
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        # create reader
        reader = DBReader(connection=postgres, source="fiddle.dummy")

        # read data from table "fiddle.dummy"
        df = reader.run()

    Reader creation with JDBC options:

    .. code:: python

        from onetl.db import DBReader
        from onetl.connection import Postgres
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Postgres.package)
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
        options = {"sessionInitStatement": "select 300", "fetchsize": "100"}
        # or (it is the same):
        options = Postgres.ReadOptions(sessionInitStatement="select 300", fetchsize="100")

        # create reader and pass some options to the underlying connection object
        reader = DBReader(connection=postgres, source="fiddle.dummy", options=options)

        # read data from table "fiddle.dummy"
        df = reader.run()

    Reader creation with all parameters:

    .. code:: python

        from onetl.db import DBReader
        from onetl.connection import Postgres
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Postgres.package)
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
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

    Incremental Reader:

    .. code:: python

        from onetl.db import DBReader
        from onetl.connection import Postgres
        from onetl.strategy import IncrementalStrategy
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Postgres.package)
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        reader = DBReader(
            connection=postgres,
            source="fiddle.dummy",
            hwm_column="d_age",  # mandatory for IncrementalStrategy
        )

        # read data from table "fiddle.dummy"
        # but only with new rows (`WHERE d_age > previous_hwm_value`)
        with IncrementalStrategy():
            df = reader.run()
    """

    connection: BaseDBConnection
    source: Table = Field(alias="table")
    columns: Optional[List[str]] = None
    hwm_column: Optional[Column] = None
    hwm_expression: Optional[str] = None
    where: Optional[Any] = None
    hint: Optional[Any] = None
    df_schema: Optional[StructType] = None
    options: Optional[GenericOptions] = None

    @validator("source", pre=True, always=True)
    def validate_source(cls, source, values):
        connection: BaseDBConnection = values["connection"]
        dialect = connection.Dialect
        if isinstance(source, str):
            # source="dbschema.table" or source="table", If source="dbschema.some.table" in class Table will raise error.
            source = Table(name=source, instance=connection.instance_url)
            # Here Table(name='source', db='dbschema', instance='some_instance')
        return dialect.validate_name(connection, source)

    @validator("where", pre=True, always=True)
    def validate_where(cls, where: Any, values: dict) -> Any:
        connection: BaseDBConnection = values["connection"]
        dialect = connection.Dialect
        result = dialect.validate_where(connection, where)
        if isinstance(result, dict):
            return frozendict.frozendict(result)  # type: ignore[attr-defined, operator]
        return result

    @validator("hint", pre=True, always=True)
    def validate_hint(cls, hint: Any, values: dict) -> Any:
        connection: BaseDBConnection = values["connection"]
        dialect = connection.Dialect
        result = dialect.validate_hint(connection, hint)
        if isinstance(result, dict):
            return frozendict.frozendict(result)  # type: ignore[attr-defined, operator]
        return result

    @validator("df_schema", pre=True, always=True)
    def validate_df_schema(cls, df_schema: StructType | None, values: dict) -> StructType | None:
        connection: BaseDBConnection = values["connection"]
        dialect = connection.Dialect
        return dialect.validate_df_schema(connection, df_schema)

    @root_validator(pre=True)  # noqa: WPS231
    def validate_hwm_column(cls, values: dict) -> dict:
        hwm_column: str | tuple[str, str] | Column | None = values.get("hwm_column")
        df_schema: StructType | None = values.get("df_schema")
        hwm_expression: str | None = values.get("hwm_expression")

        if hwm_column is None or isinstance(hwm_column, Column):
            # nothing to validate
            return values

        if not hwm_expression and not isinstance(hwm_column, str):
            # ("new_hwm_column", "cast(hwm_column as date)")  noqa: E800
            hwm_column, hwm_expression = hwm_column  # noqa: WPS434

            if not hwm_expression:
                raise ValueError(
                    "When the 'hwm_column' field is a tuple, then it must be "
                    "specified as tuple('column_name', 'expression'). Otherwise, "
                    "the 'hwm_column' field should be a string.",
                )

        if df_schema is not None and hwm_column not in df_schema.fieldNames():
            raise ValueError(
                "'df_schema' struct must contain column specified in 'hwm_column'. "
                "Otherwise DBReader cannot determine HWM type for this column",
            )

        values["hwm_column"] = Column(name=hwm_column)  # type: ignore
        values["hwm_expression"] = hwm_expression

        return values

    @root_validator(pre=True)  # noqa: WPS231
    def validate_columns(cls, values: dict) -> dict:
        connection: BaseDBConnection = values["connection"]
        dialect = connection.Dialect

        columns: list[str] | str | None = values.get("columns")
        columns_list: list[str] | None
        if isinstance(columns, str):
            columns_list = columns.split(",")
        else:
            columns_list = columns

        columns_list = dialect.validate_columns(connection, columns_list)
        if columns_list is None:
            return values

        if not columns_list:
            raise ValueError("Parameter 'columns' can not be an empty list")

        hwm_column = values.get("hwm_column")
        hwm_expression = values.get("hwm_expression")

        result: list[str] = []
        already_visited: set[str] = set()

        for item in columns_list:
            column = item.strip()

            if not column:
                raise ValueError(f"Column name cannot be empty string, got {item!r}")

            if column.casefold() in already_visited:
                raise ValueError(f"Duplicated column name {item!r}")

            if hwm_expression and hwm_column and hwm_column.name.casefold() == column.casefold():
                raise ValueError(f"{item!r} is an alias for HWM, it cannot be used as 'columns' name")

            result.append(column)
            already_visited.add(column.casefold())

        values["columns"] = result
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

    @validator("hwm_expression", pre=True, always=True)  # noqa: WPS238, WPS231
    def validate_hwm_expression(cls, hwm_expression, values):
        connection: BaseDBConnection = values["connection"]
        dialect = connection.Dialect
        return dialect.validate_hwm_expression(connection=connection, value=hwm_expression)

    def get_df_schema(self) -> StructType:
        if self.df_schema:
            return self.df_schema

        if not self.df_schema and isinstance(self.connection, ContainsGetDFSchemaMethod):
            return self.connection.get_df_schema(
                source=str(self.source),
                columns=self._resolve_all_columns(),
                **self._get_read_kwargs(),
            )

        raise ValueError(
            "|DBReader| You should specify `df_schema` field to use DBReader with "
            f"{self.connection.__class__.__name__} connection",
        )

    def get_min_max_bounds(self, column: str, expression: str | None = None) -> tuple[Any, Any]:
        return self.connection.get_min_max_bounds(
            source=str(self.source),
            column=column,
            expression=expression,
            hint=self.hint,
            where=self.where,
            **self._get_read_kwargs(),
        )

    @slot
    def run(self) -> DataFrame:
        """
        Reads data from source table and saves as Spark dataframe. |support_hooks|

        .. note::

            This method can return different results depending on :ref:`strategy`

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        .. note::

            Keep in mind that with differences in the timezone settings of the source and Spark,
            there may be discrepancies in the datetime on the source and in the Spark dataframe.
            It depends on the ``spark.sql.session.timeZone`` option set when creating the Spark session.

        Examples
        --------

        Read data to Spark dataframe:

        .. code:: python

            df = reader.run()
        """

        # avoid circular imports
        from onetl.db.db_reader.strategy_helper import (
            HWMStrategyHelper,
            NonHWMStrategyHelper,
            StrategyHelper,
        )

        entity_boundary_log(msg="DBReader starts")

        self._log_parameters()
        self._log_options()
        self.connection.check()

        helper: StrategyHelper
        if self.hwm_column:
            helper = HWMStrategyHelper(reader=self, hwm_column=self.hwm_column, hwm_expression=self.hwm_expression)
        else:
            helper = NonHWMStrategyHelper(reader=self)

        start_from, end_at = helper.get_boundaries()

        df = self.connection.read_df(
            source=str(self.source),
            columns=self._resolve_all_columns(),
            hint=self.hint,
            where=self.where,
            df_schema=self.df_schema,
            start_from=start_from,
            end_at=end_at,
            **self._get_read_kwargs(),
        )

        df = helper.save(df)
        entity_boundary_log(msg="DBReader ends", char="-")

        return df

    def _log_parameters(self) -> None:
        log.info("|%s| -> |Spark| Reading DataFrame from source using parameters:", self.connection.__class__.__name__)
        log_with_indent("source = '%s'", self.source)

        if self.hint:
            log_json(self.hint, "hint")

        if self.columns:
            log_collection("columns", self.columns)

        if self.where:
            log_json(self.where, "where")

        if self.hwm_column:
            log_with_indent("hwm_column = '%s'", self.hwm_column)

        if self.hwm_expression:
            log_json(self.hwm_expression, "hwm_expression")

        if self.df_schema:
            empty_df = self.connection.spark.createDataFrame([], self.df_schema)  # type: ignore
            log_dataframe_schema(empty_df)

    def _log_options(self) -> None:
        options = self.options.dict(by_alias=True, exclude_none=True) if self.options else None
        log_options(options)

    def _resolve_all_columns(self) -> list[str] | None:
        """
        Unwraps "*" in columns list to real column names from existing table.

        Also adds 'hwm_column' to the result if it is not present.
        """

        if not isinstance(self.connection, ContainsGetDFSchemaMethod):
            # Some databases have no `get_df_schema` method
            return self.columns

        columns: list[str] = []
        original_columns = self.columns or ["*"]

        for column in original_columns:
            if column == "*":
                schema = self.connection.get_df_schema(
                    source=str(self.source),
                    columns=["*"],
                    **self._get_read_kwargs(),
                )
                field_names = schema.fieldNames()
                columns.extend(field_names)
            else:
                columns.append(column)

        columns = uniq_ignore_case(columns)

        if not self.hwm_column:
            return columns

        hwm_statement = self.hwm_column.name
        if self.hwm_expression:
            hwm_statement = self.connection.Dialect._expression_with_alias(  # noqa: WPS437
                self.hwm_expression,
                self.hwm_column.name,
            )

        columns_normalized = [column_name.casefold() for column_name in columns]
        hwm_column_name = self.hwm_column.name.casefold()

        if hwm_column_name in columns_normalized:
            column_index = columns_normalized.index(hwm_column_name)
            columns[column_index] = hwm_statement
        else:
            columns.append(hwm_statement)

        return columns

    def _get_read_kwargs(self) -> dict:
        if self.options:
            return {"options": self.options}

        return {}

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        # avoid importing pyspark unless user called the constructor,
        # as we allow user to use `Connection.package` for creating Spark session

        refs = super()._forward_refs()
        try:
            from pyspark.sql.types import StructType  # noqa: WPS442
        except (ImportError, NameError) as e:
            raise ImportError(
                textwrap.dedent(
                    f"""
                    Cannot import module "pyspark".

                    Since onETL v0.7.0 you should install package as follows:
                        pip install onetl[spark]

                    or inject PySpark to sys.path in some other way BEFORE creating {cls.__name__} instance.
                    """,
                ).strip(),
            ) from e

        refs["StructType"] = StructType
        return refs
