from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Union, List, Dict

from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)
# TODO:(@mivasil6) implement logging


@dataclass
class DBReader:
    """Class allows you to read data from a table with specified connection
    and parameters and save it as Spark dataframe

    Parameters
    ----------
    connection: onetl.connection.db_connection.DBConnection
        Class which contain DB connection properties. See in DBConnection section.
    table : str
        Table name from which to read. You need to specify the full path to the table, including the schema.
        Name like ``schema.name``
    columns : list of str, optional, default: ``*``
        The list of columns to be read
    sql_where : str, optional, default: ``None``
        Custom ``where`` for SQL query
    sql_hint : str, optional, default: ``None``
        Add sql hint to SQL query
    jdbc_options : dict, optional, default: ``None``
        Spark jdbc read options.
        For example: ``{"partitionColumn": "some_column", "numPartitions": 20, "fetchsize": 1000}``

        You can find all list of options in link below:

        https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

        .. warning ::

            Doesn't work on ``Hive`` DBConnection


    Examples
    --------
    Simple Reader creation

    .. code::

        from onetl.reader import DBReader
        from onetl.connection.db_connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Postgres.package,
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

         reader = DBReader(postgres, table="fiddle.dummy")

    RDBMS table with jdbc_options

    .. code::

        from onetl.reader import DBReader
        from onetl.connection.db_connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Postgres.package,
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
        jdbc_options = {"sessionInitStatement": "select 300", "fetchsize": "100"}

        reader = DBReader(postgres, table="fiddle.dummy", jdbc_options=jdbc_options)

    Reader creation with all params:

    .. code::

        from onetl.reader import DBReader
        from onetl.connection.db_connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Postgres.package,
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
        jdbc_options = {"sessionInitStatement": "select 300", "fetchsize": "100"}

        reader = DBReader(
            connection=postgres,
            table="default.test",
            sql_where="d_id > 100",
            sql_hint="NOWAIT",
            columns=["d_id", "d_name", "d_age"],
            jdbc_options=jdbc_options,
        )

    Reader for Hive with all available params:

    .. code::

        from onetl.connection.db_connection import Hive
        from onetl.reader import DBReader
        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        hive = Hive(spark=spark)

        reader = DBReader(
            connection=hive,
            table="default.test",
            sql_where="d_id > 100",
            sql_hint="NOWAIT",
            columns=["d_id", "d_name", "d_age"],
        )
    """

    connection: DBConnection
    # table is 'schema.table'
    table: str
    columns: Optional[Union[str, List]] = "*"
    sql_where: Optional[str] = ""
    sql_hint: Optional[str] = ""
    jdbc_options: Dict = field(default_factory=dict)

    def run(self) -> "pyspark.sql.DataFrameReader":
        """
        Reads data from source table and saves as Spark dataframe

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Read df

        .. code::

            df = reader.run()

        """

        return self.connection.read_table(
            jdbc_options=self.jdbc_options,
            sql_hint=self.sql_hint,
            columns=", ".join(self.columns) if isinstance(self.columns, list) else self.columns,
            sql_where=self.sql_where,
            table=self.table,
        )
