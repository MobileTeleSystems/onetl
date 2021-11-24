from dataclasses import dataclass, field
from logging import getLogger
from typing import Dict

from onetl.connection.db_connection import DBConnection
from onetl.connection.db_connection.hive_connection import Hive


log = getLogger(__name__)
# TODO:(@mivasil6) implement logging


@dataclass
class DBWriter:
    """Class specifies database and table where you can write your dataframe.

    Parameters
    ----------
    connection : onetl.connection.db_connection.DBConnection
        Class which contain DB connection properties. See in DBConnection section.
    table : str
        Table from which we read. You need to specify the full path to the table, including the schema.
        Like ``schema.name``
    format : str, optional, default: ``orc``
        Format of written data. Can be ``json``, ``parquet``, ``jdbc``, ``orc``, ``libsvm``, ``csv``, ``text``
    mode : str, optional, default: ``append``
        The way of handling errors when table is already exists.

        Possible values:
            * ``overwrite``
                Remove old table data and write new one
            * ``append``
                Append data to a table
            * ``ignore``
                Don't write anything
            * ``error``
                Raise exception
    jdbc_options : dict, optional, default: ``None``
        Spark jdbc write options.

        For example: ``{"truncate": "true", "batchsize": 1000}``

        You can find all list of options in link below:

        https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

        .. warning ::

            Doesn't work on ``Hive`` DBConnection


    Examples
    --------
    Simple Writer creation

    .. code::

        from onetl.writer import DBWriter
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

         writer = DBWriter(
          connection=postgres,
          table="fiddle.dummy",
        )

    RDBMS table with jdbc_options

    .. code::

        from onetl.writer import DBWriter
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

        jdbc_options = {"truncate": "true", "batchsize": 1000}

        writer = DBWriter(
          connection=postgres,
          table="fiddle.dummy",
          jdbc_options=jdbc_options,
        )

    Reader creation with all params:

    .. code::

        from onetl.writer import DBWriter
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

        jdbc_options = {"truncate": "true", "batchsize": 1000}

        writer = DBWriter(
          connection=postgres,
          table="default.test",
          mode="overwrite",
          jdbc_options=jdbc_options,
        )

    Reader for Hive with all available params:
    .. code::

        from onetl.writer import DBWriter
        from onetl.connection.db_connection import Hive
        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})
        hive = Hive(spark=spark)

        writer = DBWriter(
          connection=hive,
          table="default.test",
          mode="skip",
        )
    """

    connection: DBConnection
    table: str
    mode: str = field(default="append")
    jdbc_options: Dict = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.connection, Hive) and self.jdbc_options:
            raise ValueError("It is forbidden to pass the jdbc_options parameter if you passed Hive.")

    def run(self, df):
        """
        Method for writing your df to specified table.

        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Write df

        .. code::

            writer.run(df)

        """
        jdbc_options = self.jdbc_options.copy()

        self.connection.save_df(
            df=df,
            table=self.table,
            jdbc_options=jdbc_options,
            mode=self.mode,
        )
