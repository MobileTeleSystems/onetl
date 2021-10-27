from dataclasses import dataclass, field
from onetl.connection.db_connection.db_connection import DBConnection


@dataclass(frozen=True)
class Postgres(DBConnection):
    """Class for Postgres jdbc connection.

    Parameters
    ----------
    host : str
        Host of postgres database. For example: ``test-db-vip.msk.mts.ru``
    port : int, optional, default: ``5432``
        Port of postgres database
    user : str, default: ``None``
        User, which have access to the database and table. For example: ``appmetrica_test``
    password : str, default: ``None``
        Password for database connection
    database : str, default: ``default``
        Database in rdbms. To provide schema, use DBReader class

        Difference like https://www.educba.com/postgresql-database-vs-schema/
    spark : pyspark.sql.SparkSession, default: ``None``
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization.

    Examples
    --------

    Postgres jdbc connection initialization

    .. code::

        from onetl.connection.db_connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Postgres.package,
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="appmetrica_test",
            password="*****",
            database='target_database',
            spark=spark,
        )

    """

    driver: str = field(init=False, default="org.postgresql.Driver")
    package: str = field(init=False, default="org.postgresql:postgresql:42.2.5")
    port: int = 5432

    @property
    def url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def _get_timestamp_value_sql(self, value):
        return f"{value.lit()}::timestamp"
