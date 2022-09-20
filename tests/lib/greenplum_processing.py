import os

from tests.lib.postgres_processing import PostgresProcessing


class GreenplumProcessing(PostgresProcessing):
    @property
    def user(self) -> str:
        return os.getenv("ONETL_GP_CONN_USER")

    @property
    def password(self) -> str:
        return os.getenv("ONETL_GP_CONN_PASSWORD")

    @property
    def host(self) -> str:
        return os.getenv("ONETL_GP_CONN_HOST")

    @property
    def port(self) -> int:
        return int(os.getenv("ONETL_GP_CONN_PORT"))

    @property
    def database(self) -> str:
        return os.getenv("ONETL_GP_CONN_DATABASE")

    @property
    def schema(self) -> str:
        return os.getenv("ONETL_GP_CONN_SCHEMA", "onetl")

    @property
    def extra(self) -> dict:
        return {"server.hostEnv": "SPARK_EXTERNAL_IP"}
