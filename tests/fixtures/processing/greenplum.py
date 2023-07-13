import os

from tests.fixtures.processing.postgres import PostgresProcessing


class GreenplumProcessing(PostgresProcessing):
    @property
    def user(self) -> str:
        return os.environ["ONETL_GP_USER"]

    @property
    def password(self) -> str:
        return os.environ["ONETL_GP_PASSWORD"]

    @property
    def host(self) -> str:
        return os.environ["ONETL_GP_HOST"]

    @property
    def port(self) -> int:
        return int(os.environ["ONETL_GP_PORT"])

    @property
    def database(self) -> str:
        return os.environ["ONETL_GP_DATABASE"]

    @property
    def schema(self) -> str:
        return os.getenv("ONETL_GP_SCHEMA", "onetl")

    @property
    def extra(self) -> dict:
        return {"server.hostEnv": "SPARK_EXTERNAL_IP"}
