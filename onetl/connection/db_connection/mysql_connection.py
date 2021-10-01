from dataclasses import dataclass, field

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass(frozen=True)
class MySQL(DBConnection):
    driver: str = field(init=False, default="com.mysql.jdbc.Driver")
    port: int = 3306

    @property
    def url(self):
        prop = self.extra.copy()
        prop["useUnicode"] = "yes"
        prop["characterEncoding"] = "UTF-8"
        params = "&".join(f"{k}={v}" for k, v in prop.items())

        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}?{params}"

    def _get_timestamp_value_sql(self, value):
        return f"STR_TO_DATE({value.lit()}, '%Y-%m-%d %H:%i:%s.%f')"  # noqa: WPS323
