from dataclasses import dataclass, field

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass
class Teradata(DBConnection):
    driver: str = field(init=False, default='com.teradata.jdbc.TeraDriver')
    port: int = 1025

    @property
    def url(self):
        if 'jdbc:' in self.host:
            url = self.host
        else:
            prop = self.extra.copy()
            prop['DATABASE'] = self.schema
            if self.port:
                prop['DBS_PORT'] = self.port

            schema_items = [f'{k}={v}' for k, v in prop.items()]
            schema = ','.join(schema_items)

            url = f'jdbc:teradata://{self.host}/{schema}'
        return url

    def _get_timestamp_value_sql(self, value):
        return f'CAST({value.lit()} AS TIMESTAMP)'
