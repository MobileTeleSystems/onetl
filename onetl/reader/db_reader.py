from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Union, Mapping, Tuple

from mts_atlas_client.hwm import HWM

from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)


@dataclass
class DBReader:
    connection: DBConnection
    table: str
    # TODO: нужна для dataset qualified name, но в целом безполезно, может убрать?
    process_name: Optional[str] = None
    cluster_name: Optional[str] = None
    columns: Union[str, list] = None
    hwm_columns: Optional[str] = None
    where: Optional[str] = ''
    hint: Optional[str] = ''
    check_source: bool = False
    crutch_oracle: bool = False
    spark_read_conf: Optional[Mapping] = field(default_factory=dict)

    def __post_init__(self):
        self.columns = ', '.join(self.columns) if isinstance(self.columns, list) else self.columns or '*'

    @property
    def dataset_qualified_name(self):
        # TODO: Насколько правильно, возможно стоит переработать?
        return f'{self.hwm_columns}@{self.process_name}@{self.table}@{self.cluster_name}'

    def read(self):
        conf = self.spark_read_conf.copy()

        conf['properties']['user'] = self.connection.login
        conf['properties']['password'] = self.connection.password
        conf['properties']['driver'] = self.connection.driver

        hwm_old, hwm_new = self.get_hwm_old_and_new(self.connection.spark, conf)

        sql_text = self.get_sql_text(hwm_old, hwm_new)
        log.info(f'SQL statement: {sql_text}')

        num_partitions = conf.get('numPartitions')
        partition_column = conf.get('column')
        lower = conf.get('lowerBound')
        upper = conf.get('upperBound')

        if num_partitions is not None:
            if partition_column is not None and (lower is None or upper is None) and hwm_old and hwm_new:
                lower = hwm_old.casted_value
                upper = hwm_new.casted_value

            if partition_column is None:
                log.warning("read_column task parameter wasn't specified; the reading will be slowed down!")
            else:
                conf['numPartitions'] = num_partitions
                conf['column'] = partition_column
                conf['lowerBound'] = lower
                conf['upperBound'] = upper

        prepare = conf['properties'].get('sessionInitStatement')
        if prepare:
            log.info(f'Init SQL statement: {prepare}')

        return self.connection.spark.read.jdbc(url=self.connection.url, table=sql_text, **conf)

    def get_sql_text(self, hwm_old, hwm_new):
        if (hwm_old and hwm_new) or self.columns != '*' or self.where or self.hint:
            statements = [
                'SELECT ',
                self.hint,
                self.columns,
                ' FROM {0}'.format(self.table),
            ]

            where = []
            if self.where:
                where.append('({0})'.format(self.where))
            if hwm_old and hwm_new:
                where.append(
                    '({column} > {hwm_old} AND {column} <= {hwm_new})'.format(
                        column=hwm_old.hwm_name,
                        hwm_old=self.connection.get_value_sql(hwm_old),
                        hwm_new=self.connection.get_value_sql(hwm_new),
                    ),
                )
            if where:
                statements.append(' WHERE ' + ' AND '.join(where))

            sql = '({sql}) T'.format(sql=' '.join(statements))
        else:
            sql = '{sql} T'.format(sql=self.table)
        return sql

    def get_hwm_old_and_new(self, spark, spark_read_conf) -> Tuple[Optional[HWM], Optional[HWM]]:
        if self.hwm_columns:
            statements = [
                'SELECT',
                self.hint,
                'MIN({0}) AS hwm_min,'.format(self.hwm_columns),
                'MAX({0}) AS hwm_max'.format(self.hwm_columns),
                'FROM {0}'.format(self.table),
            ]

            if self.where:
                statements.append('WHERE {where}'.format(where=self.where))

            if self.crutch_oracle:
                sql = '({sql} UNION {sql}) T'.format(sql=' '.join(statements))
            else:
                sql = '({sql}) T'.format(sql=' '.join(statements))

            log.info(f'Extreme values sql statement: {sql}.')
            df = spark.read.jdbc(
                url=self.connection.url,
                table=sql,
                properties=spark_read_conf or None,
            )

            data = df.collect()
            if not data or data[0] is None:
                RuntimeError('Extreme value for {column} is NULL'.format(column=self.hwm_columns))

            log.info('HWM raw type: {}'.format(type(data[0][0]).__name__))

            hwm_old = HWM.from_raw_value(
                hwm_name=self.hwm_columns,
                process_name=self.process_name,
                dataset_name=self.dataset_qualified_name,
                value=data[0][0],
            )
            hwm_new = HWM.from_raw_value(
                hwm_name=self.hwm_columns,
                process_name=self.process_name,
                dataset_name=self.dataset_qualified_name,
                value=data[0][1],
            )

            log.info(f'HWM old: {hwm_old} = {hwm_old.value}')
            log.info(f'HWM new: {hwm_new} = {hwm_new.value}')
            return hwm_old - 1, hwm_new
        return None, None
