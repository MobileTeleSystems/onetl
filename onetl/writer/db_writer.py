from dataclasses import dataclass, field
from logging import getLogger
from typing import Mapping

from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)


@dataclass
class DBWriter:
    connection: DBConnection
    table: str
    write_mode: str = 'append'
    write_format: str = 'orc'
    spark_write_config: Mapping = field(default_factory=dict)

    def run(self, df):
        df_out = self.prepare_writer(df)

        self.connection.save_df(df_out, self.table, self.spark_write_config)

    def prepare_writer(self, df):
        """
        :type df: pyspark.sql.DataFrame

        :rtype pyspark.sql.DataFrameWriter
        """
        writer = df.write

        spark_conf = self.spark_write_config.copy()
        spark_conf.update(mode=self.write_mode, format=self.write_format)

        for prop, value in self.spark_write_config.items():
            # Custom write options example:
            #   mode: overwrite
            #   format: parquet
            #   options:
            #     spark.sql.parquet.compression.codec: snappy
            #     spark.sql.parquet.filterPushdown: 'true'
            if prop == 'options':
                writer = writer.options(**value)
            # Options like "mode" and "format"
            elif prop != 'properties':
                writer = getattr(writer, prop)(value)  # Like, writer = writer.mode('overwrite')
            # Else, don't set "properties" -- they'll be used in write.jdbc

        return writer
