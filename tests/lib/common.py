from pyspark.sql.types import StructType


def insert_into_hive_table(spark, schema, data, write_mode, write_method, table_name):
    df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        StructType(schema),
    )
    getattr(df.write.mode(write_mode), write_method)(table_name)


def dataframe_equal(left_df, right_df):
    return sorted(left_df.collect()) == sorted(right_df.collect())
