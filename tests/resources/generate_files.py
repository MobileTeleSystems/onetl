#!/bin/env python3

from __future__ import annotations

import csv
import datetime
import gzip
import shutil
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import IO

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def create_dataframe(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("str_value", StringType()),
            StructField("int_value", IntegerType()),
            StructField("date_value", DateType()),
            StructField("datetime_value", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )

    data = [
        [1, "val1", 123, datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 1, 1, 1), 1.23],
        [2, "val1", 234, datetime.date(2022, 2, 2), datetime.datetime(2022, 2, 2, 2, 2, 2), 2.34],
        [3, "val2", 345, datetime.date(2023, 3, 3), datetime.datetime(2023, 3, 3, 3, 3, 3), 3.45],
        [4, "val2", 456, datetime.date(2024, 4, 4), datetime.datetime(2024, 4, 4, 4, 4, 4), 4.56],
        [5, "val3", 567, datetime.date(2025, 5, 5), datetime.datetime(2025, 5, 5, 5, 5, 5), 5.67],
        [6, "val3", 678, datetime.date(2026, 6, 6), datetime.datetime(2026, 6, 6, 6, 6, 6), 6.78],
        [7, "val3", 789, datetime.date(2027, 7, 7), datetime.datetime(2027, 7, 7, 7, 7, 7), 7.89],
    ]
    return spark.createDataFrame(data, schema=schema).repartition(1).sortWithinPartitions("id")


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def _write_csv(df: DataFrame, file: IO, header: bool = False, **kwargs) -> None:
    columns = df.columns
    writer = csv.DictWriter(file, fieldnames=columns, **kwargs)

    if header:
        writer.writeheader()

    for row in df.collect():
        writer.writerow(row.asDict())


def save_as_csv_without_header(df: DataFrame, path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "file.csv", "w", newline="") as file:
        _write_csv(df, file)


def save_as_csv_with_header(df: DataFrame, path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "file.csv", "w", newline="") as file:
        _write_csv(df, file, header=True)


def save_as_csv_with_delimiter(df: DataFrame, path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "file.csv", "w", newline="") as file:
        _write_csv(df, file, delimiter=";")


def save_as_csv_with_compression(df: DataFrame, path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "file.csv.gz", "wb") as file:
        with gzip.open(file, "wt") as gzfile:
            _write_csv(df, gzfile)


def save_as_csv_nested(df: DataFrame, path: Path) -> None:
    path.joinpath("some/path").mkdir(parents=True, exist_ok=True)
    path.joinpath("some/path/more").mkdir(parents=True, exist_ok=True)
    path.joinpath("some/path/more/even_more").mkdir(parents=True, exist_ok=True)
    with open(path / "some/path/for_val1.csv", "w", newline="") as file:
        _write_csv(df.filter(col("str_value") == "val1"), file)
    with open(path / "some/path/more/for_val2.csv", "w", newline="") as file:
        _write_csv(df.filter(col("str_value") == "val2"), file)
    with open(path / "some/path/more/even_more/for_val3.csv", "w", newline="") as file:
        _write_csv(df.filter(col("str_value") == "val3"), file)


def save_as_csv_partitioned(df: DataFrame, path: Path) -> None:
    path.joinpath("str_value=val1").mkdir(parents=True, exist_ok=True)
    path.joinpath("str_value=val2").mkdir(parents=True, exist_ok=True)
    path.joinpath("str_value=val3").mkdir(parents=True, exist_ok=True)
    with open(path / "str_value=val1/file.csv", "w", newline="") as file:
        _write_csv(df.filter(col("str_value") == "val1").drop("str_value"), file)
    with open(path / "str_value=val2/file.csv", "w", newline="") as file:
        _write_csv(df.filter(col("str_value") == "val2").drop("str_value"), file)
    with open(path / "str_value=val3/file.csv", "w", newline="") as file:
        _write_csv(df.filter(col("str_value") == "val3").drop("str_value"), file)


def save_as_csv(df: DataFrame, path: Path) -> None:
    root = path / "csv"
    shutil.rmtree(root)

    save_as_csv_without_header(df, root / "without_header")
    save_as_csv_with_header(df, root / "with_header")
    save_as_csv_with_delimiter(df, root / "with_delimiter")
    save_as_csv_with_compression(df, root / "with_compression")
    save_as_csv_nested(df, root / "nested")
    save_as_csv_partitioned(df, root / "partitioned")


format_mapping = {
    "csv": save_as_csv,
}


def get_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument("--path", type=Path, default="tests/resources")
    parser.add_argument("--format", type=str, required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = get_parser()
    args = parser.parse_args(argv or sys.argv[1:])
    path = args.path

    spark = get_spark()
    df = create_dataframe(spark)

    if args.format not in format_mapping:
        raise ValueError(f"Format {args.format} is not supported")

    save_func = format_mapping[args.format]
    save_func(df, path / "file_df_connection")


if __name__ == "__main__":
    main()
