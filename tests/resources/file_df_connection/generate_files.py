#!/bin/env python3

from __future__ import annotations

import csv
import gzip
import io
import json
import os
import shutil
import sys
from argparse import ArgumentParser
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, TextIO


def get_data() -> list[dict]:
    return [
        {
            "id": 1,
            "str_value": "val1",
            "int_value": 123,
            "date_value": date(2021, 1, 1),
            "datetime_value": datetime(2021, 1, 1, 1, 1, 1, tzinfo=timezone.utc),
            "float_value": 1.23,
        },
        {
            "id": 2,
            "str_value": "val1",
            "int_value": 234,
            "date_value": date(2022, 2, 2),
            "datetime_value": datetime(2022, 2, 2, 2, 2, 2, tzinfo=timezone.utc),
            "float_value": 2.34,
        },
        {
            "id": 3,
            "str_value": "val2",
            "int_value": 345,
            "date_value": date(2023, 3, 3),
            "datetime_value": datetime(2023, 3, 3, 3, 3, 3, tzinfo=timezone.utc),
            "float_value": 3.45,
        },
        {
            "id": 4,
            "str_value": "val2",
            "int_value": 456,
            "date_value": date(2024, 4, 4),
            "datetime_value": datetime(2024, 4, 4, 4, 4, 4, tzinfo=timezone.utc),
            "float_value": 4.56,
        },
        {
            "id": 5,
            "str_value": "val3",
            "int_value": 567,
            "date_value": date(2025, 5, 5),
            "datetime_value": datetime(2025, 5, 5, 5, 5, 5, tzinfo=timezone.utc),
            "float_value": 5.67,
        },
        {
            "id": 6,
            "str_value": "val3",
            "int_value": 678,
            "date_value": date(2026, 6, 6),
            "datetime_value": datetime(2026, 6, 6, 6, 6, 6, tzinfo=timezone.utc),
            "float_value": 6.78,
        },
        {
            "id": 7,
            "str_value": "val3",
            "int_value": 789,
            "date_value": date(2027, 7, 7),
            "datetime_value": datetime(2027, 7, 7, 7, 7, 7, tzinfo=timezone.utc),
            "float_value": 7.89,
        },
    ]


def _to_string(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return obj


def _write_csv(data: list[dict], file: TextIO, header: bool = False, **kwargs) -> None:
    columns = list(data[0].keys())
    writer = csv.DictWriter(file, fieldnames=columns, **kwargs)

    if header:
        writer.writeheader()

    for row in data:
        writer.writerow({key: _to_string(value) for key, value in row.items()})


def save_as_csv_without_header(data: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "file.csv", "w", newline="") as file:
        _write_csv(data, file)


def save_as_csv_with_header(data: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "file.csv", "w", newline="") as file:
        _write_csv(data, file, header=True)


def save_as_csv_with_delimiter(data: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "file.csv", "w", newline="") as file:
        _write_csv(data, file, delimiter=";")


def save_as_csv_gz(data: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

    # https://stackoverflow.com/questions/66955482/why-does-recompressing-a-file-using-gzip-produces-a-different-output
    # Instead of just writing data to file we write it to a buffer, and then compress with fixed mtime
    buffer = io.StringIO()
    _write_csv(data, buffer)
    with open(path / "file.csv.gz", "wb") as file:
        with gzip.GzipFile(fileobj=file, mode="w", mtime=0) as gzfile:
            gzfile.write(buffer.getvalue().encode("utf-8"))


def save_as_csv_nested(data: list[dict], path: Path) -> None:
    path.joinpath("some/path").mkdir(parents=True, exist_ok=True)
    path.joinpath("some/path/more").mkdir(parents=True, exist_ok=True)
    path.joinpath("some/path/more/even_more").mkdir(parents=True, exist_ok=True)

    with open(path / "some/path/for_val1.csv", "w", newline="") as file:
        data_for_val1 = [row for row in data if row["str_value"] == "val1"]
        _write_csv(data_for_val1, file)

    with open(path / "some/path/more/for_val2.csv", "w", newline="") as file:
        data_for_val2 = [row for row in data if row["str_value"] == "val2"]
        _write_csv(data_for_val2, file)

    with open(path / "some/path/more/even_more/for_val3.csv", "w", newline="") as file:
        data_for_val3 = [row for row in data if row["str_value"] == "val3"]
        _write_csv(data_for_val3, file)


def save_as_csv_partitioned(data: list[dict], path: Path) -> None:
    def filter_and_drop(rows: list[dict], column: str, value: Any) -> list[dict]:
        result = []
        for row in rows:
            if row[column] == value:
                row_copy = row.copy()
                row_copy.pop(column)
                result.append(row_copy)
        return result

    path.joinpath("str_value=val1").mkdir(parents=True, exist_ok=True)
    path.joinpath("str_value=val2").mkdir(parents=True, exist_ok=True)
    path.joinpath("str_value=val3").mkdir(parents=True, exist_ok=True)

    columns = list(data[0].keys())
    columns.remove("str_value")
    with open(path / "str_value=val1/file.csv", "w", newline="") as file:
        data_for_val1 = filter_and_drop(data, "str_value", "val1")
        _write_csv(data_for_val1, file)

    with open(path / "str_value=val2/file.csv", "w", newline="") as file:
        data_for_val2 = filter_and_drop(data, "str_value", "val2")
        _write_csv(data_for_val2, file)

    with open(path / "str_value=val3/file.csv", "w", newline="") as file:
        data_for_val3 = filter_and_drop(data, "str_value", "val3")
        _write_csv(data_for_val3, file)


def save_as_csv(data: list[dict], path: Path) -> None:
    root = path / "csv"
    shutil.rmtree(root, ignore_errors=True)

    save_as_csv_without_header(data, root / "without_header")
    save_as_csv_with_header(data, root / "with_header")
    save_as_csv_with_delimiter(data, root / "with_delimiter")
    save_as_csv_gz(data, root / "with_compression")
    save_as_csv_nested(data, root / "nested")
    save_as_csv_partitioned(data, root / "partitioned")


def save_as_json_plain(data: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.joinpath("file.json").write_text(json.dumps(data, default=_to_string))


def save_as_json_gz(data: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO()
    json.dump(data, buffer, default=_to_string)
    with open(path / "file.json.gz", "wb") as file:
        with gzip.GzipFile(fileobj=file, mode="w", mtime=0) as gzfile:
            gzfile.write(buffer.getvalue().encode("utf-8"))


def save_as_json(data: list[dict], path: Path) -> None:
    root = path / "json"
    shutil.rmtree(root, ignore_errors=True)

    save_as_json_plain(data, root / "without_compression")
    save_as_json_gz(data, root / "with_compression")


def save_as_jsonline_plain(data: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "file.jsonl", "w") as file:
        for row in data:
            row_str = json.dumps(row, default=_to_string)
            file.write(row_str + os.linesep)


def save_as_jsonline_gz(data: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

    buffer = io.StringIO()
    for row in data:
        row_str = json.dumps(row, default=_to_string)
        buffer.write(row_str + os.linesep)

    with open(path / "file.jsonl.gz", "wb") as file:
        with gzip.GzipFile(fileobj=file, mode="w", mtime=0) as gzfile:
            gzfile.write(buffer.getvalue().encode("utf-8"))


def save_as_jsonline(data: list[dict], path: Path) -> None:
    root = path / "jsonline"
    shutil.rmtree(root, ignore_errors=True)

    save_as_jsonline_plain(data, root / "without_compression")
    save_as_jsonline_gz(data, root / "with_compression")


format_mapping = {
    "csv": save_as_csv,
    "json": save_as_json,
    "jsonline": save_as_jsonline,
}


def get_parser() -> ArgumentParser:
    known_formats = ["all", *format_mapping.keys()]
    parser = ArgumentParser()
    parser.add_argument("--path", type=Path, default=Path(__file__).parent)
    parser.add_argument("--format", type=str, default="all", choices=known_formats)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = get_parser()
    args = parser.parse_args(argv or sys.argv[1:])

    if args.format not in format_mapping and args.format != "all":
        raise ValueError(f"Format {args.format} is not supported")

    data = get_data()
    if args.format == "all":
        save_functions = list(format_mapping.values())
    else:
        save_functions = [format_mapping[args.format]]

    for save_func in save_functions:
        save_func(data, args.path)


if __name__ == "__main__":
    main()
