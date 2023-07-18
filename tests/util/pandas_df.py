from __future__ import annotations

import pandas


def lowercase_columns(df: pandas.DataFrame, columns: list[str] | None = None) -> pandas.DataFrame:
    """
    Rename columns to lowercase.

    If `columns` is None, convert all columns.
    """
    if not columns:
        columns = list(df.columns)

    rename_columns = {x: x.lower() for x in columns}
    return df.rename(columns=rename_columns, inplace=False)
