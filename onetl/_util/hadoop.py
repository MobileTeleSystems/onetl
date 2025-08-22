# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_hadoop_config(spark_session: SparkSession):
    """
    Get ``org.apache.hadoop.conf.Configuration`` object
    """
    return spark_session.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
