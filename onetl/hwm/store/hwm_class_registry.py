# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from etl_entities.hwm import HWM, ColumnDateHWM, ColumnDateTimeHWM, ColumnIntHWM

if TYPE_CHECKING:
    from pyspark.sql.types import DataType


class SparkTypeToHWM:
    """Registry class for HWM types

    Examples
    --------

    >>> from etl_entities.hwm import ColumnIntHWM, ColumnDateHWM
    >>> from pyspark.sql.types import IntegerType, ShortType, DateType, StringType
    >>> from onetl.hwm.store import SparkTypeToHWM
    >>> SparkTypeToHWM.get(IntegerType())
    <class 'etl_entities.hwm.column.int_hwm.ColumnIntHWM'>
    >>> # multiple type names are supported
    >>> SparkTypeToHWM.get(ShortType())
    <class 'etl_entities.hwm.column.int_hwm.ColumnIntHWM'>
    >>> SparkTypeToHWM.get(DateType())
    <class 'etl_entities.hwm.column.date_hwm.ColumnDateHWM'>
    >>> SparkTypeToHWM.get(StringType())
    """

    _mapping: ClassVar[dict[DataType | type[DataType], type[HWM]]] = {}

    @classmethod
    def get(cls, spark_type: DataType) -> type[HWM] | None:
        # avoid importing pyspark in the module
        from pyspark.sql.types import (  # noqa: WPS235
            ByteType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            FractionalType,
            IntegerType,
            LongType,
            NumericType,
            ShortType,
            TimestampType,
        )

        default_mapping: dict[type[DataType], type[HWM]] = {
            ByteType: ColumnIntHWM,
            IntegerType: ColumnIntHWM,
            ShortType: ColumnIntHWM,
            LongType: ColumnIntHWM,
            DateType: ColumnDateHWM,
            TimestampType: ColumnDateTimeHWM,
            # for Oracle which does not differ between int and float/double - everything is Decimal
            FloatType: ColumnIntHWM,
            DoubleType: ColumnIntHWM,
            DecimalType: ColumnIntHWM,
            FractionalType: ColumnIntHWM,
            NumericType: ColumnIntHWM,
        }

        return (
            cls._mapping.get(spark_type)
            or cls._mapping.get(spark_type.__class__)
            or default_mapping.get(spark_type.__class__)
        )

    @classmethod
    def add(cls, spark_type: DataType | type[DataType], klass: type[HWM]) -> None:
        cls._mapping[spark_type] = klass


def register_spark_type_to_hwm_type_mapping(*spark_types: DataType | type[DataType]):
    """Decorator for registering mapping between Spark data type and HWM type.

    Accepts both data type class and instance.

    Examples
    --------

    >>> from etl_entities.hwm import ColumnHWM
    >>> from onetl.hwm.store import SparkTypeToHWM
    >>> from onetl.hwm.store import SparkTypeToHWM, register_spark_type_to_hwm_type_mapping
    >>> from pyspark.sql.types import IntegerType, DecimalType
    >>> @register_spark_type_to_hwm_type_mapping(IntegerType, DecimalType(38, 0))
    ... class MyHWM(ColumnHWM): ...
    >>> SparkTypeToHWM.get(IntegerType())
    <class 'onetl.hwm.store.hwm_class_registry.MyHWM'>
    >>> SparkTypeToHWM.get(DecimalType(38, 0))
    <class 'onetl.hwm.store.hwm_class_registry.MyHWM'>
    >>> SparkTypeToHWM.get(DecimalType(38, 10))
    <class 'etl_entities.hwm.column.int_hwm.ColumnIntHWM'>
    """

    def wrapper(cls: type[HWM]):
        for spark_type in spark_types:
            SparkTypeToHWM.add(spark_type, cls)
        return cls

    return wrapper
