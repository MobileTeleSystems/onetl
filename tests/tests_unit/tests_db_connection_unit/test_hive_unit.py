import pytest

from onetl.connection import Hive


@pytest.mark.parametrize(
    "sort_by",
    ["id_int", ["id_int", "hwm_int"]],
    ids=["sortBy as string.", "sortBy as List."],
)
def test_sort_by_without_bucket_by(sort_by):
    with pytest.raises(ValueError):
        Hive.Options(sortBy=sort_by)


@pytest.mark.parametrize(
    "options",
    [
        # disallowed modes
        {"mode": "error"},
        {"mode": "ignore"},
        # options user only for table creation
        {"compression": True},
        {"partitionBy": "id_int"},
        {"bucketBy": (10, "id_int")},
        {"bucketBy": (10, "id_int"), "sortBy": "id_int"},
    ],
)
def test_insert_into_wrong_options(options):
    with pytest.raises(ValueError):
        Hive.Options(insert_into=True, **options)


def test_hive_empty_connection():
    with pytest.raises(TypeError):
        Hive()  # noqa: F841
