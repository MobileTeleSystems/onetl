from onetl.connection import Hive
from onetl.core import DBWriter


def test_writer_target_alias(spark_mock):
    writer1 = DBWriter(
        connection=Hive(cluster="rnd-dwh", spark=spark_mock),
        target="schema.table",
    )
    writer2 = DBWriter(
        connection=Hive(cluster="rnd-dwh", spark=spark_mock),
        table="schema.table",
    )

    assert writer1.target == writer2.target
