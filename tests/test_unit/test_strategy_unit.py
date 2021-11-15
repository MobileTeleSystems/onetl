# noinspection PyPackageRequirements

import secrets
import pytest
from unittest.mock import Mock

from onetl.connection import Postgres
from onetl.reader import DBReader
from onetl.strategy import IncrementalStrategy, IncrementalBatchStrategy, SnapshotBatchStrategy


@pytest.mark.parametrize(
    "step",
    [
        0,
        None,
    ],
)
@pytest.mark.parametrize("strategy", [IncrementalBatchStrategy, SnapshotBatchStrategy])
def test_strategy_batch_step_is_empty(step, strategy):
    with pytest.raises(ValueError):
        strategy(step=step)


@pytest.mark.parametrize("strategy", [IncrementalStrategy, IncrementalBatchStrategy, SnapshotBatchStrategy])
def test_strategy_hwm_column_missing(strategy):
    with strategy():
        reader = DBReader(
            connection=Postgres(spark=Mock()),
            table=secrets.token_hex(),
        )

        with pytest.raises(ValueError):
            reader.run()
