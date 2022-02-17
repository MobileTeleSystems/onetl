import secrets
from unittest.mock import Mock

import pytest

from onetl.connection import Postgres
from onetl.core import DBReader
from onetl.strategy import (
    IncrementalBatchStrategy,
    IncrementalStrategy,
    SnapshotBatchStrategy,
)


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
        strategy()

    with pytest.raises(ValueError):
        strategy(step=step)


@pytest.mark.parametrize(
    "strategy, kwargs",
    [
        (IncrementalStrategy, {}),
        (IncrementalBatchStrategy, {"step": 1}),
        (SnapshotBatchStrategy, {"step": 1}),
    ],
)
def test_strategy_hwm_column_not_set(strategy, kwargs):
    with strategy(**kwargs):
        reader = DBReader(
            connection=Postgres(spark=Mock(), host="some_host", user="valid_user", database="default", password="pwd"),
            table=f"{secrets.token_hex()}.{secrets.token_hex()}",
        )

        with pytest.raises(ValueError):
            reader.run()
