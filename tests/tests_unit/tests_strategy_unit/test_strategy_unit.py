import secrets
from datetime import timedelta
from unittest.mock import patch

import pytest

from onetl.connection import Postgres
from onetl.db import DBReader
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
        timedelta(),
    ],
)
@pytest.mark.parametrize("strategy", [IncrementalBatchStrategy, SnapshotBatchStrategy])
def test_strategy_batch_step_is_empty(step, strategy):
    with pytest.raises(ValueError):
        strategy()

    with pytest.raises(ValueError, match=f"'step' argument of {strategy.__name__} cannot be empty!"):
        strategy(step=step)


@patch.object(Postgres, "check")
@pytest.mark.parametrize(
    "strategy, kwargs",
    [
        (IncrementalStrategy, {}),
        (IncrementalBatchStrategy, {"step": 1}),
        (SnapshotBatchStrategy, {"step": 1}),
    ],
)
def test_strategy_hwm_column_not_set(check, strategy, kwargs, spark_mock):
    check.return_value = None

    with strategy(**kwargs):
        reader = DBReader(
            connection=Postgres(
                spark=spark_mock,
                host="some_host",
                user="valid_user",
                database="default",
                password="pwd",
            ),
            table=f"{secrets.token_hex()}.{secrets.token_hex()}",
        )

        with pytest.raises(ValueError):
            reader.run()
