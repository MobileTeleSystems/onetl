#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql.conf import RuntimeConfig


@contextmanager
def inject_spark_param(conf: RuntimeConfig, name: str, value: Any):
    """Inject a parameter into a Spark session, and return previous value after exit.

    If value is ``None``, parameter will be reset to default.
    """
    original_value = conf.get(name, None)

    try:
        conf.unset(name)
        if value is not None:
            conf.set(name, value)

        yield
    finally:
        conf.unset(name)
        if original_value is not None:
            conf.set(name, original_value)
