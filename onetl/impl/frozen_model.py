# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import ConfigDict

from onetl.impl.base_model import BaseModel


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, arbitrary_types_allowed=True, extra="forbid")
