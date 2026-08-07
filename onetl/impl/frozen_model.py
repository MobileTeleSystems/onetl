# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import ConfigDict

from onetl.impl.base_model import BaseModel


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True)
