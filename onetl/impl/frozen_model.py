# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from onetl.impl.base_model import BaseModel


class FrozenModel(BaseModel):
    class Config:
        smart_union = True
        frozen = True
