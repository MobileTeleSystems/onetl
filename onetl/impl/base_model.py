# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
# isort: skip_file

from pydantic import ConfigDict

from pydantic import BaseModel as PydanticBaseModel


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True, extra="forbid")
