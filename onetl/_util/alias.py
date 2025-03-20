# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
def avoid_alias(name: str) -> str:
    # https://github.com/pydantic/pydantic/issues/5893
    return name
