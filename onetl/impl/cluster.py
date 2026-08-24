# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Annotated

from pydantic import StringConstraints

Cluster = Annotated[
    str,
    StringConstraints(pattern="^[a-zA-Z]+([-_]*[a-zA-Z0-9])*$"),
]
"""
Cluster name should be in format ``somecluster``, ``some-cluster``, ``some-cluster``.

.. warning::

    Name can have only alphanumeric symbols and ``-``, ``_``.

    Name cannot be just a numeric value, prefer ``somecluster-001``.
"""
