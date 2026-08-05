# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import re

try:
    from pydantic.v1 import StrictStr
except (ImportError, AttributeError):
    from pydantic import StrictStr


class Cluster(StrictStr):
    """Cluster representation

    Cluster name should be in format ``somecluster``, ``some-cluster``, ``some-cluster``.

    .. warning::

        Name can have only alphanumeric symbols and ``-``, ``_``.

        Name cannot be just a numeric value, prefer ``somecluster-001``.
    """

    regex = re.compile("^[a-zA-Z]+([-_]*[a-zA-Z0-9])*$")
