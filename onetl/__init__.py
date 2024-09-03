# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os

from onetl.plugins import import_plugins
from onetl.version import __version__


def plugins_auto_import():
    """
    Automatically import all onETL plugins.

    Executed while onETL is being imported.

    See :ref:`plugins` documentation.
    """
    plugins_enabled = os.getenv("ONETL_PLUGINS_ENABLED", "true").lower() != "false"
    if not plugins_enabled:
        return

    plugins_whitelist = list(filter(None, os.getenv("ONETL_PLUGINS_WHITELIST", "").split(",")))
    plugins_blacklist = list(filter(None, os.getenv("ONETL_PLUGINS_BLACKLIST", "").split(",")))

    import_plugins("onetl.plugins", whitelist=plugins_whitelist, blacklist=plugins_blacklist)


plugins_auto_import()
