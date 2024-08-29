# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import inspect
import logging
import textwrap

from importlib_metadata import EntryPoint, entry_points

from onetl.log import log_collection, log_with_indent
from onetl.version import __version__

log = logging.getLogger(__name__)


def _prepare_error_msg(plugin_name: str, package_name: str, package_version: str, package_value: str):
    failed_import = package_value.split(":")
    if len(failed_import) > 1:
        import_statement = f"from {failed_import[0]} import {failed_import[1]}"
    else:
        import_statement = f"import {failed_import[0]}"

    return textwrap.dedent(
        f"""
        Error while importing plugin {plugin_name!r} from package {package_name!r} v{package_version}.

        Statement:
            {import_statement}

        Check if plugin is compatible with current onETL version {__version__}.

        You can disable loading this plugin by setting environment variable:
            ONETL_PLUGINS_BLACKLIST='{plugin_name},failing-plugin'

        You can also define a whitelist of packages which can be loaded by onETL:
            ONETL_PLUGINS_WHITELIST='not-failing-plugin1,not-failing-plugin2'

        Please take into account that plugin name may differ from package or module name.
        See package metadata for more details
        """,
    ).strip()


def import_plugin(entrypoint: EntryPoint):
    """
    Import a specific entrypoint.

    If any exception is raised during import, it will be wrapped with ImportError
    containing all diagnostic information about entrypoint.
    """
    try:
        loaded = entrypoint.load()
        log.debug("|onETL| Successfully loaded plugin %r", entrypoint.name)
        log_with_indent(log, "source = %r", inspect.getfile(loaded), level=logging.DEBUG)
    except Exception as e:
        error_msg = _prepare_error_msg(
            plugin_name=entrypoint.name,
            package_name=entrypoint.dist.name if entrypoint.dist else "unknown",
            package_version=entrypoint.dist.version if entrypoint.dist else "unknown",
            package_value=entrypoint.value,
        )
        raise ImportError(error_msg) from e


def import_plugins(group: str, whitelist: list[str] | None = None, blacklist: list[str] | None = None):  # noqa: WPS213
    """
    Import all plugins registered for onETL
    """
    log.debug("|onETL| Searching for plugins with group %r", group)

    entrypoints = entry_points(group=group)
    plugins_count = len(entrypoints)

    if not plugins_count:
        log.debug("|Plugins| No plugins registered")
        return

    log.debug("|Plugins| Found %d plugins", plugins_count)
    log.debug("|Plugins| Plugin load options:")
    log_collection(log, "whitelist", whitelist or [], level=logging.DEBUG)
    log_collection(log, "blacklist", blacklist or [], level=logging.DEBUG)

    for i, entrypoint in enumerate(entrypoints):
        if whitelist and entrypoint.name not in whitelist:
            log.info("|onETL| Skipping plugin %r because it is not in whitelist", entrypoint.dist.name)
            continue

        if blacklist and entrypoint.name in blacklist:
            log.info("|onETL| Skipping plugin %r because it is in a blacklist", entrypoint.dist.name)
            continue

        if log.isEnabledFor(logging.DEBUG):
            log.debug("|onETL| Loading plugin (%d of %d):", i + 1, plugins_count)
            log_with_indent(log, "name = %r", entrypoint.name, level=logging.DEBUG)
            log_with_indent(log, "package = %r", entrypoint.dist.name, level=logging.DEBUG)
            log_with_indent(log, "version = %r", entrypoint.dist.version, level=logging.DEBUG)
            log_with_indent(log, "importing = %r", entrypoint.value, level=logging.DEBUG)
        else:
            log.info("|onETL| Loading plugin %r", entrypoint.name)

        import_plugin(entrypoint)
