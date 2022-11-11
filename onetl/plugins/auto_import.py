#  Copyright 2022 MTS (Mobile Telesystems)
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

import logging
import inspect

from importlib_metadata import entry_points

from onetl.log import log_with_indent

log = logging.getLogger(__name__)


def plugins_auto_import(group: str):
    log.debug("|onETL| Searching for plugins with group {group!r}")

    ep = entry_points(group=group)

    for to_load in ep:
        log.info("|onETL| Loading plugin:")
        log_with_indent(f"name: {to_load.name!r}")
        log_with_indent(f"version: {to_load.dist.version!r}")
        log_with_indent(f"module: {to_load.value!r}")

        loaded = to_load.load()
        log.info("|onETL| Successfully loaded plugin")
        log_with_indent(f"source: {inspect.getfile(loaded)!r}", level=logging.DEBUG)
