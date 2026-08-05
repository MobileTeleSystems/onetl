# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import os
import socket

import psutil


def get_process_info() -> tuple[str, str]:
    process = psutil.Process(os.getpid())
    return process.name(), socket.getfqdn()
