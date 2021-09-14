# -*- coding: utf-8 -*-
import os

version = '0.1.{build_num}'  # Bump minor version here


def get_version():
    build_num = 0
    build_info = os.path.join(os.path.dirname(__file__), '..', 'build.info')
    if os.path.exists(build_info):
        with open(build_info) as fp:
            build_num = fp.read().strip()

    build_num = os.environ.get('BUILD_NUMBER', build_num)
    res = version.format(build_num=build_num)

    branch_name = os.environ.get('gitlabBranch', os.environ.get('BRANCH_NAME', ''))
    if branch_name != 'master':
        res += '.dev0'

    return res
