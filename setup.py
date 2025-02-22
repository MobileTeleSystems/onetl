from __future__ import annotations

import os
from pathlib import Path

from setuptools import find_packages, setup

here = Path(__file__).parent.resolve()


def get_version():
    if os.getenv("GITHUB_REF_TYPE", "branch") == "tag":
        return os.environ["GITHUB_REF_NAME"]

    version_file = here / "onetl" / "VERSION"
    version = version_file.read_text().strip()

    build_num = os.getenv("GITHUB_RUN_ID", "0")
    branch_name = os.getenv("GITHUB_REF_NAME", "")

    if not branch_name:
        return version

    return f"{version}.dev{build_num}"


def parse_requirements(file: Path) -> list[str]:
    lines = file.read_text().splitlines()
    return [line.rstrip() for line in lines if line and not line.startswith("#")]


requirements_core = parse_requirements(here / "requirements" / "core.txt")

requirements_ftp = parse_requirements(here / "requirements" / "ftp.txt")
requirements_sftp = parse_requirements(here / "requirements" / "sftp.txt")
requirements_samba = parse_requirements(here / "requirements" / "samba.txt")
requirements_hdfs = parse_requirements(here / "requirements" / "hdfs.txt")
requirements_s3 = parse_requirements(here / "requirements" / "s3.txt")
requirements_webdav = parse_requirements(here / "requirements" / "webdav.txt")
requirements_files = [
    *requirements_ftp,
    *requirements_sftp,
    *requirements_hdfs,
    *requirements_s3,
    *requirements_webdav,
    *requirements_samba,
]

requirements_kerberos = parse_requirements(here / "requirements" / "kerberos.txt")
requirements_spark = parse_requirements(here / "requirements" / "spark.txt")
requirements_all = [*requirements_files, *requirements_kerberos, *requirements_spark]

long_description = (here / "README.rst").read_text()

setup(
    name="onetl",
    version=get_version(),
    author="DataOps.ETL",
    author_email="onetools@mts.ru",
    description="One ETL tool to rule them all",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    license="Apache-2.0",
    license_files=("LICENSE.txt",),
    url="https://github.com/MobileTeleSystems/onetl",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Framework :: Pydantic",
        "Framework :: Pydantic :: 1",
        "Framework :: Pydantic :: 2",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Java Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Typing :: Typed",
    ],
    project_urls={
        "Documentation": "https://onetl.readthedocs.io/",
        "Source": "https://github.com/MobileTeleSystems/onetl",
        "CI/CD": "https://github.com/MobileTeleSystems/onetl/actions",
        "Tracker": "https://github.com/MobileTeleSystems/onetl/issues",
    },
    keywords=["Spark", "ETL", "JDBC", "HWM"],
    packages=find_packages(exclude=["docs", "docs.*", "tests", "tests.*"]),
    entry_points={"tricoder_package_spy.register": ["onetl=onetl"]},
    python_requires=">=3.7",
    install_requires=requirements_core,
    extras_require={
        "spark": requirements_spark,
        "ftp": requirements_ftp,
        "ftps": requirements_ftp,
        "sftp": requirements_sftp,
        "samba": requirements_samba,
        "hdfs": requirements_hdfs,
        "s3": requirements_s3,
        "webdav": requirements_webdav,
        "files": requirements_files,
        "kerberos": requirements_kerberos,
        "all": requirements_all,
    },
    include_package_data=True,
    zip_safe=False,
)
