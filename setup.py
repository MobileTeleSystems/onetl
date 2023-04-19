from __future__ import annotations

import os
from pathlib import Path

from setuptools import find_packages, setup

here = Path(__file__).parent.resolve()


def get_version():
    if "CI_COMMIT_TAG" in os.environ:
        return os.environ["CI_COMMIT_TAG"]

    version_file = here / "onetl" / "VERSION"
    version = version_file.read_text().strip()  # noqa: WPS410

    build_num = os.environ.get("CI_PIPELINE_IID", "")
    branch_name = os.environ.get("CI_COMMIT_REF_SLUG", "")
    branches_protect = ["master", "develop"]

    if not branch_name or branch_name in branches_protect:
        return f"{version}.dev{build_num}"

    return f"{version}.dev{build_num}+{branch_name}"


def parse_requirements(file: Path) -> list[str]:
    lines = file.read_text().splitlines()
    return [line.rstrip() for line in lines if line and not line.startswith("#")]


requirements = parse_requirements(here / "requirements" / "requirements.txt")

requirements_ftp = parse_requirements(here / "requirements" / "requirements-ftp.txt")
requirements_sftp = parse_requirements(here / "requirements" / "requirements-sftp.txt")
requirements_hdfs = parse_requirements(here / "requirements" / "requirements-hdfs.txt")
requirements_s3 = parse_requirements(here / "requirements" / "requirements-s3.txt")
requirements_webdav = parse_requirements(here / "requirements" / "requirements-webdav.txt")
requirements_files = [*requirements_ftp, *requirements_sftp, *requirements_hdfs, *requirements_s3, *requirements_webdav]

requirements_kerberos = parse_requirements(here / "requirements" / "requirements-kerberos.txt")
requirements_spark = parse_requirements(here / "requirements" / "requirements-spark.txt")
requirements_all = [*requirements_files, *requirements_kerberos, *requirements_spark]

long_description = (here / "README.rst").read_text()

setup(
    name="onetl",
    version=get_version(),
    author="ONEtools Team",
    author_email="onetools@mts.ru",
    description="etl-tool for extract and load operations",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    license="Apache License 2.0",
    license_files=("LICENSE.txt",),
    url="https://github.com/MobileTeleSystems/onetl",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Pydantic",
        "Framework :: Pydantic :: 1",
        "Intended Audience :: Data engineers",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Spark Tools",
        "Topic :: System :: Distributed Computing",
        "Typing :: Typed",
    ],
    project_urls={
        "Documentation": "https://onetl.readthedocs.io/en/stable/",
        "Source": "https://github.com/MobileTeleSystems/onetl",
        "CI/CD": "https://github.com/MobileTeleSystems/onetl/actions",
        "Tracker": "https://github.com/MobileTeleSystems/onetl/issues",
    },
    keywords=["Spark", "ETL", "JDBC", "HWM"],
    packages=find_packages(exclude=["docs", "docs.*", "tests", "tests.*"]),
    python_requires=">=3.7",
    install_requires=requirements,
    extras_require={
        "spark": requirements_spark,
        "ftp": requirements_ftp,
        "ftps": requirements_ftp,
        "sftp": requirements_sftp,
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
