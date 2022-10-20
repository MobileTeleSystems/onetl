from os.path import abspath, dirname, join

from setuptools import find_packages, setup

from version import get_version

__version__ = get_version()
SELF_PATH = abspath(dirname(__file__))

with open(join(SELF_PATH, "requirements.txt")) as f:
    requirements = [line.rstrip() for line in f if line and not line.startswith("#")]

with open(join(SELF_PATH, "README.rst")) as f:
    long_description = f.read()

setup(
    name="onetl",
    version=__version__,
    author="ONEtools Team",
    author_email="onetools@mts.ru",
    description="etl-tool for extract and load operations",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    license="Apache License 2.0",
    license_files=("LICENSE.txt",),
    url="https://gitlab.services.mts.ru/bigdata/platform/onetools/onetl",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Data engineers",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Spark Tools",
        "Topic :: System :: Distributed Computing",
        "Typing :: Typed",
    ],
    project_urls={
        "Documentation": "https://bigdata.pages.mts.ru/platform/onetools/onetl/",
        "Source": "https://gitlab.services.mts.ru/bigdata/platform/onetools/onetl",
        "CI/CD": "https://gitlab.services.mts.ru/bigdata/platform/onetools/onetl/-/pipelines",
        "Tracker": "https://jira.bd.msk.mts.ru/projects/ONE/issues",
    },
    keywords=["Spark", "ETL", "JDBC", "HWM"],
    packages=find_packages(exclude=["docs", "docs.*", "tests", "tests.*"]),
    python_requires=">=3.7",
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False,
)
