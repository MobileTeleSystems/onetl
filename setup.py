from os.path import abspath, dirname, join

from setuptools import setup, find_packages
from version import get_version

__version__ = get_version()
SELF_PATH = abspath(dirname(__file__))

with open(join(SELF_PATH, 'requirements.txt')) as f:
    requirements = [line.rstrip() for line in f if line and not line.startswith('#')]

with open(join(SELF_PATH, 'README.rst')) as f:
    long_description = f.read()

setup(
    name='onetl',
    version=__version__,
    description='etl-tool for extract and load operations',
    long_description=long_description,
    url='https://gitlab.services.mts.ru/bigdata/platform/onetools/onetl',
    packages=find_packages(),
    author='ONEtools Team',
    author_email='mivasil6@mts.ru',
    python_requires='>=3.7',
    install_requires=requirements,
    # dependency_links=[
    #     'http://rep.msk.mts.ru/artifactory/api/pypi/pypi-dev-local/simple',
    #     'http://rep.msk.mts.ru/artifactory/api/pypi/pypi/simple',
    # ],
)
