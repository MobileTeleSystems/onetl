#!make

include .env.local

SPARK_EXTERNAL_IP := $(shell docker network inspect onetl_onetl --format '{{ (index .IPAM.Config 0).Gateway }}')
VERSION := $(shell cat onetl/VERSION)
DATE := $(shell date --rfc-3339=date)
SPARK_VERSION ?= 3.5
ETL_ENTITIES_VERSION ?= 3
VIRTUAL_ENV ?= .venv
PYTHON = ${VIRTUAL_ENV}/bin/python
PIP = ${VIRTUAL_ENV}/bin/pip
UV ?= ${VIRTUAL_ENV}/bin/uv
PYTEST ?= pytest

# Fix docker build and docker compose build using different backends
COMPOSE_DOCKER_CLI_BUILD = 1
DOCKER_BUILDKIT = 1
# Fix docker build on M1/M2
DOCKER_DEFAULT_PLATFORM = linux/amd64

HELP_FUN = \
	%help; while(<>){push@{$$help{$$2//'options'}},[$$1,$$3] \
	if/^([\w-_]+)\s*:.*\#\#(?:@(\w+))?\s(.*)$$/}; \
    print"$$_:\n", map"  $$_->[0]".(" "x(20-length($$_->[0])))."$$_->[1]\n",\
    @{$$help{$$_}},"\n" for keys %help; \

all: help

help: ##@Help Show this help
	@echo -e "Usage: make [target] ...\n"
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)



venv: venv-cleanup venv-install##@Env Init venv and install uv dependencies

venv-cleanup: ##@Env Cleanup venv
	@rm -rf ${VIRTUAL_ENV} || true
	python3 -m venv ${VIRTUAL_ENV}
	${PIP} install uv

venv-install: ##@Env Install requirements to venv
	${UV} sync \
		--inexact \
		--all-extras \
		--group dev \
		--group docs \
		--group test \
		--group test-clickhouse \
		--group test-mongodb \
		--group test-mssql \
		--group test-mysql \
		--group test-oracle \
		--group test-postgres \
		--group test-spark-${SPARK_VERSION} \
		--group test-etl-entities-${ETL_ENTITIES_VERSION} \
		$(UV_ARGS)

	${UV} pip install --no-deps sphinx-plantuml


test-spark: ##@Run tests with Spark
	uv run \
		$(UV_ARGS) \
		--group test \
		--group test-spark-${SPARK_VERSION} \
		--group test-etl-entities-${ETL_ENTITIES_VERSION} \
			${PYTEST} \
			$(PYTEST_ARGS)


test-no-spark: ##@Run tests without Spark installed
	uv run \
		$(UV_ARGS) \
		--group test \
		--group test-etl-entities-${ETL_ENTITIES_VERSION} \
			${PYTEST} \
				$(PYTEST_ARGS)


test-core: ##@Run core tests
	uv run \
		$(UV_ARGS) \
		--group test \
		--group test-spark-${SPARK_VERSION} \
		--group test-etl-entities-${ETL_ENTITIES_VERSION} \
		--with-editable tests/libs/dummy \
		--with-editable tests/libs/failing \
			${PYTEST} \
				-m "not connection" \
				$(PYTEST_ARGS)


test-doctest: ##@Run documentation tests
	uv run \
		$(UV_ARGS) \
		--group test \
		--group test-spark-${SPARK_VERSION} \
		--group test-etl-entities-${ETL_ENTITIES_VERSION} \
			${PYTEST} \
				--doctest-modules onetl/_util onetl/hooks onetl/file/filter onetl/file/limit onetl/hwm/store/hwm_class_registry.py \
				$(PYTEST_ARGS)


.PHONY: docs

docs: docs-build docs-open ##@Docs Generate & open docs

docs-build: ##@Docs Generate docs
	DISABLE_MKDOCS_2_WARNING=true mkdocs build -f mddocs/mkdocs.yml

docs-open: ##@Docs Open docs
	xdg-open mddocs/generated/index.html

docs-cleanup: ##@Docs Cleanup docs
	rm -rf mddocs/generated/

docs-fresh: docs-cleanup docs-build ##@Docs Cleanup & build docs

docs-serve: ##@Docs Run docs server
	DISABLE_MKDOCS_2_WARNING=true mkdocs serve -f mddocs/mkdocs.yml

docs-generate-changelog: ##@Docs Generate changelog
	echo "Building changelog for ${VERSION}"
	cp "mddocs/docs/changelog/RELEASE_TEMPLATE.md" "mddocs/docs/changelog/temp_RELEASE_TEMPLATE.md"
	towncrier build "--version=${VERSION}" --yes
	mv "mddocs/docs/changelog/RELEASE_TEMPLATE.md" "mddocs/docs/changelog/${VERSION}.md"
	mv "mddocs/docs/changelog/temp_RELEASE_TEMPLATE.md" "mddocs/docs/changelog/RELEASE_TEMPLATE.md"

	# Remove content above the version number heading in the `${VERSION}.md` file
	awk '/##/,0' "mddocs/docs/changelog/${VERSION}.md" > temp && mv temp "mddocs/docs/changelog/${VERSION}.md"

	# Update Changelog Index and Navigation
	sed "s#\(.*NEXT_RELEASE.*\)#\1\n- [${VERSION} (${DATE})][DBR-onetl-changelog-${VERSION_ANCHOR}]#" "mddocs/docs/changelog/index.md" > temp && mv temp "mddocs/docs/changelog/index.md"
	sed "s#\(.*NEXT_RELEASE.*\)#\1\n    * [${VERSION}](changelog/${VERSION}.md)#" "mddocs/docs/nav.md" > temp && mv temp "mddocs/docs/nav.md"
