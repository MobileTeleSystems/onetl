# Contributing Guide { #DBR-onetl-contributing-guide }

Welcome! There are many ways to contribute, including submitting bug
reports, improving documentation, submitting feature requests, reviewing
new submissions, or contributing code that can be incorporated into the
project.

## Review process { #DBR-onetl-contributing-review-process }

For any **significant** changes please create a new GitHub issue and
enhancements that you wish to make. Describe the feature you would like
to see, why you need it, and how it will work. Discuss your ideas
transparently and get community feedback before proceeding.

Small changes can directly be crafted and submitted to the GitHub
Repository as a Pull Request. This requires creating a **repo fork** using
[instruction](https://docs.github.com/en/get-started/quickstart/fork-a-repo).

## Important notes { #DBR-onetl-contributing-important-notes }

Please take into account that:

-   Some companies still use old Spark versions, like 3.2.0. So it is required to keep compatibility if possible, e.g. adding branches for different Spark versions.
-   Different users uses onETL in different ways - some uses only DB connectors, some only files. Connector-specific dependencies should be optional.
-   Instead of creating classes with a lot of different options, prefer splitting them into smaller classes, e.g. options class, context manager, etc, and using composition.

## Initial setup for local development { #DBR-onetl-contributing-initial-setup-for-local-development }

### Install Git { #DBR-onetl-contributing-install-git }

Please follow [instruction](https://docs.github.com/en/get-started/quickstart/set-up-git).

### Clone the repo { #DBR-onetl-contributing-clone-the-repo }

Open terminal and run these commands to clone a **forked** repo:

```bash
git clone git@github.com:myuser/onetl.git -b develop

cd onetl
```

### Enable pre-commit hooks { #DBR-onetl-contributing-enable-pre-commit-hooks }

Create virtualenv and install dependencies:

```bash
make venv-install
```

Install pre-commit hooks:

```bash
prek install --install-hooks
```

Test pre-commit hooks run:

```bash
prek run
```

## How to { #DBR-onetl-contributing-how-to }

### Run tests locally { #DBR-onetl-contributing-run-tests-locally }

!!! note

    You can skip this if only documentation is changed.

#### Setup environment { #DBR-onetl-contributing-setup-environment }

Create virtualenv and install dependencies:

```bash
make venv-install
```

#### Using docker-compose { #DBR-onetl-contributing-using-docker-compose }

Build image for running tests:

```bash
docker-compose build
```

Start all containers with dependencies:

```bash
docker-compose --profile all up -d
```

You can run limited set of dependencies:

```bash
docker-compose --profile mongodb up -d
```

Run tests:

```bash
docker-compose run --rm onetl pytest
```

You can pass additional arguments, they will be passed to pytest:

```bash
docker-compose run --rm onetl pytest -m mongodb -lsx -vvvv --log-cli-level=INFO
```

You can run interactive bash session and use it:

```bash
docker-compose run --rm onetl bash

pytest -m mongodb -lsx -vvvv --log-cli-level=INFO
```

See logs of test container:

```bash
docker-compose logs -f onetl
```

Stop all containers and remove created volumes:

```bash
docker-compose --profile all down -v
```

#### Without docker-compose { #DBR-onetl-contributing-without-docker-compose }

!!! warning

    To run HDFS tests locally you should add the following line to your `/etc/hosts` (file path depends on OS):

    ```default
    # HDFS server returns container hostname as connection address, causing error in DNS resolution
    127.0.0.1 hdfs
    ```

!!! note

    To run Oracle tests you need to install [Oracle instantclient](https://www.oracle.com/database/technologies/instant-client.html),
    and pass its path to `ONETL_ORA_CLIENT_PATH` and `LD_LIBRARY_PATH` environment variables,
    e.g. `ONETL_ORA_CLIENT_PATH=/path/to/client64/lib`.

    It may also require to add the same path into `LD_LIBRARY_PATH` environment variable

!!! note

    To run Greenplum tests, you should:

    * Download [VMware Greenplum connector for Spark][DBR-onetl-connection-db-connection-greenplum-prerequisites]
    * Either move it to `~/.ivy2/jars/`, or pass file path to `CLASSPATH`
    * Set environment variable `ONETL_GP_PACKAGE_VERSION=local`.

Start all containers with dependencies:

```bash
docker-compose --profile all up -d
```

You can run limited set of dependencies:

```bash
docker-compose --profile mongodb up -d
```

Run core tests:

```bash
make test-core
```

Run specific connection tests:

```bash
make test-spark PYTEST_ARGS="-m mongodb"
make test-no-spark PYTEST_ARGS="-m ftp"
```

You can pass additional arguments, they will be passed to pytest:

```bash
make test-spark PYTEST_ARGS="-m mongodb -lsx -vvvv --log-cli-level=INFO"
```

Stop all containers and remove created volumes:

```bash
docker-compose --profile all down -v
```

### Build documentation { #DBR-onetl-contributing-build-documentation }

!!! note

    You can skip this if only source code behavior remains the same.

Create virtualenv and install dependencies:

```bash
make venv-install
```

Build documentation using mkdocs:

```bash
make docs-serve
```

Then open in browser http://localhost:8000/

### Create pull request { #DBR-onetl-contributing-create-pull-request }

Commit your changes:

```bash
git commit -m "Commit message"
git push
```

Then open Github interface and [create pull request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-a-pull-request).
Please follow guide from PR body template.

After pull request is created, it get a corresponding number, e.g. 123 (`pr_number`).

### Write release notes { #DBR-onetl-contributing-write-release-notes }

`onETL` uses [towncrier](https://pypi.org/project/towncrier/)
for changelog management.

To submit a change note about your PR, add a text file into the
[docs/changelog/RELEASE_TEMPLATE](./changelog/RELEASE_TEMPLATE.md) folder. It should contain an
explanation of what applying this PR will change in the way
end-users interact with the project. One sentence is usually
enough but feel free to add as many details as you feel necessary
for the users to understand what it means.

**Use the past tense** for the text in your fragment because,
combined with others, it will be a part of the "news digest"
telling the readers **what changed** in a specific version of
the library *since the previous version*.

Finally, name your file following the convention that Towncrier
understands: it should start with the number of an issue or a
PR followed by a dot, then add a patch type, like `feature`,
`doc`, `misc` etc., and add `.md` as a suffix. If you
need to add more than one fragment, you may add an optional
sequence number (delimited with another period) between the type
and the suffix.

In general the name will follow `<pr_number>.<category>.md` pattern,
where the categories are:

* `feature`: Any new feature
* `bugfix`: A bug fix
* `improvement`: An improvement
* `doc`: A change to the documentation
* `dependency`: Dependency-related changes
* `misc`: Changes internal to the repo like CI, test and build changes

A pull request may have more than one of these components, for example
a code change may introduce a new feature that deprecates an old
feature, in which case two fragments should be added. It is not
necessary to make a separate documentation fragment for documentation
changes accompanying the relevant code changes.

#### Examples for adding changelog entries to your Pull Requests { #DBR-onetl-contributing-examples-for-adding-changelog-entries-to-your-pull-requests }

```markdown title="mddocs/docs/changelog/RELEASE_TEMPLATE/2345.bugfix.md"
Fixed behavior of `WebDAV` connector
```

```markdown title="mddocs/docs/changelog/RELEASE_TEMPLATE/3456.feature.md"
Added support of `timeout` in ``S3`` connector
```

#### How to skip change notes check? { #DBR-onetl-contributing-how-to-skip-change-notes-check }

Just add `ci:skip-changelog` label to pull request.

!!! tip

    See [pyproject.toml](https://github.com/MTSWebServices/onetl/blob/develop/pyproject.toml) for all available categories (`tool.towncrier.type`).

#### Release Process { #DBR-onetl-contributing-release-process }

!!! note

    This is for repo maintainers only

Before making a release from the `develop` branch, follow these steps:

1. Checkout to `develop` branch and update it to the actual state

    ```bash
    git checkout develop
    git pull -p
    ```

2. Get current release version

    ```bash
    VERSION=$(cat onetl/VERSION)
    ```

3. Build changelog for current release

    ```bash
    make docs-generate-changelog
    ```

4. Commit and push changes to `develop` branch

    ```bash
    git add .
    git commit -m "Prepare for release ${VERSION}"
    git push
    ```

5. Merge `develop` branch to `master`, **WITHOUT** squashing

    ```bash
    git checkout master
    git pull
    git merge develop
    git push
    ```

6. Add git tag to the latest commit in `master` branch

    ```bash
    git tag "$VERSION"
    git push origin "$VERSION"
    ```

7. Update version in `develop` branch **after release**:

    ```bash
    git checkout develop

    NEXT_VERSION=$(echo "$VERSION" | awk -F. '/[0-9]+\./{$NF++;print}' OFS=.)
    echo "$NEXT_VERSION" > onetl/VERSION

    git add .
    git commit -m "Bump version"
    git push
    ```
