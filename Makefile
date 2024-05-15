SHELL := bash

export PYTHON?=python3
export PYTHONIOENCODING?=utf8
export NO_VENV?=
export COMPOSE_HTTP_TIMEOUT?=300
export CLICKHOUSE_VERSION?=latest
export PROJECT_NAME ?= ch-backup

ifndef NO_VENV
  PATH:=venv/bin:${PATH}
endif

PYTHON_VERSION=$(shell ${PYTHON} -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
SESSION_FILE=.session_conf.sav
INSTALL_DIR=$(DESTDIR)/opt/yandex/ch-backup
SRC_DIR ?= ch_backup
TESTS_DIR ?= tests

TEST_ENV=env \
    PATH=${PATH} \
    PYTHON_VERSION=${PYTHON_VERSION} \
    PYTHONIOENCODING=${PYTHONIOENCODING} \
    CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION} \
    COMPOSE_HTTP_TIMEOUT=${COMPOSE_HTTP_TIMEOUT}

INTEGRATION_TEST_TOOL=${TEST_ENV} python -m tests.integration.env_control

export BUILD_PYTHON_OUTPUT_DIR ?= dist
export BUILD_DEB_OUTPUT_DIR ?= out

# Different ways of passing signing key for building debian package
export DEB_SIGN_KEY_ID ?=
export DEB_SIGN_KEY ?=
export DEB_SIGN_KEY_PATH ?=

# Platform of image for building debian package according to
# https://docs.docker.com/build/building/multi-platform/#building-multi-platform-images
# E.g. linux/amd64, linux/arm64, etc.
# If platform is not provided Docker uses platform of the host performing the build
export DEB_TARGET_PLATFORM ?=
# Name of image (with tag) for building deb package.
# E.g. ubuntu:22.04, ubuntu:jammy, ubuntu:bionic, etc.
# If it is not provided, default value in Dockerfile is used
export DEB_BUILD_DISTRIBUTION ?=

.PHONY: build
build: install-deps ch_backup/version.txt

.PHONY: all
all: build lint test-unit test-integration

.PHONY: lint
lint: install-deps isort black codespell ruff pylint mypy bandit

.PHONY: isort
isort: install-deps
	${TEST_ENV} isort --check --diff $(SRC_DIR) $(TESTS_DIR)

.PHONY: black
black: install-deps
	${TEST_ENV} black --check --diff $(SRC_DIR) $(TESTS_DIR)

.PHONY: codespell
codespell: install-deps
	${TEST_ENV} codespell

.PHONY: fix-codespell-errors
fix-codespell-errors: install-deps
	${TEST_ENV} codespell -w

.PHONY: ruff
ruff: install-deps
	${TEST_ENV} ruff check $(SRC_DIR) $(TESTS_DIR)

.PHONY: pylint
pylint: install-deps
	${TEST_ENV} pylint $(SRC_DIR)
	${TEST_ENV} pylint --disable=missing-docstring,invalid-name $(TESTS_DIR)

.PHONY: mypy
mypy: install-deps
	${TEST_ENV} mypy $(SRC_DIR) $(TESTS_DIR)

.PHONY: bandit
bandit: install-deps
	${TEST_ENV} bandit -c bandit.yaml -r ch_backup


.PHONY: test-unit
test-unit: build
	${TEST_ENV} py.test $(PYTEST_ARGS) tests


.PHONY: test-integration
test-integration: build create-env
	rm -rf staging/logs
	${TEST_ENV} behave --show-timings --stop -D skip_setup $(BEHAVE_ARGS) @tests/integration/ch_backup.featureset


.PHONY: clean
clean: clean-env clean-pycache clean-debuild
	rm -rf venv *.egg-info htmlcov .coverage* .hypothesis .mypy_cache .pytest_cache .install-deps ch_backup/version.txt

.PHONY: clean-pycache
clean-pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +


.PHONY: install
install: .copy-target-python
	@echo "Installing into $(INSTALL_DIR)"
	$(PYTHON) -m venv $(INSTALL_DIR)
	$(INSTALL_DIR)/bin/pip install -r requirements.txt
	$(INSTALL_DIR)/bin/pip install .
	mkdir -p $(DESTDIR)/usr/bin/
	ln -s /opt/yandex/ch-backup/bin/ch-backup $(DESTDIR)/usr/bin/
	mkdir -p $(DESTDIR)/etc/bash_completion.d/
	env LC_ALL=C.UTF-8 LANG=C.UTF-8 \
	    _CH_BACKUP_COMPLETE=bash_source $(INSTALL_DIR)/bin/ch-backup > $(DESTDIR)/etc/bash_completion.d/ch-backup || \
	    test -s $(DESTDIR)/etc/bash_completion.d/ch-backup
	rm -rf $(INSTALL_DIR)/bin/activate*
	find $(INSTALL_DIR) -name __pycache__ -type d -exec rm -rf {} +
	test -n '$(DESTDIR)' \
	    && grep -l -r -F '#!$(INSTALL_DIR)' $(INSTALL_DIR) \
	        | xargs sed -i -e 's|$(INSTALL_DIR)|/opt/yandex/ch-backup|' \
	    || true


.PHONY: uninstall
uninstall:
	@echo "Uninstalling from $(INSTALL_DIR)"
	rm -rf $(INSTALL_DIR) $(DESTDIR)/usr/bin/ch-backup $(DESTDIR)/etc/bash_completion.d/ch-backup




.PHONY: build-deb-package
build-deb-package:
	./build_deb_in_docker.sh


.PHONY: build-deb-package-local
build-deb-package-local: prepare-changelog
	./build_deb.sh


.PHONY: prepare-changelog
prepare-changelog: build
	@rm -f debian/changelog
	dch --create --package ch-backup --distribution stable \
	    -v `cat ch_backup/version.txt` \
	    "Yandex autobuild"


.PHONY: clean-debuild
clean-debuild:
	rm -rf debian/{changelog,files,ch-backup,.debhelper}
	rm -f ../ch-backup_*{build,changes,deb,dsc,tar.gz}


.PHONY: create-env
create-env: build ${SESSION_FILE}

${SESSION_FILE}:
	${INTEGRATION_TEST_TOOL} create


.PHONY: start-env
start-env: create-env
	${INTEGRATION_TEST_TOOL} start


.PHONY: stop-env
stop-env:
	test -f ${SESSION_FILE} && ${INTEGRATION_TEST_TOOL} stop || true


.PHONY: clean-env
clean-env: stop-env
	rm -rf staging ${SESSION_FILE}


.PHONY: format
format: install-deps
	${TEST_ENV} isort .
	${TEST_ENV} black .


ch_backup/version.txt:
	@echo "0.0.0.10" > ch_backup/version.txt
#	@echo "2.$$(git rev-list HEAD --count).$$(git rev-parse --short HEAD | perl -ne 'print hex $$_')" > ch_backup/version.txt


.PHONY: install-deps
install-deps: check-environment .install-deps

.PHONY: check-environment
check-environment:
	@test="$(command -v ${PYTHON})"; if [ $$? -eq 1 ]; then \
		echo 'Python interpreter "${PYTHON}" ($$PYTHON) not found' >&2; exit 1; \
	fi
	@if [ -z "${PYTHON_VERSION}" ]; then \
		echo 'Failed to determine version of Python interpreter "${PYTHON}" ($$PYTHON)' >&2; exit 1; \
	fi

.install-deps: requirements.txt requirements-dev.txt
	if [ -z "${NO_VENV}" ]; then ${PYTHON} -m venv venv; fi
	${TEST_ENV} pip install --upgrade pip
	${TEST_ENV} pip install --no-cache-dir --disable-pip-version-check -r requirements.txt -r requirements-dev.txt
	touch .install-deps


.PHONY: build-python
build-python:
	PYTHON_INSTALL_PREFIX=${INSTALL_DIR}/python ./build_python_in_docker.sh

.PHONY: .copy-target-python
.copy-target-python:
	@echo "Target python version: ${TARGET_PYTHON_VERSION}"
	@if [[ -n "${TARGET_PYTHON_VERSION}" ]]; then \
		echo "Copying custom python to ${INSTALL_DIR}/python"; \
		mkdir -p ${INSTALL_DIR}/python; \
		cp -R /opt/yandex/ch-backup/python ${INSTALL_DIR}; \
	else \
		echo "Custom target python version is not set."; \
	fi

.PHONY: help
help:
	@echo "Targets:"
	@echo "  build (default)            Build project. It installs dependencies and generates version.txt."
	@echo "  all                        Alias for \"build lint test-unit test-integration\"."
	@echo "  lint                       Run all linter tools. Alias for \"isort black codespell ruff pylint mypy bandit\"."
	@echo "  test-unit                  Run unit tests."
	@echo "  test-integration           Run integration tests."
	@echo "  isort                      Perform isort checks."
	@echo "  black                      Perform black checks."
	@echo "  codespell                  Perform codespell checks."
	@echo "  ruff                       Perform ruff checks."
	@echo "  pylint                     Perform pylint checks."
	@echo "  mypy                       Perform mypy checks.."
	@echo "  bandit                     Perform bandit checks."
	@echo "  clean                      Clean up build and test artifacts."
	@echo "  create-env                 Create test environment."
	@echo "  start-env                  Start test environment runtime."
	@echo "  stop-env                   Stop test environment runtime."
	@echo "  clean-env                  Clean up test environment."
	@echo "  debuild                    Build Debian package."
	@echo "  clean-debuild              Clean up build and test artifacts including ones produced by"
	@echo "                             debuild target outside the project worksapce."
	@echo "  format                     Re-format source code to conform style settings enforced by"
	@echo "                             isort and black tools."
	@echo "  help                       Show this help message."
	@echo
	@echo "Environment Variables:"
	@echo "  PYTHON                     Python executable to use (default: \"$(PYTHON)\")."
	@echo "  PYTEST_ARGS                Arguments to pass to pytest (unit tests)."
	@echo "  BEHAVE_ARGS                Arguments to pass to behave (integration tests)."
	@echo "  NO_VENV                    Disable creation of venv if the variable is set to non-empty value."
	@echo "  CLICKHOUSE_VERSION         ClickHouse version to use in integration tests (default: \"$(CLICKHOUSE_VERSION)\")."
