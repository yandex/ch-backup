SHELL := bash

export PYTHON_VERSION ?= $(shell cat .python-version)
export PYTHONIOENCODING ?= utf8
export COMPOSE_HTTP_TIMEOUT ?= 300
export CLICKHOUSE_VERSION ?= latest
export PROJECT_NAME ?= ch-backup

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


SRC_DIR = ch_backup
TESTS_DIR = tests
VENV = .venv
SESSION_FILE = .session_conf.sav
INSTALL_DIR = $(DESTDIR)/opt/yandex/ch-backup
INTEGRATION_TEST_TOOL=uv run python -m tests.integration.env_control


.PHONY: build
build: setup
	uv build --python $(PYTHON_VERSION)


.PHONY: setup
setup: check-environment ch_backup/version.txt


.PHONY: all
all: lint test-unit build test-integration


.PHONY: lint
lint: setup isort black codespell ruff pylint mypy bandit


.PHONY: isort
isort: setup
	uv run --python $(PYTHON_VERSION) isort --check --diff $(SRC_DIR) $(TESTS_DIR)


.PHONY: black
black: setup
	uv run --python $(PYTHON_VERSION) black --check --diff $(SRC_DIR) $(TESTS_DIR)


.PHONY: codespell
codespell: setup
	uv run --python $(PYTHON_VERSION) codespell $(SRC_DIR) $(TESTS_DIR)


.PHONY: fix-codespell-errors
fix-codespell-errors: setup
	uv run --python $(PYTHON_VERSION) codespell -w $(SRC_DIR) $(TESTS_DIR)


.PHONY: ruff
ruff: setup
	uv run --python $(PYTHON_VERSION) ruff check $(SRC_DIR) $(TESTS_DIR)


.PHONY: pylint
pylint: setup
	uv run --python $(PYTHON_VERSION) pylint $(SRC_DIR)
	uv run --python $(PYTHON_VERSION) pylint --disable=missing-docstring,invalid-name $(TESTS_DIR)


.PHONY: mypy
mypy: setup
	uv run --python $(PYTHON_VERSION) mypy $(SRC_DIR) $(TESTS_DIR)


.PHONY: bandit
bandit: setup
	uv run --python $(PYTHON_VERSION) bandit -c bandit.yaml -r ch_backup


.PHONY: format
format: setup
	uv run --python ${PYTHON_VERSION} isort .
	uv run --python ${PYTHON_VERSION} black .


.PHONY: test-unit
test-unit: setup
	uv run --python $(PYTHON_VERSION) py.test $(PYTEST_ARGS) tests


.PHONY: test-integration
test-integration: create-test-env
	rm -rf staging/logs
	uv run behave --show-timings --stop -D skip_setup $(BEHAVE_ARGS) @tests/integration/ch_backup.featureset


.PHONY: clean
clean: clean-test-env clean-pycache clean-debuild
	rm -rf ${VENV} *.egg-info htmlcov .coverage* .hypothesis .mypy_cache .pytest_cache \
	     .ruff_cache ch_backup/version.txt dist

.PHONY: clean-pycache
clean-pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +


.PHONY: install
install:
	@echo "Installing into $(INSTALL_DIR)"
	python3 -m venv $(INSTALL_DIR)
	$(INSTALL_DIR)/bin/pip install --no-compile $(BUILD_PYTHON_OUTPUT_DIR)/ch_backup-*.whl
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
build-deb-package: setup
	./build_deb_in_docker.sh


.PHONY: build-deb-package-local
build-deb-package-local: prepare-changelog
	./build_deb.sh


.PHONY: prepare-changelog
prepare-changelog: setup
	@rm -f debian/changelog
	dch --create --package ch-backup --distribution stable \
	    -v `cat ch_backup/version.txt` \
	    "Yandex autobuild"


.PHONY: clean-debuild
clean-debuild:
	rm -rf debian/{changelog,files,ch-backup,.debhelper}
	rm -f ../ch-backup_*{build,changes,deb,dsc,tar.gz}


.PHONY: create-test-env
create-test-env: build ${SESSION_FILE}

${SESSION_FILE}:
	${INTEGRATION_TEST_TOOL} create


.PHONY: start-test-env
start-test-env: create-test-env
	${INTEGRATION_TEST_TOOL} start


.PHONY: stop-test-env
stop-test-env:
	test -f ${SESSION_FILE} && ${INTEGRATION_TEST_TOOL} stop || true


.PHONY: clean-test-env
clean-test-env: stop-test-env
	rm -rf staging ${SESSION_FILE}


ch_backup/version.txt:
	@echo "2.$$(git rev-list HEAD --count).$$(git rev-parse --short HEAD | perl -ne 'print hex $$_')" > ch_backup/version.txt


.PHONY: check-environment
check-environment:
	@if ! command -v "uv" &>/dev/null; then \
		echo 'Python project manager tool "uv" not found. Please follow installation instructions at https://docs.astral.sh/uv/getting-started/installation.' >&2; exit 1; \
	fi
	@if [ -z "${PYTHON_VERSION}" ]; then \
		echo 'Failed to determine version of Python interpreter to use.' >&2; exit 1; \
	fi


.PHONY: help
help:
	@echo "Targets:"
	@echo "  build (default)            Build Python packages (sdist and wheel)."
	@echo "  all                        Alias for \"lint test-unit build test-integration\"."
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
	@echo "  create-test-env            Create test environment."
	@echo "  start-test-env             Start test environment runtime."
	@echo "  stop-test-env              Stop test environment runtime."
	@echo "  clean-test-env             Clean up test environment."
	@echo "  build-deb-package          Build Debian package."
	@echo "  format                     Re-format source code to conform style settings enforced by"
	@echo "                             isort and black tools."
	@echo "  clean                      Clean up build and test artifacts."
	@echo "  help                       Show this help message."
	@echo
	@echo "Environment Variables:"
	@echo "  PYTHON_VERSION             Python version to use (default: \"$(PYTHON_VERSION)\")."
	@echo "  PYTEST_ARGS                Arguments to pass to pytest (unit tests)."
	@echo "  BEHAVE_ARGS                Arguments to pass to behave (integration tests)."
	@echo "  CLICKHOUSE_VERSION         ClickHouse version to use in integration tests (default: \"$(CLICKHOUSE_VERSION)\")."
