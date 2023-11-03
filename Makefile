export PYTHON?=python3
export PYTHONIOENCODING?=utf8
export NO_VENV?=
export COMPOSE_HTTP_TIMEOUT?=300
export CLICKHOUSE_VERSION?=latest

ifndef NO_VENV
  PATH:=venv/bin:${PATH}
endif

PYTHON_VERSION=$(shell ${PYTHON} -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
SESSION_FILE=.session_conf.sav
INSTALL_DIR=$(DESTDIR)/opt/yandex/ch-backup

TEST_ENV=env \
    PATH=${PATH} \
    PYTHON_VERSION=${PYTHON_VERSION} \
    PYTHONIOENCODING=${PYTHONIOENCODING} \
    CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION} \
    COMPOSE_HTTP_TIMEOUT=${COMPOSE_HTTP_TIMEOUT}

INTEGRATION_TEST_TOOL=${TEST_ENV} python -m tests.integration.env_control


.PHONY: build
build: install-deps ch_backup/version.txt

.PHONY: all
all: build lint test-unit test-integration

.PHONY: lint
lint: install-deps isort black codespell ruff pylint mypy bandit

.PHONY: isort
isort: install-deps
	${TEST_ENV} isort --check --diff .

.PHONY: black
black: install-deps
	${TEST_ENV} black --check --diff .

.PHONY: codespell
codespell: install-deps
	${TEST_ENV} codespell

.PHONY: fix-codespell-errors
fix-codespell-errors: install-deps
	${TEST_ENV} codespell -w

.PHONY: ruff
ruff: install-deps
	${TEST_ENV} ruff check ch_backup tests

.PHONY: pylint
pylint: install-deps
	${TEST_ENV} pylint ch_backup
	${TEST_ENV} pylint --disable=missing-docstring,invalid-name tests

.PHONY: mypy
mypy: install-deps
	${TEST_ENV} mypy ch_backup tests

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
clean: clean-env clean-pycache
	rm -rf venv *.egg-info htmlcov .coverage* .hypothesis .mypy_cache .pytest_cache .install-deps ch_backup/version.txt

.PHONY: clean-pycache
clean-pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +


.PHONY: install
install:
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


.PHONY: debuild
debuild: debian-changelog
	cd debian && \
	    debuild --check-dirname-level 0 --no-tgz-check --preserve-env -uc -us

.PHONY: debian-changelog
debian-changelog: build
	@rm -f debian/changelog
	dch --create --package ch-backup --distribution stable \
	    -v `cat ch_backup/version.txt` \
	    "Yandex autobuild"


.PHONY: clean-debuild
clean-debuild: clean
	rm -rf debian/{changelog,files,ch-backup*}
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
	@echo "2.$$(git rev-list HEAD --count).$$(git rev-parse --short HEAD | perl -ne 'print hex $$_')" > ch_backup/version.txt


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
