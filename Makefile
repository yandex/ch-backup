export PYTHON?=python3
export PYTHONIOENCODING?=utf8
export CLICKHOUSE_VERSION?=23.3.7.5

SESSION_FILE=.session_conf.sav
INSTALL_DIR=$(DESTDIR)/opt/yandex/ch-backup

INTEGRATION_TEST_ENV=env -i \
    PATH=venv/bin:$$PATH \
    PYTHONIOENCODING=${PYTHONIOENCODING} \
    CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION} \
    DOCKER_HOST=$$DOCKER_HOST \
    DOCKER_TLS_VERIFY=$$DOCKER_TLS_VERIFY \
    DOCKER_CERT_PATH=$$DOCKER_CERT_PATH \
    COMPOSE_HTTP_TIMEOUT=300

INTEGRATION_TEST_TOOL=${INTEGRATION_TEST_ENV} venv/bin/python -m tests.integration.env_control


.PHONY: build
build: ch_backup/version.txt


.PHONY: all
all: build lint unit_test integration_test


.PHONY: test
test: lint unit_test


.PHONY: lint
lint: venv build git-diff-check isort yapf flake8 pylint mypy bandit

.PHONY: git-diff-check
git-diff-check:
	git --no-pager diff HEAD~1 --check

.PHONY: isort
isort:
	venv/bin/isort --check-only --ignore-whitespace --diff ch_backup tests

.PHONY: yapf
yapf:
	venv/bin/yapf -rpd ch_backup tests

.PHONY: flake8
flake8:
	venv/bin/flake8 ch_backup tests

.PHONY: pylint
pylint:
	venv/bin/pylint --reports=no --score=no ch_backup
	venv/bin/pylint --disable=redefined-outer-name,missing-docstring,invalid-name --reports=no --score=no tests

.PHONY: mypy
mypy:
	venv/bin/mypy ch_backup tests

.PHONY: bandit
bandit:
	venv/bin/bandit -c bandit.yaml -r ch_backup


.PHONY: unit_test
unit_test: venv build
	venv/bin/py.test $(PYTEST_ARGS) tests


.PHONY: integration_test
integration_test: venv build create_env
	rm -rf staging/logs
	${INTEGRATION_TEST_ENV} venv/bin/behave --show-timings --stop -D skip_setup $(BEHAVE_ARGS) @tests/integration/ch_backup.featureset


.PHONY: clean
clean: clean_env clean_pycache
	rm -rf venv *.egg-info htmlcov .coverage* .hypothesis .mypy_cache .pytest_cache ch_backup/version.txt

.PHONY: clean_pycache
clean_pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +


.PHONY: install
install:
	@echo "Installing into $(INSTALL_DIR)"
	python3.6 -m venv $(INSTALL_DIR)
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
debuild: debian_changelog
	cd debian && \
	    debuild --check-dirname-level 0 --no-tgz-check --preserve-env -uc -us

.PHONY: debian_changelog
debian_changelog: build
	@rm -f debian/changelog
	dch --create --package ch-backup --distribution stable \
	    -v `cat ch_backup/version.txt` \
	    "Yandex autobuild"


.PHONY: clean_debuild
clean_debuild: clean
	rm -rf debian/{changelog,files,ch-backup*}
	rm -f ../ch-backup_*{build,changes,deb,dsc,tar.gz}


.PHONY: create_env
create_env: venv build ${SESSION_FILE}

${SESSION_FILE}:
	${INTEGRATION_TEST_TOOL} create


.PHONY: start_env
start_env: create_env
	${INTEGRATION_TEST_TOOL} start


.PHONY: stop_env
stop_env:
	test -d venv/bin && test -f ${SESSION_FILE} && ${INTEGRATION_TEST_TOOL} stop || true


.PHONY: clean_env
clean_env: stop_env
	rm -rf staging ${SESSION_FILE}


.PHONY: format
format: venv
	venv/bin/isort ch_backup tests
	venv/bin/yapf --recursive --parallel --in-place ch_backup tests


ch_backup/version.txt:
	@echo "2.$$(git rev-list HEAD --count).$$(git rev-parse --short HEAD | perl -ne 'print hex $$_')" > ch_backup/version.txt


venv: requirements.txt requirements-dev.txt
	${PYTHON} -m venv venv
	venv/bin/pip install --no-cache-dir --disable-pip-version-check -r requirements.txt -r requirements-dev.txt


.PHONY: help
help:
	@echo "Targets:"
	@echo "  build (default)            Build project (it only generates version.txt for now)."
	@echo "  all                        Alias for \"build lint unit_test integration_test\"."
	@echo "  test                       Alias for \"lint unit_test\"."
	@echo "  lint                       Run all linter tools. Alias for \"git-diff-check isort yapf"
	@echo "                             flake8 pylint mypy bandit\"."
	@echo "  unit_test                  Run unit tests."
	@echo "  integration_test           Run integration tests."
	@echo "  git-diff-check             Perform \"git diff --check\"."
	@echo "  isort                      Perform isort checks."
	@echo "  yapf                       Perform yapf checks."
	@echo "  flake8                     Perform flake8 checks."
	@echo "  pylint                     Perform pylint checks."
	@echo "  mypy                       Perform mypy checks.."
	@echo "  bandit                     Perform bandit checks."
	@echo "  clean                      Clean up build and test artifacts."
	@echo "  create_env                 Create test environment."
	@echo "  start_env                  Start test environment runtime."
	@echo "  stop_env                   Stop test environment runtime."
	@echo "  clean_env                  Clean up test environment."
	@echo "  debuild                    Build Debian package."
	@echo "  clean_debuild              Clean up build and test artifacts including ones produced by"
	@echo "                             debuild target outside the project worksapce."
	@echo "  format                     Re-format source code to conform style settings enforced by"
	@echo "                             isort and yapf tools."
	@echo "  help                       Show this help message."
	@echo
	@echo "Environment Variables:"
	@echo "  PYTHON                     Python executable to use (default: \"$(PYTHON)\")."
	@echo "  PYTEST_ARGS                Arguments to pass to pytest (unit tests)."
	@echo "  BEHAVE_ARGS                Arguments to pass to behave (integration tests)."
	@echo "  CLICKHOUSE_VERSION         ClickHouse version to use in integration tests (default: \"$(CLICKHOUSE_VERSION)\")."
