TEST_VENV=.tox/py35_integration_test
ISORT_VENV=.tox/isort
YAPF_VENV=.tox/yapf
SESSION_FILE=.session_conf.sav
INSTALL_DIR=$(DESTDIR)/opt/yandex/ch-backup

CLICKHOUSE_VERSIONS?=1.1.54343 1.1.54385 18.5.1 18.12.17
CLICKHOUSE_VERSION?=$(lastword $(CLICKHOUSE_VERSIONS))


.PHONY: build
build:


.PHONY: all
all: lint unit_test integration_test


.PHONY: test
test: lint unit_test


.PHONY: lint
lint:
	git --no-pager diff HEAD~1 --check
	tox -e isort,yapf,flake8,pylint,bandit


.PHONY: unit_test
unit_test:
	tox -e py35_unit_test


.PHONY: integration_test
integration_test:
	CLICKHOUSE_VERSION=$(CLICKHOUSE_VERSION) tox -e py35_integration_test


.PHONY: integration_test_all
integration_test_all:
	@for version in $(CLICKHOUSE_VERSIONS); do \
		CLICKHOUSE_VERSION=$$version tox -e py35_integration_test; \
	done


.PHONY: clean
clean: clean_env clean_pycache
	rm -rf .tox .cache *.egg-info htmlcov .coverage* .hypothesis

.PHONY: clean_pycache
clean_pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +


.PHONY: install
install:
	@echo "Installing into $(INSTALL_DIR)"
	python3.5 -m venv $(INSTALL_DIR)
	$(INSTALL_DIR)/bin/pip install .
	mkdir -p $(DESTDIR)/usr/bin/
	ln -s /opt/yandex/ch-backup/bin/ch-backup $(DESTDIR)/usr/bin/
	mkdir -p $(DESTDIR)/etc/bash_completion.d/
	env LC_ALL=C.UTF-8 LANG=C.UTF-8 \
	    _CH_BACKUP_COMPLETE=source $(INSTALL_DIR)/bin/ch-backup > $(DESTDIR)/etc/bash_completion.d/ch-backup || \
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
debian_changelog:
	@rm -f debian/changelog
	dch --create --package ch-backup --distribution trusty \
	    -v `git rev-list HEAD --count`-`git rev-parse --short HEAD` \
	    "Yandex autobuild"


.PHONY: clean_debuild
clean_debuild: clean
	rm -rf debian/{changelog,files,ch-backup*}
	rm -f ../ch-backup_*{build,changes,deb,dsc,tar.gz}


.PHONY: create_env
create_env: ${TEST_VENV}
	CLICKHOUSE_VERSION=$(CLICKHOUSE_VERSION) PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control create


.PHONY: start_env
start_env: create_env
	CLICKHOUSE_VERSION=$(CLICKHOUSE_VERSION) PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control start


.PHONY: stop_env
stop_env:
	test -d ${TEST_VENV}/bin && test -f ${SESSION_FILE} && \
	CLICKHOUSE_VERSION=$(CLICKHOUSE_VERSION) PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control stop || true


.PHONY: clean_env
clean_env: stop_env
	rm -rf staging ${SESSION_FILE}


.PHONY: format
format: ${ISORT_VENV} ${YAPF_VENV}
	${ISORT_VENV}/bin/isort --recursive --apply ch_backup tests
	${YAPF_VENV}/bin/yapf --recursive --parallel --in-place ch_backup tests


${TEST_VENV}:
	CLICKHOUSE_VERSION=$(CLICKHOUSE_VERSION) tox -e py35_integration_test --notest

${ISORT_VENV}:
	tox -e isort --notest

${YAPF_VENV}:
	tox -e yapf --notest


.PHONY: help
help:
	@echo "Targets:"
	@echo "  build (default)            Build project (it's currently no-op)."
	@echo "  all                        Alias for \"lint unit_test integration_test\"."
	@echo "  test                       Alias for \"lint unit_test\"."
	@echo "  lint                       Run linter tools."
	@echo "  unit_test                  Run unit tests."
	@echo "  integration_test           Run integration tests."
	@echo "  integration_test_all       Run integration tests against all supported versions of ClickHouse."
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
	@echo "  CLICKHOUSE_VERSION         ClickHouse version to use in integration_test target (default: \"$(CLICKHOUSE_VERSION)\")."
	@echo "  CLICKHOUSE_VERSIONS        List of ClickHouse versions to use in integration_test_all target"
	@echo "                             (default: \"$(CLICKHOUSE_VERSIONS)\")."
