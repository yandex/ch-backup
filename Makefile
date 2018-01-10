.PHONY: all build lint test test_integration test_unit clean clean_pycache \
        install uninstall debuild debian_changelog clean-debuild format help \
        create_env start_env stop_env
TEST_VENV=.tox/py35_integration_test
ISORT_VENV=.tox/isort
YAPF_VENV=.tox/yapf
SESSION_FILE=.session_conf.sav
INSTALL_DIR=$(DESTDIR)/opt/yandex/ch-backup

build:

all: lint unit_test integration_test

test: lint unit_test

lint:
	git --no-pager diff HEAD~1 --check
	tox -e isort,yapf,flake8,pylint,bandit

unit_test:
	tox -e py35_unit_test

integration_test:
	tox -e py35_integration_test

clean: stop_env clean_pycache
	rm -rf staging .tox ${SESSION_FILE}
	rm -rf .cache
	rm -rf *.egg-info htmlcov .coverage*
	rm -rf .hypothesis

clean_pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +


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

uninstall:
	@echo "Uninstalling from $(INSTALL_DIR)"
	rm -rf $(INSTALL_DIR) $(DESTDIR)/usr/bin/ch-backup $(DESTDIR)/etc/bash_completion.d/ch-backup


debuild: debian_changelog
	cd debian && \
	    debuild --check-dirname-level 0 --no-tgz-check --preserve-env -uc -us

debian_changelog:
	@rm -f debian/changelog
	dch --create --package ch-backup --distribution trusty \
	    -v `git rev-list HEAD --count`-`git rev-parse --short HEAD` \
	    "Yandex autobuild"

clean-debuild: clean
	rm -rf debian/{changelog,files,ch-backup*}
	rm -f ../ch-backup_*{build,changes,deb,dsc,tar.gz}


create_env: ${TEST_VENV}
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control create

start_env: create_env
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control start

stop_env:
	test -d ${TEST_VENV}/bin && test -f ${SESSION_FILE} && \
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control stop || true


format: ${ISORT_VENV} ${YAPF_VENV}
	${ISORT_VENV}/bin/isort --recursive --apply ch_backup tests
	${YAPF_VENV}/bin/yapf --recursive --parallel --in-place ch_backup tests


${TEST_VENV}:
	tox -e py35_integration_test --notest

${ISORT_VENV}:
	tox -e isort --notest

${YAPF_VENV}:
	tox -e yapf --notest


help:
	@echo 'Targets:'
	@echo "  build (default)    Build project (it's currently no-op)."
	@echo '  all                Alias for "lint unit_test integration_test".'
	@echo '  test               Alias for "lint unit_test".'
	@echo '  lint               Run linter tools on tests source code.'
	@echo '  unit_test          Run unit tests.'
	@echo '  integration_test   Run integration tests.'
	@echo '  create_env         Create test environment.'
	@echo '  start_env          Start test environment runtime.'
	@echo '  stop_env           Stop test environment runtime.'
	@echo '  clean              Clean up test environment left from the previous test run.'
	@echo '  clean_pycache      Clean up __pycache__ directories.'
	@echo "  debuild            Build Debian package."
	@echo "  clean-debuild      Clean up build and test artifacts including ones produced by"
	@echo "                     debuild target outside the project worksapce."
	@echo '  format             Re-format source code to conform style settings enforced by'
	@echo '                     isort and yapf tools.'
	@echo '  help               Show this help message.'
