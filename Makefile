.PHONY: all lint test test_integration test_unit clean clean_pycache \
		help create_env start_env stop_env
TOX_BIN=.tox/py35_integration_test/bin
SESSION_FILE=.session_conf.sav

all: lint test

lint:
	git --no-pager diff HEAD~1 --check
	tox -e yapf,flake8,pylint,bandit

test: unit_test integration_test

integration_test:
	tox -e py35_integration_test

unit_test:
	tox -e py35_unit_test

clean: stop_env clean_pycache
	rm -rf staging .tox ${SESSION_FILE}
	rm -rf .cache
	rm -rf *.egg-info htmlcov .coverage*
	rm -rf .hypothesis

clean_pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +

${TOX_BIN}:
	tox -e py35_integration_test --notest

create_env: ${TOX_BIN}
	PATH=${TOX_BIN}:$$PATH ${TOX_BIN}/python -m tests.integration.env_control create

start_env: create_env
	PATH=${TOX_BIN}:$$PATH ${TOX_BIN}/python -m tests.integration.env_control start

stop_env:
	test -d ${TOX_BIN} && test -f ${SESSION_FILE} && \
	PATH=${TOX_BIN}:$$PATH ${TOX_BIN}/python -m tests.integration.env_control stop || true

help:
	@echo 'Targets:'
	@echo '  all (default)      Alias for "lint test".'
	@echo '  lint               Run linter tools on tests source code.'
	@echo '  test               Run tests.'
	@echo '  integration_test   Run integration tests.'
	@echo '  unit_test          Run unit tests.'
	@echo '  create_env         Create test environment.'
	@echo '  start_env          Start test environment runtime.'
	@echo '  stop_env           Stop test environment runtime.'
	@echo '  clean              Clean up test environment left from the previous test run.'
	@echo '  clean_pycache      Clean up __pycache__ directories.'
	@echo '  help               Show this help message.'
