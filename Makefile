.PHONY: all lint test test_integration test_unit clean clean_pycache \
        format help create_env start_env stop_env
TEST_VENV=.tox/py35_integration_test
ISORT_VENV=.tox/isort
YAPF_VENV=.tox/yapf
SESSION_FILE=.session_conf.sav

all: lint test

lint:
	git --no-pager diff HEAD~1 --check
	tox -e isort,yapf,flake8,pylint,bandit

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
	@echo '  format             Re-format source code to conform style settings enforced by'
	@echo '                     isort and yapf tools.'
	@echo '  help               Show this help message.'
