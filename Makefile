PYTHON ?= python3

.PHONY: compile test check

compile:
	$(PYTHON) -m compileall -q bnl01_bot.py bnl_*.py tests

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

check: compile test
