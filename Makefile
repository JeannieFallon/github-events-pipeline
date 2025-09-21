VENV	:= .venv
PY		:= $(VENV)/bin/python
BLACK	:= $(VENV)/bin/black
RUFF	:= $(VENV)/bin/ruff
MYPY	:= $(VENV)/bin/mypy
PYTEST	:= $(VENV)/bin/pytest

OUTDIR	:= events

.PHONY: events run test fmt lint type clean help

events:
	@mkdir -p events

run: events
	$(PY) pipeline.py

test:
	$(PYTEST) -q

fmt:
	$(BLACK) .

lint:
	$(RUFF) check .

type:
	$(MYPY) .

clean:
	rm -rf $(OUTDIR)/*.csv __pycache__ .pytest_cache
