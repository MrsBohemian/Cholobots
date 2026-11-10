# Local development task runner for Cholobots. `make` (or `make help`) lists the targets.
# Keep it GNU make 3.81 compatible (the macOS default): plain sh recipes, no .ONESHELL.
# Every Supabase target works on the local Docker stack only; nothing here talks to the
# hosted project, and `make run` refuses a non-local SUPABASE_URL (scripts/check_local_env.py).

SUPABASE_CLI_VERSION := 2.121.0
# Homebrew or Scoop installs of the same version: make SUPABASE=supabase <target>
SUPABASE ?= npx --yes supabase@$(SUPABASE_CLI_VERSION)
PYTHON ?= python$(shell cat .python-version 2>/dev/null || echo 3)
VENV_PY := .venv/bin/python

.DEFAULT_GOAL := help
.PHONY: help setup check-tools db-up db-down db-reset db-env run compile lint test test-integration check

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "%-17s %s\n", $$1, $$2}'

setup: check-tools ## Check shell-test tools, create .venv, install the pinned requirements and the pre-commit hook, create .env
	$(PYTHON) -m venv .venv
	$(VENV_PY) -m pip install -r requirements.txt -r requirements-dev.txt
	$(VENV_PY) -m pre_commit install
	[ -f .env ] || cp .env.example .env

check-tools: ## Check the system tools the shell tests need (jq, timeout, setsid, curl)
	@missing=0; \
	for tool in jq timeout setsid curl; do \
		command -v $$tool >/dev/null 2>&1 && continue; \
		missing=1; \
		case $$tool in \
			jq) hint='brew install jq (macOS) or apt install jq (Debian/Ubuntu)';; \
			timeout) hint='brew install coreutils, then put "$$(brew --prefix coreutils)/libexec/gnubin" on PATH (macOS); part of coreutils on Linux';; \
			setsid) hint='brew install util-linux, then put "$$(brew --prefix util-linux)/bin" on PATH (macOS); part of util-linux on Linux';; \
			*) hint='brew install curl (macOS) or apt install curl (Debian/Ubuntu)';; \
		esac; \
		echo "Missing required tool '$$tool': $$hint" >&2; \
	done; \
	exit $$missing

db-up: ## Start the local Supabase stack in Docker
	$(SUPABASE) start

db-down: ## Stop the local Supabase stack
	$(SUPABASE) stop

db-reset: ## Rebuild the local database from supabase/migrations (and seed.sql when present)
	$(SUPABASE) db reset

db-env: ## Print the SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY lines to paste into .env
	@$(SUPABASE) status -o env --override-name api.url=SUPABASE_URL --override-name auth.service_role_key=SUPABASE_SERVICE_ROLE_KEY | grep -E '^(SUPABASE_URL|SUPABASE_SERVICE_ROLE_KEY)='

run: ## Check .env points at the local stack, then start the bot
	$(VENV_PY) scripts/check_local_env.py && $(VENV_PY) cholobots.py

compile: ## Byte-compile the bot modules
	$(VENV_PY) -m py_compile cholobots.py config.py db/database.py bots/*.py

lint: ## Run ruff
	$(VENV_PY) -m ruff check .

test: check-tools ## Run the unit tests, the script pytest tests and every tests/scripts/test-*.sh
	$(VENV_PY) -m pytest -q tests/unit tests/scripts
	for t in tests/scripts/test-*.sh; do echo "== $$t"; bash "$$t" || exit 1; done

test-integration: ## Run the integration tests against the local stack
	@if [ -d tests/integration ]; then $(VENV_PY) -m pytest -q -m integration tests/integration; else echo "No integration tests yet (#51)."; fi

check: compile lint test ## Run compile, lint and test
