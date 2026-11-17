# Local development task runner. `make` (or `make help`) lists the targets.
# Keep it GNU make 3.81 compatible (the macOS default): plain sh recipes, no .ONESHELL.
# Nothing here talks to the hosted Supabase project; the db-* targets use the local Docker stack.

SUPABASE_CLI_VERSION := 2.121.0
# CLI used by scripts/supabase-local.sh; for a Homebrew or Scoop install of the same version:
# make SUPABASE_CLI=supabase <target>
SUPABASE_CLI ?= npx --yes supabase@$(SUPABASE_CLI_VERSION)
export SUPABASE_CLI
# One stack per checkout; make SUPABASE=supabase <target> bypasses the script.
SUPABASE ?= scripts/supabase-local.sh
VENV_PY := .venv/bin/python
# Production (Railway) installs requirements.txt, generated from uv.lock; --no-header keeps the file and stdout identical.
UV_EXPORT := uv export --frozen --no-dev --no-hashes --no-header --no-emit-project

.DEFAULT_GOAL := help
.PHONY: help setup lock lock-check check-tools db-up db-down db-reset db-env run compile lint test test-integration check

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "%-17s %s\n", $$1, $$2}'

setup: check-tools ## Install the locked dependencies into .venv with uv, the pre-commit hook, and .env
	uv sync --locked
	$(VENV_PY) -m pre_commit install
	[ -f .env ] || cp .env.example .env

lock: ## Re-lock after editing pyproject.toml and regenerate requirements.txt (for Railway)
	uv lock
	$(UV_EXPORT) -o requirements.txt

lock-check: ## Fail when uv.lock or requirements.txt is stale against pyproject.toml
	uv lock --check
	$(UV_EXPORT) | diff -u requirements.txt -

check-tools: ## Check the system tools setup and the tests need (uv, jq, timeout, setsid, curl)
	@missing=0; \
	for tool in uv jq timeout setsid curl; do \
		command -v $$tool >/dev/null 2>&1 && continue; \
		missing=1; \
		case $$tool in \
			uv) hint='brew install uv (macOS) or curl -LsSf https://astral.sh/uv/install.sh | sh; the version to use is in the README';; \
			jq) hint='brew install jq (macOS) or apt install jq (Debian/Ubuntu)';; \
			timeout) hint='brew install coreutils, then put "$$(brew --prefix coreutils)/libexec/gnubin" on PATH (macOS); part of coreutils on Linux';; \
			setsid) hint='brew install util-linux, then put "$$(brew --prefix util-linux)/bin" on PATH (macOS); part of util-linux on Linux';; \
			*) hint='brew install curl (macOS) or apt install curl (Debian/Ubuntu)';; \
		esac; \
		echo "Missing required tool '$$tool': $$hint" >&2; \
	done; \
	exit $$missing

db-up: ## Start this checkout's local Supabase stack in Docker
	$(SUPABASE) start

db-down: ## Stop this checkout's stack and delete its data
	$(SUPABASE) stop

db-reset: ## Rebuild the local database from supabase/migrations and supabase/seed.sql
	$(SUPABASE) db reset

db-env: ## Print the SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY lines to paste into .env
	@$(SUPABASE) status -o env --override-name api.url=SUPABASE_URL --override-name auth.service_role_key=SUPABASE_SERVICE_ROLE_KEY | grep -E '^(SUPABASE_URL|SUPABASE_SERVICE_ROLE_KEY)='

run: ## Check .env points at the local stack, then start the bot
	$(VENV_PY) scripts/check_local_env.py && $(VENV_PY) cholobots.py

compile: ## Byte-compile the bot modules
	$(VENV_PY) -m py_compile cholobots.py config.py db/database.py bots/*.py

lint: ## Run ruff
	$(VENV_PY) -m ruff check .

test: check-tools ## Run pytest (unit + scripts) and every tests/scripts/test-*.sh
	$(VENV_PY) -m pytest -q tests/unit tests/scripts
	for t in tests/scripts/test-*.sh; do echo "== $$t"; bash "$$t" || exit 1; done

test-integration: ## Run the integration tests against the local stack (make db-up first)
	@if [ -d tests/integration ]; then \
		env_lines="$$($(SUPABASE) status -o env --override-name api.url=SUPABASE_URL --override-name auth.service_role_key=SUPABASE_SERVICE_ROLE_KEY --override-name db.url=SUPABASE_DB_URL)" || exit 1; \
		eval "$$(printf '%s\n' "$$env_lines" | grep -E '^SUPABASE_(URL|SERVICE_ROLE_KEY|DB_URL)=' | sed -E 's/^([A-Z_]+)="?([^"]*)"?$$/: "$${\1:=\2}"; export \1/')"; \
		CHOLOBOTS_INTEGRATION_REQUIRED=1 $(VENV_PY) -m pytest -q -m integration tests/integration; \
	else echo "No integration tests yet (#51)."; fi

check: compile lint lock-check test ## Run compile, lint, lock-check and test
