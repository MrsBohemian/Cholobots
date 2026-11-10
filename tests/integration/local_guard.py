# ABOUTME: Decides whether the integration harness may run, and refuses anything but the local Supabase stack.
# ABOUTME: Stdlib-only so unit tests can import it without a bot module or environment variables.
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
DEFAULT_API_PORT = 54321
DEFAULT_DB_URL = "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
MAKE_HINT = (
    "run `make test-integration` "
    "(or export SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY from `supabase status -o env`)"
)


def _hostname(url):
    try:
        return urlparse(url).hostname
    except ValueError:
        return None


def _port(url):
    try:
        return urlparse(url).port
    except ValueError:
        return None


def is_local_url(url: str | None) -> bool:
    if not url:
        return False
    return _hostname(url) in LOCAL_HOSTS


@dataclass(frozen=True)
class IntegrationSettings:
    url: str
    service_key: str
    db_url: str


def _not_local(name, value):
    host = _hostname(value) or "<unparseable>"
    return f"{name} host {host!r} is not local; integration tests only run against the local stack: {MAKE_HINT}"


def integration_settings(environ: Mapping[str, str]) -> tuple[IntegrationSettings | None, str]:
    """Return the settings, or None and a human reason the harness cannot run."""
    url = environ.get("SUPABASE_URL") or ""
    key = environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
    if not url or not key:
        return None, f"SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must point at the local stack: {MAKE_HINT}"
    if not is_local_url(url):
        return None, _not_local("SUPABASE_URL", url)
    db_url = environ.get("SUPABASE_DB_URL")
    if not db_url:
        if _port(url) != DEFAULT_API_PORT:
            return None, (
                f"SUPABASE_DB_URL is required when SUPABASE_URL is not on port {DEFAULT_API_PORT} "
                "(per-checkout stack); export the lines from scripts/supabase-local.sh env"
            )
        db_url = DEFAULT_DB_URL
    if not is_local_url(db_url):
        return None, _not_local("SUPABASE_DB_URL", db_url)
    # A stack's DB port is its API port plus one (54321/54322, 54331/54332, ...). A URL pair from
    # two stacks would reset one database while the bot clients use the other.
    api_port, db_port = _port(url), _port(db_url)
    if api_port is None or db_port != api_port + 1:
        return None, (
            f"SUPABASE_URL (port {api_port}) and SUPABASE_DB_URL (port {db_port}) belong to different stacks; "
            "export both from the same `scripts/supabase-local.sh env`"
        )
    return IntegrationSettings(url=url, service_key=key, db_url=db_url), ""
