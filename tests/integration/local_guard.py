# ABOUTME: Decides whether the integration harness may run, and refuses anything but the local Supabase stack.
# ABOUTME: Stdlib-only so unit tests can import it without a bot module or environment variables.
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
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


def is_local_url(url: str | None) -> bool:
    if not url:
        return False
    return _hostname(url) in LOCAL_HOSTS


@dataclass(frozen=True)
class IntegrationSettings:
    url: str
    service_key: str
    db_url: str


def integration_settings(environ: Mapping[str, str]) -> tuple[IntegrationSettings | None, str]:
    """Return the settings, or None and a human reason the harness cannot run."""
    url = environ.get("SUPABASE_URL") or ""
    key = environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
    db_url = environ.get("SUPABASE_DB_URL") or DEFAULT_DB_URL
    if not url or not key:
        return None, f"SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must point at the local stack: {MAKE_HINT}"
    for name, value in (("SUPABASE_URL", url), ("SUPABASE_DB_URL", db_url)):
        if not is_local_url(value):
            host = _hostname(value) or "<unparseable>"
            return None, f"{name} host {host!r} is not local; integration tests only run against the local stack: {MAKE_HINT}"
    return IntegrationSettings(url=url, service_key=key, db_url=db_url), ""
