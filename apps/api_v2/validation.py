"""Shared request validation for v2 API resources."""

from psycopg2.extensions import parse_dsn


def validate_database_uri(value, *, required=False):
    """Return a validated PostgreSQL URI without ever including it in an error."""
    if value is None or not str(value).strip():
        if required:
            raise ValueError("'database_uri' is required.")
        return None

    uri = str(value).strip()
    if not uri.lower().startswith(("postgresql://", "postgres://")):
        raise ValueError("'database_uri' must be a PostgreSQL URI.")
    try:
        parse_dsn(uri)
    except Exception as exc:
        raise ValueError("'database_uri' is not a valid PostgreSQL URI.") from exc
    return uri


def database_config_from_uri(uri):
    """Build the minimal internal config and retain the URI database name."""
    parsed = parse_dsn(uri)
    return {
        "db_uri": uri,
        "db_name": str(parsed.get("dbname") or "").strip(),
    }
