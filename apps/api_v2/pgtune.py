"""pgTune endpoint for the v2 integration API."""

from flask import request
from flask_restx import Namespace, Resource, fields

from apps.home import database, pgtune, pgtune_resource_detector

from . import api
from .auth import require_api_token
from .validation import database_config_from_uri, validate_database_uri


namespace = Namespace("pgtune", description="PostgreSQL configuration sizing", path="/pgtune")
api.add_namespace(namespace)

request_model = namespace.model(
    "PgTuneRequest",
    {
        "database_uri": fields.String(
            description=(
                "Optional PostgreSQL URI. When provided, pgAssistant detects the "
                "server resources, PostgreSQL version, current settings and generates SQL."
            ),
            example="postgresql://user:password@postgres:5432/application",
        ),
        "cpu": fields.Integer(
            min=1,
            description="CPU count. Required without database_uri; overrides detection when set.",
            example=4,
        ),
        "memory_mb": fields.Integer(
            min=1,
            description="Memory in MiB. Required without database_uri; overrides detection when set.",
            example=8192,
        ),
        "postgresql_version": fields.String(
            description="PostgreSQL major version. Defaults to 18 without database_uri.",
            example="18",
        ),
        "database_type": fields.String(
            enum=["web", "oltp", "dw"],
            default="web",
            description="Workload profile.",
        ),
        "storage": fields.String(
            enum=["ssd", "san", "hdd"],
            default="ssd",
            description="Storage type.",
        ),
        "max_connections": fields.Integer(
            min=1,
            default=100,
            description="Maximum connections; current database value is used when available.",
        ),
    },
)


def _error(message, status_code, **details):
    payload = {"success": False, "error": message}
    payload.update(details)
    return payload, status_code


def _positive_integer(payload, name, required=False):
    value = payload.get(name)
    if value is None:
        if required:
            raise ValueError(f"'{name}' is required when 'database_uri' is not provided.")
        return None
    if isinstance(value, bool):
        raise ValueError(f"'{name}' must be a positive integer.")
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"'{name}' must be a positive integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"'{name}' must be a positive integer.") from exc
    if parsed < 1:
        raise ValueError(f"'{name}' must be a positive integer.")
    return parsed


def _database_inputs(database_uri):
    db_config = database_config_from_uri(database_uri)
    connection, connection_message = database.connectdb(db_config)
    if connection is None:
        raise RuntimeError(connection_message or "Unable to connect to PostgreSQL.")
    try:
        resources = pgtune_resource_detector.detect_postgresql_resources(connection)
    finally:
        connection.close()

    parameter_result = database.get_pg_tune_parameter(db_config)
    if not parameter_result:
        raise RuntimeError("Unable to read the PostgreSQL configuration.")
    running_values, major_version = parameter_result
    return resources, running_values, str(major_version)


@namespace.route("")
class PgTuneResource(Resource):
    @namespace.doc(
        security="BearerAuth",
        description=(
            "Generate PostgreSQL tuning recommendations. Bearer authentication is "
            "enforced only when PGA_API_TOKEN is configured on the server."
        ),
    )
    @namespace.expect(request_model)
    @namespace.response(200, "Recommendations generated")
    @namespace.response(400, "Invalid request")
    @namespace.response(401, "Missing or invalid API token")
    @namespace.response(422, "Database connection or resource detection failed")
    @require_api_token
    def post(self):
        payload = request.get_json(silent=True)
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            return _error("The request body must be a JSON object.", 400)

        try:
            database_uri = validate_database_uri(payload.get("database_uri"))
            cpu = _positive_integer(payload, "cpu", required=not database_uri)
            memory_mb = _positive_integer(payload, "memory_mb", required=not database_uri)
            max_connections = _positive_integer(payload, "max_connections")
        except ValueError as exc:
            return _error(str(exc), 400)

        database_type = str(payload.get("database_type") or "web").lower()
        storage = str(payload.get("storage") or "ssd").lower()
        if database_type not in {"web", "oltp", "dw"}:
            return _error("'database_type' must be one of: web, oltp, dw.", 400)
        if storage not in {"ssd", "san", "hdd"}:
            return _error("'storage' must be one of: ssd, san, hdd.", 400)

        resources = None
        running_values = None
        detected_version = None
        if database_uri:
            try:
                resources, running_values, detected_version = _database_inputs(database_uri)
            except Exception:
                # Do not expose connection details (and potentially credentials)
                # through errors returned to API clients.
                return _error("Unable to inspect the PostgreSQL database.", 422)
            cpu = cpu or resources["cpu"]
            memory_mb = memory_mb or resources["memory_mb"]

        version = str(payload.get("postgresql_version") or detected_version or "18")
        if not version.isdigit() or int(version) < 10:
            return _error("'postgresql_version' must be a supported major version.", 400)

        if max_connections is None:
            try:
                max_connections = int((running_values or {}).get("max_connections", 100))
            except (TypeError, ValueError):
                max_connections = 100

        tuner = pgtune.pgTune(
            version,
            cpu,
            f"{memory_mb}MB",
            storage,
            database_type,
            max_connections,
        )
        recommendations = tuner.get_pg_tune()
        response = {
            "success": True,
            "source": "database" if database_uri else "request",
            "inputs": {
                "cpu": cpu,
                "memory_mb": memory_mb,
                "postgresql_version": version,
                "database_type": database_type,
                "storage": storage,
                "max_connections": max_connections,
            },
            "recommendations": recommendations,
        }
        if resources:
            response["detected_resources"] = resources
            response["current_values"] = running_values
            response["alter_system_sql"] = tuner.get_alter_system(running_values)
        return response, 200
