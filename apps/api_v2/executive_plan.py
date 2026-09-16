"""Executive Plan endpoint for the v2 integration API."""

from copy import deepcopy

from flask import request
from flask_restx import Namespace, Resource, fields

from apps.home import database, executive_plan

from . import api
from .auth import require_api_token
from .validation import database_config_from_uri, validate_database_uri


namespace = Namespace(
    "executive-plan",
    description="Prioritized PostgreSQL continuous-improvement plan",
    path="/executive-plan",
)
api.add_namespace(namespace)

request_model = namespace.model(
    "ExecutivePlanRequest",
    {
        "database_uri": fields.String(
            required=True,
            description="PostgreSQL connection URI used by the Executive Plan advisors.",
            example="postgresql://user:password@postgres:5432/application",
        ),
    },
)

response_model = namespace.model(
    "ExecutivePlanResponse",
    {
        "status": fields.String(example="ok"),
        "database": fields.String(example="application"),
        "phases": fields.List(
            fields.Raw,
            description="Ordered implementation phases referencing canonical tasks by task_ids.",
        ),
        "tasks": fields.List(
            fields.Raw,
            description="Canonical tasks; every recommendation occurs exactly once here.",
        ),
        "errors": fields.List(fields.Raw, description="Advisor-level errors, if any."),
        "summary": fields.Raw(description="Recommendation and task totals."),
        "postgres_context": fields.Raw(description="PostgreSQL version and settings context."),
    },
)


def _error(message, status_code):
    return {"success": False, "error": message}, status_code


def _check_connection(db_config):
    connection, message = database.connectdb(db_config)
    if connection is None:
        raise RuntimeError(message or "Unable to connect to PostgreSQL.")
    connection.close()


def _serialize_plan(plan):
    """Return an integration-friendly plan without the UI's repeated objects."""
    result = deepcopy(plan)

    canonical_tasks = []
    for task in result.get("tasks") or []:
        task.pop("recommendation_groups", None)
        canonical_tasks.append(task)
    result["tasks"] = canonical_tasks

    normalized_phases = []
    for phase in result.get("phases") or []:
        phase_tasks = phase.pop("tasks", []) or []
        phase["task_ids"] = [task.get("id") for task in phase_tasks if task.get("id")]
        normalized_phases.append(phase)
    result["phases"] = normalized_phases
    return result


@namespace.route("")
class ExecutivePlanResource(Resource):
    @namespace.doc(
        security="BearerAuth",
        description=(
            "Build the complete Executive Plan for a PostgreSQL database. Bearer "
            "authentication is enforced only when PGA_API_TOKEN is configured."
        ),
    )
    @namespace.expect(request_model)
    @namespace.response(200, "Executive Plan generated", response_model)
    @namespace.response(400, "Invalid request")
    @namespace.response(401, "Missing or invalid API token")
    @namespace.response(422, "Unable to connect to PostgreSQL")
    @namespace.response(500, "Executive Plan generation failed")
    @require_api_token
    def post(self):
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _error("The request body must be a JSON object.", 400)

        try:
            database_uri = validate_database_uri(
                payload.get("database_uri"),
                required=True,
            )
        except ValueError as exc:
            return _error(str(exc), 400)

        db_config = database_config_from_uri(database_uri)
        try:
            _check_connection(db_config)
        except Exception:
            # A libpq error may contain connection details. Keep the public
            # response stable and avoid leaking any part of the supplied URI.
            return _error("Unable to connect to the PostgreSQL database.", 422)

        try:
            plan = executive_plan.build_executive_plan(db_config)
            return _serialize_plan(plan), 200
        except Exception:
            return _error("Unable to generate the Executive Plan.", 500)
