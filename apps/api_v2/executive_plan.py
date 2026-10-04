"""Executive Plan endpoint for the v2 integration API."""

from copy import deepcopy

from flask import request, send_file
from flask_restx import Namespace, Resource, fields

from apps.home import database, executive_plan, executive_plan_report

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

report_request_model = namespace.inherit(
    "ExecutivePlanReportRequest",
    request_model,
    {
        "teams": fields.List(
            fields.String(enum=["DEV", "OPS"]), required=True,
            description="Select DEV, OPS, or both. Shared DEV/OPS tasks are always included.",
            example=["DEV", "OPS"],
        ),
        "include_db_design": fields.Boolean(
            default=False,
            description="Include AI DB Design analysis using the server's configured LLM. May take several minutes.",
        ),
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


class _PdfFile(fields.Raw):
    """Describe a binary download in the Swagger 2 response schema."""

    __schema_type__ = "file"


@namespace.route("/report.pdf")
class ExecutivePlanReportResource(Resource):
    @namespace.doc(
        security="BearerAuth",
        produces=["application/pdf"],
        description=(
            "Generate the Workload Insight PDF report with the same DEV/OPS and "
            "optional AI DB Design analysis options as the web form. Runs the advisors "
            "again against the supplied database, without a web session. Errors are JSON."
        ),
    )
    @namespace.expect(report_request_model)
    @namespace.response(200, "PDF report generated", _PdfFile)
    @namespace.response(400, "Invalid request")
    @namespace.response(401, "Missing or invalid API token")
    @namespace.response(422, "Unable to connect to PostgreSQL")
    @namespace.response(500, "Report or AI analysis generation failed")
    @require_api_token
    def post(self):
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _error("The request body must be a JSON object.", 400)
        try:
            database_uri = validate_database_uri(payload.get("database_uri"), required=True)
        except ValueError as exc:
            return _error(str(exc), 400)

        teams = payload.get("teams")
        if not isinstance(teams, list) or not teams or any(
            not isinstance(team, str) or team not in {"DEV", "OPS"} for team in teams
        ):
            return _error("'teams' must be a non-empty array containing DEV and/or OPS.", 400)
        include_db_design = payload.get("include_db_design", False)
        if not isinstance(include_db_design, bool):
            return _error("'include_db_design' must be a boolean.", 400)

        db_config = database_config_from_uri(database_uri)
        try:
            _check_connection(db_config)
        except Exception:
            return _error("Unable to connect to the PostgreSQL database.", 422)

        try:
            pdf = executive_plan_report.build_report(
                db_config, list(dict.fromkeys(teams)), include_db_design=include_db_design,
            )
            database_name = database.get_resolved_database_name(db_config) or "database"
            safe_name = "".join(
                char if char.isalnum() or char in {"-", "_"} else "-"
                for char in database_name
            )
            return send_file(
                pdf, mimetype="application/pdf", as_attachment=True,
                download_name=f"pgassistant-executive-plan-{safe_name}.pdf",
            )
        except Exception:
            return _error("Unable to generate the Executive Plan PDF report.", 500)
