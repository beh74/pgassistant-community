"""Versioned API blueprint.

The v2 API deliberately lives outside the historical home blueprint so its
contract and authentication can evolve without changing existing routes.
"""

from flask import Blueprint
from flask_restx import Api


blueprint = Blueprint("api_v2", __name__, url_prefix="/api/v2")

api = Api(
    blueprint,
    version="2.0",
    title="pgAssistant API",
    description="Versioned integration API for pgAssistant.",
    doc="/docs",
    authorizations={
        "BearerAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "Authorization",
            "description": (
                "Use `Bearer <token>` when PGA_API_TOKEN is configured. "
                "Authentication is disabled when that environment variable is empty."
            ),
        }
    },
)


from . import executive_plan, pgtune  # noqa: E402,F401
