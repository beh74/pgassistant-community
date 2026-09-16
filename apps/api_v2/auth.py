"""Authentication helpers for the versioned integration API."""

import hmac
import os
from functools import wraps

from flask import jsonify, make_response, request


TOKEN_ENVIRONMENT_VARIABLE = "PGA_API_TOKEN"


def require_api_token(view):
    """Require a Bearer token only when the server has one configured."""

    @wraps(view)
    def wrapped(*args, **kwargs):
        expected_token = os.getenv(TOKEN_ENVIRONMENT_VARIABLE, "").strip()
        if not expected_token:
            return view(*args, **kwargs)

        authorization = request.headers.get("Authorization", "")
        scheme, separator, supplied_token = authorization.partition(" ")
        authenticated = (
            bool(separator)
            and scheme.lower() == "bearer"
            and bool(supplied_token.strip())
            and hmac.compare_digest(supplied_token.strip(), expected_token)
        )
        if authenticated:
            return view(*args, **kwargs)

        response = make_response(
            jsonify({"success": False, "error": "Missing or invalid API token."}),
            401,
        )
        response.headers["WWW-Authenticate"] = "Bearer"
        return response

    return wrapped
