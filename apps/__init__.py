# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from flask import Flask

from .version import __version__


def register_blueprints(app):
    from apps.home.routes import blueprint as home_blueprint
    app.register_blueprint(home_blueprint)

    # Keep the versioned integration API independent from historical routes.
    from apps.api_v2 import blueprint as api_v2_blueprint
    app.register_blueprint(api_v2_blueprint)


def create_app(config):
    app = Flask(__name__)
    app.config.from_object(config)
    app.config["PGASSISTANT_VERSION"] = __version__
    register_blueprints(app)
    return app
