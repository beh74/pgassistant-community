# -*- encoding: utf-8 -*-
"""
API routes.

"""
import json

import requests
from flask import Response, jsonify, request, session

from apps.home import blueprint
from . import action
from . import api_helper
from . import database
from . import reporting
from . import sqlhelper
from . import indexe_helper
from . import query_index_advisor
from . import schema_helper


@blueprint.route("/execute", methods=["POST"])
def execute_sql():
    con = None
    try:
        sql = request.json.get("sql")
        if not sql:
            return jsonify({"error": "No SQL clause provided", "success": False})

        con, _ = database.connectdb(session)
        result = database.db_exec_recommandation(con, sql)

        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if con is not None:
            con.close()


@blueprint.route('/api/v1/fetch_column_data', methods=['POST'])
def fetch_column_data_route():
    """
    Flask route to fetch data from a column in a table.
    Expects a JSON payload with 'table', 'column', and 'data_type'.
    """
    try:
        payload = request.json
        table = payload.get('table')
        column = payload.get('column')
        data_type = payload.get('data_type')

        if not table or not column or not data_type:
            return jsonify({"error": "Missing required parameters (table, column, data_type)."}), 400

        result = sqlhelper.fetch_column_data(table, column, data_type, session)

        return jsonify({"data": result})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint.route("/api/v1/llm_get_models", methods=["POST"])
def llm_get_models():
    data = request.get_json()
    llm_uri = data.get("llm_uri", "").rstrip("/")
    api_key = data.get("llm_api_key", "").strip()

    if not llm_uri:
        return jsonify({"error": "Missing LLM URI"}), 400

    models_url = f"{llm_uri}/models"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        response = requests.get(models_url, headers=headers, timeout=10)
        response.raise_for_status()
        models_data = response.json()

        model_names = [m.get("id") for m in models_data.get("data", []) if "id" in m]

        return jsonify({"models": model_names})

    except Exception:
        return jsonify({"error": "Could not fetch models"}), 500


@blueprint.route("/api/v1/report", methods=["POST"])
def api_database_report():
    try:
        data = request.get_json(force=True)

        if not data or "db_config" not in data:
            return jsonify({"error": "Missing 'db_config' in request body"}), 400
        db_config = data["db_config"]

        required_keys = ["db_host", "db_port", "db_name", "db_user", "db_password"]
        missing = [k for k in required_keys if k not in db_config]
        if missing:
            return jsonify({"error": f"Missing keys in db_config: {', '.join(missing)}"}), 400

        database_reports = reporting.get_database_report(
            db_config,
            report_yaml_definition_file="./reporting.yml",
            template_folder="db_report_templates"
        )
        if not database_reports:
            return jsonify({"error": "No report generated"}), 500
        return Response(database_reports, mimetype="text/markdown; charset=utf-8")

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint.route("/api/v1/rank_top_10_queries", methods=["GET"])
def api_rank_top_10_queries():
    try:
        data = request.get_json(force=True)

        if not data or "db_config" not in data:
            return jsonify({"error": "Missing 'db_config' in request body"}), 400
        db_config = data["db_config"]
        ranked_queries = api_helper.get_rank_top_10_queries(db_config)
        return jsonify({"ranked_queries": ranked_queries})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint.route("/api/v1/query_index_advisor", methods=["GET", "POST"])
def api_query_index_advisor():
    try:
        data = request.get_json(silent=True) or {}
        db_config = data.get("db_config") or session

        if not db_config or not (db_config.get("db_name") or db_config.get("db_uri")):
            return jsonify({"success": False, "error": "Database is not connected."}), 401

        limit = data.get("limit", 10)
        try:
            limit = max(1, min(int(limit), 50))
        except (TypeError, ValueError):
            limit = 10

        result = query_index_advisor.analyze_top_ranked_query_indexes(
            db_config,
            limit=limit,
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/v1/global_advisor", methods=["GET"])
def api_global_advisor():
    try:
        data = request.get_json(force=True)

        if not data or "db_config" not in data:
            return jsonify({"error": "Missing 'db_config' in request body"}), 400
        db_config = data["db_config"]
        ranked_queries = api_helper.get_top_10_global_advisor_recommendations(db_config)
        return jsonify({"ranked_queries": ranked_queries})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint.route("/api/v1/dashboard/dev_advisor", methods=["GET"])
def api_dashboard_dev_advisor():
    try:
        if not session.get("db_name"):
            return jsonify({"status": "error", "error": "Database is not connected."}), 401

        result = api_helper.get_dev_advisor_dashboard(session)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@blueprint.route("/api/v1/dashboard/postgresql_version", methods=["GET"])
def api_dashboard_postgresql_version():
    try:
        if not session.get("db_name"):
            return jsonify({"status": "error", "error": "Database is not connected."}), 401

        result = api_helper.get_postgresql_version_advisor(session)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@blueprint.route("/api/v1/pg_stat_statements_reset", methods=["POST"])
def api_reset_stats():
    try:
        data = request.get_json(force=True)

        if not data or "db_config" not in data:
            return jsonify({"error": "Missing 'db_config' in request body"}), 400
        db_config = data["db_config"]

        required_keys = ["db_host", "db_port", "db_name", "db_user", "db_password"]
        missing = [k for k in required_keys if k not in db_config]
        if missing:
            return jsonify({"error": f"Missing keys in db_config: {', '.join(missing)}"}), 400

        database.exec_cmd(db_config, "pg_stat_statements_reset")

        return Response("pg_stat_statements statistics are reset", mimetype="text/markdown; charset=utf-8")

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint.route("/api/v1/apply_recommandations", methods=["POST"])
def api_apply_recommendations():
    try:
        data = request.get_json(force=True)

        if not data or "db_config" not in data:
            return jsonify({"error": "Missing 'db_config' in request body"}), 400

        db_config = data["db_config"]

        if "unique_name" not in data or not data["unique_name"]:
            return jsonify({"error": "Missing or empty 'unique_name'"}), 400
        unique_name = data["unique_name"]

        required_keys = ["db_host", "db_port", "db_name", "db_user", "db_password"]
        missing = [k for k in required_keys if k not in db_config]
        if missing:
            return jsonify({"error": f"Missing keys in db_config: {', '.join(missing)}"}), 400

        dryrun = data.get("dryrun", True)
        if not isinstance(dryrun, bool):
            return jsonify({"error": "'dryrun' must be a boolean"}), 400

        allowed_reco = {
            "create_missing_fk_indexes",
            "drop_duplicate_indexes",
            "alter_table_columns_on_fk",
            "drop_unused_indexes",
            "all",
        }
        run_recommandations = data.get("run_recommandations", None)

        if run_recommandations is None:
            return jsonify({"error": "Missing 'run_recommandations'"}), 400

        if not isinstance(run_recommandations, list):
            return jsonify({"error": "'run_recommandations' must be a list"}), 400

        unknown = [x for x in run_recommandations if x not in allowed_reco]
        if unknown:
            return jsonify({"error": f"Unknown recommendation(s): {', '.join(unknown)}"}), 400

        if "all" in run_recommandations:
            run_recommandations = list(allowed_reco - {"all"})

        executed_sql, errors = action.run_actions(db_config, unique_name=unique_name, dry_mode=dryrun)
        result = {
            "dryrun": dryrun,
            "run_recommandations": run_recommandations,
            "message": "Recommendations executed." if not errors else "Errors occurred while executing recommendations.",
            "executed_sql": executed_sql,
            "errors": errors
        }

        if errors:
            return Response(
                json.dumps(result, indent=2),
                mimetype="application/json; charset=utf-8",
                status=400
            )

        return Response(
            json.dumps(result, indent=2),
            mimetype="application/json; charset=utf-8",
            status=200
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

  

@blueprint.route("/api/v1/indexe_stats/<schemaname>/<indexname>", methods=["GET"])
def api_indexe_stats_by_schema(schemaname, indexname):
    """
    Return detailed statistics for one index identified by schema and index name.

    Example:
      /api/v1/indexe_stats/bookings/segments_pkey
    """
    conn = None

    try:
        if not session.get("db_host") and not session.get("db_uri"):
            return jsonify({
                "success": False,
                "error": "No database connection found in session."
            }), 400

        conn, status = database.connectdb(session)

        if conn is None or status != "OK":
            return jsonify({
                "success": False,
                "error": status or "Unable to connect to database."
            }), 500

        qualified_index_name = f"{schemaname}.{indexname}"

        result = indexe_helper.get_index_stats_by_name(
            conn,
            qualified_index_name
        )

        status_code = 200 if result.get("success") else 404
        return jsonify(result), status_code

    except Exception as exc:
        return jsonify({
            "success": False,
            "error": str(exc)
        }), 500

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

@blueprint.route("/api/v1/table_indexe_stats/<schemaname>/<tablename>", methods=["GET"])
def api_table_indexe_stats(schemaname, tablename):
    """
    Return detailed statistics for all indexes attached to a table.

    Example:
      /api/v1/table_indexe_stats/public/orders
    """
    conn = None

    try:
        if not session.get("db_host") and not session.get("db_uri"):
            return jsonify({
                "success": False,
                "error": "No database connection found in session."
            }), 400

        conn, status = database.connectdb(session)

        if conn is None or status != "OK":
            return jsonify({
                "success": False,
                "error": status or "Unable to connect to database."
            }), 500

        result = indexe_helper.get_table_indexes_stats(
            conn,
            schemaname=schemaname,
            table_name=tablename
        )

        status_code = 200 if result.get("success") else 404
        return jsonify(result), status_code

    except Exception as exc:
        return jsonify({
            "success": False,
            "error": str(exc)
        }), 500

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


@blueprint.route("/api/v1/database_indexe_stats", methods=["GET"])
@blueprint.route("/api/v1/database_indexes_stats", methods=["GET"])
def api_database_indexe_stats():
    """
    Return detailed statistics for all user indexes in the connected database.

    PostgreSQL internal schemas and information_schema are excluded.

    Example:
      /api/v1/database_indexe_stats
      /api/v1/database_indexes_stats
    """
    conn = None

    try:
        if not session.get("db_host") and not session.get("db_uri"):
            return jsonify({
                "success": False,
                "error": "No database connection found in session."
            }), 400

        conn, status = database.connectdb(session)

        if conn is None or status != "OK":
            return jsonify({
                "success": False,
                "error": status or "Unable to connect to database."
            }), 500

        result = indexe_helper.get_database_indexes_stats(conn)
        return jsonify(result), 200

    except Exception as exc:
        return jsonify({
            "success": False,
            "error": str(exc)
        }), 500

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


@blueprint.route("/api/v1/database_schema_llm_context", methods=["GET"])
def api_database_schema_llm_context():
    """
    Return a compact database schema relationship digest for LLM analysis.

    The digest includes PK/FK/UNIQUE information, FK index coverage, and
    table-level pg_stat / pg_statio statistics for user tables.

    Example:
      /api/v1/database_schema_llm_context
    """
    conn = None

    try:
        if not session.get("db_host") and not session.get("db_uri"):
            return jsonify({
                "success": False,
                "error": "No database connection found in session."
            }), 400

        conn, status = database.connectdb(session)

        if conn is None or status != "OK":
            return jsonify({
                "success": False,
                "error": status or "Unable to connect to database."
            }), 500

        result = schema_helper.get_database_schema_llm_context(conn)
        return jsonify(result), 200

    except Exception as exc:
        return jsonify({
            "success": False,
            "error": str(exc)
        }), 500

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
