import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


class _Connection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _Recommendation:
    def to_dict(self):
        return {
            "recommendation_id": "test_recommendation",
            "label": "Test recommendation",
            "rank": 80,
            "priority": "HIGH",
        }


def _load_reporting_module(rendered_templates):
    repo_root = Path(__file__).resolve().parents[1]
    module_name = "apps.home.reporting"

    apps_module = types.ModuleType("apps")
    home_module = types.ModuleType("apps.home")
    home_module.__path__ = []

    connection = _Connection()
    database_module = types.ModuleType("apps.home.database")
    database_module.connectdb = lambda _config: (connection, "OK")
    database_module.db_query = lambda _connection, query_id: (
        [{"query_id": query_id}],
        None,
    )
    database_module.get_query_by_id_reporing = lambda query_id: (
        f"SELECT '{query_id}'"
    )

    api_helper_module = types.ModuleType("apps.home.api_helper")
    api_helper_module.get_rank_top_10_queries = lambda _config: [
        {
            "queryid": "42",
            "query": "SELECT * FROM orders",
            "priority_score": 82.5,
            "priority_level": "Critical",
            "reason": "High total load",
        }
    ]

    global_module = types.ModuleType("apps.home.global_advisor")
    global_module.run_global_advisor = lambda _config, yaml_path: {
        "status": "ok",
        "recommendations": [_Recommendation() for _index in range(25)],
        "summary": {"total": 25},
        "errors": [],
        "yaml_path": yaml_path,
    }

    index_module = types.ModuleType("apps.home.query_index_advisor")
    index_module.analyze_top_ranked_query_indexes = lambda _config, limit: {
        "success": True,
        "supported": True,
        "query_limit": limit,
        "summary": {"actionable_recommendations": 0},
        "results": [],
    }

    flask_module = types.ModuleType("flask")

    def fake_render_template(template, **context):
        rendered_templates.append((template, context))
        return f"[{template}]"

    flask_module.render_template = fake_render_template

    modules = {
        "apps": apps_module,
        "apps.home": home_module,
        "apps.home.api_helper": api_helper_module,
        "apps.home.database": database_module,
        "apps.home.global_advisor": global_module,
        "apps.home.query_index_advisor": index_module,
        "flask": flask_module,
    }
    previous = {name: sys.modules.get(name) for name in modules}
    sys.modules.update(modules)

    spec = importlib.util.spec_from_file_location(
        module_name,
        repo_root / "apps" / "home" / "reporting.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module, connection, previous


class DatabaseReportOrchestrationTest(unittest.TestCase):
    def test_query_sections_are_followed_by_ranking_and_both_advisors(self):
        rendered_templates = []
        reporting, connection, previous = _load_reporting_module(
            rendered_templates
        )
        definition = """
- chapter_name: Database profile
  query_id: reporting_db_profile
  description: Profile
  enabled: true
  template: generic_select.md
- chapter_name: Top 10 Query Ranking
  source: query_ranking
  description: Ranked workload
  enabled: true
  template: query_ranking.md
  limit: 10
- chapter_name: Global Advisor
  source: global_advisor
  description: Global findings
  enabled: true
  template: global_advisor.md
  advisor_yaml_path: advisor_enriched.yml
  limit: 20
- chapter_name: Index Advisor
  source: index_advisor
  description: Index findings
  enabled: true
  template: index_advisor.md
  limit: 12
"""

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".yml",
                encoding="utf-8",
            ) as report_file:
                report_file.write(definition)
                report_file.flush()
                result = reporting.get_database_report(
                    {
                        "db_host": "localhost",
                        "db_port": 5432,
                        "db_name": "postgres",
                        "db_user": "postgres",
                    },
                    report_yaml_definition_file=report_file.name,
                )

            self.assertIn("[db_report_templates/main.md]", result)
            self.assertIn("[db_report_templates/generic_select.md]", result)
            self.assertIn("[db_report_templates/query_ranking.md]", result)
            self.assertIn("[db_report_templates/global_advisor.md]", result)
            self.assertIn("[db_report_templates/index_advisor.md]", result)
            self.assertTrue(connection.closed)

            templates = [template for template, _context in rendered_templates]
            self.assertEqual(
                templates,
                [
                    "db_report_templates/main.md",
                    "db_report_templates/generic_select.md",
                    "db_report_templates/query_ranking.md",
                    "db_report_templates/global_advisor.md",
                    "db_report_templates/index_advisor.md",
                ],
            )

            ranking_context = rendered_templates[2][1]
            self.assertEqual(ranking_context["ranked_queries"][0]["queryid"], "42")
            self.assertEqual(ranking_context["query_limit"], 10)

            global_context = rendered_templates[3][1]
            self.assertEqual(
                global_context["recommendations"][0]["recommendation_id"],
                "test_recommendation",
            )
            self.assertEqual(len(global_context["recommendations"]), 20)
            self.assertEqual(global_context["recommendations_available"], 25)
            self.assertEqual(global_context["recommendation_limit"], 20)
            index_context = rendered_templates[4][1]
            self.assertEqual(index_context["result"]["query_limit"], 12)
        finally:
            for name, old_module in previous.items():
                if old_module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = old_module

    def test_query_source_requires_query_id(self):
        rendered_templates = []
        reporting, _connection, previous = _load_reporting_module(
            rendered_templates
        )
        try:
            with self.assertRaisesRegex(ValueError, "missing query_id"):
                reporting._validate_report_entry(
                    {
                        "chapter_name": "Broken",
                        "description": "Broken query section",
                        "enabled": True,
                        "template": "generic_select.md",
                    },
                    1,
                )
        finally:
            for name, old_module in previous.items():
                if old_module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = old_module


class ReportTemplateTest(unittest.TestCase):
    def setUp(self):
        repo_root = Path(__file__).resolve().parents[1]
        self.environment = Environment(
            loader=FileSystemLoader(repo_root / "apps" / "templates")
        )

    def test_new_report_templates_parse(self):
        for template in (
            "db_report_templates/query_ranking.md",
            "db_report_templates/global_advisor.md",
            "db_report_templates/index_advisor.md",
            "db_report_templates/report_error.md",
        ):
            self.environment.get_template(template)

    def test_query_ranking_template_renders_metrics(self):
        template = self.environment.get_template(
            "db_report_templates/query_ranking.md"
        )
        rendered = template.render(
            chapter_name="Top 10 Query Ranking",
            ranked_queries=[
                {
                    "queryid": "42",
                    "query": "SELECT * FROM orders",
                    "priority_score": 82.5,
                    "priority_level": "Critical",
                    "reason": "High total load",
                    "total_exec_time_formatted": "12 s 50 ms",
                    "mean_exec_time": 25.5,
                    "calls": 500,
                    "share_total_time": 30.25,
                    "share_calls": 12.5,
                    "share_io": 18.0,
                    "cache_hit_ratio": 91.2,
                    "rows_per_call": 3.0,
                    "temp_blks_written": 12,
                    "signals": ["high_load", "poor_cache"],
                }
            ],
        )
        self.assertIn("SELECT * FROM orders", rendered)
        self.assertIn("82.5", rendered)
        self.assertIn("30.25%", rendered)

    def test_unsupported_index_advisor_renders_explanation(self):
        template = self.environment.get_template(
            "db_report_templates/index_advisor.md"
        )
        rendered = template.render(
            chapter_name="Index Advisor",
            result={
                "success": True,
                "supported": False,
                "message": "Generic plans require PostgreSQL 16 or newer.",
                "postgres_major_version": 15,
                "required_version": 16,
            },
            summary={},
            query_results=[],
        )
        self.assertIn("require PostgreSQL 16 or newer", rendered)

    def test_advisor_templates_render_recommendations(self):
        global_template = self.environment.get_template(
            "db_report_templates/global_advisor.md"
        )
        global_rendered = global_template.render(
            chapter_name="Global Advisor",
            result={"status": "ok"},
            summary={
                "total": 1,
                "advisor_message": "One finding.",
                "priority_counts": {"HIGH": 1},
                "execution": {"checks_failed": 0},
            },
            errors=[],
            recommendations=[
                {
                    "label": "Long transaction",
                    "recommendation_id": "long_running_transactions",
                    "rank": 92,
                    "priority": "HIGH",
                    "team": "OPS",
                    "risk_level": "HIGH",
                    "category_id": "MAINTENANCE",
                    "object_name": "postgres / app / pid 123",
                    "action_type": "REVIEW_ONLY",
                    "action_safety": "MANUAL_ONLY",
                    "recommendation_note": "Transaction is idle.",
                    "improvement_sql": "SELECT pg_terminate_backend(123);",
                }
            ],
        )
        self.assertIn("Long transaction", global_rendered)
        self.assertIn("pg_terminate_backend", global_rendered)

        index_template = self.environment.get_template(
            "db_report_templates/index_advisor.md"
        )
        index_rendered = index_template.render(
            chapter_name="Index Advisor",
            result={
                "success": True,
                "supported": True,
                "postgres_major_version": 16,
                "query_limit": 10,
            },
            summary={"actionable_recommendations": 1},
            query_results=[
                {
                    "queryid": "1234567890123456789",
                    "query": "SELECT * FROM orders WHERE customer_id = $1",
                    "actionable_recommendations": [
                        {
                            "schema": "public",
                            "table": "orders",
                            "confidence": "safe",
                            "recommendation_type": "filter_index",
                            "candidate_columns": ["customer_id"],
                            "reason": "Selective predicate.",
                            "create_index_sql": (
                                "CREATE INDEX CONCURRENTLY ON "
                                "public.orders (customer_id);"
                            ),
                        }
                    ],
                }
            ],
        )
        self.assertIn("1234567890123456789", index_rendered)
        self.assertIn("CREATE INDEX CONCURRENTLY", index_rendered)


if __name__ == "__main__":
    unittest.main()
