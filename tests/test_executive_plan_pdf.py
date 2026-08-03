import unittest

from apps.home.executive_plan_pdf import _wrap_sql, build_executive_plan_pdf, filter_plan_for_teams

def _sample_plan():
    dev_task = {
        "title": "Correct schema design issues",
        "team": "DEV",
        "workstream": "SCHEMA_DESIGN",
        "sources": ["global_advisor"],
        "scope_name": "public.orders",
        "recommendation_count": 1,
        "recommendations": [],
        "recommendation_groups": [{
            "scope_name": "public.orders",
            "recommendations": [{
                "title": "Add a primary key",
                "description": "The table has no primary key.",
                "sql": "ALTER TABLE public.orders ADD PRIMARY KEY (id);",
            }],
        }],
    }
    shared_task = {
        "title": "Consolidate indexes",
        "team": "DEV_OPS",
        "workstream": "INDEX_STRATEGY",
        "sources": ["global_advisor", "index_advisor"],
        "scope_name": "public.orders",
        "recommendation_count": 1,
        "recommendations": [],
        "recommendation_groups": [{
            "scope_name": "public.orders",
            "recommendations": [{"title": "Create an index", "sql": "CREATE INDEX CONCURRENTLY IF NOT EXISTS pga_idx_fk_order_details_fk_order_details_products ON public.order_details (product_id, order_id, unit_price, quantity);"}],
        }],
    }
    ops_task = {
        "title": "Tune PostgreSQL parameters",
        "team": "OPS",
        "workstream": "CONFIGURATION",
        "sources": ["parameter_advisor"],
        "scope_name": "work_mem",
        "recommendation_count": 1,
        "recommendations": [{"title": "Review work_mem", "description": "Temporary files were observed."}],
        "recommendation_groups": [{"scope_name": "work_mem", "recommendations": [{"title": "Review work_mem"}]}],
    }
    return {
        "database": "application",
        "phases": [
            {"number": 20, "name": "Correct schema design", "rationale": "Fix structural issues.", "team": "DEV", "tasks": [dev_task]},
            {"number": 40, "name": "Consolidate indexes", "rationale": "Review index changes.", "team": "DEV_OPS", "tasks": [shared_task]},
            {"number": 60, "name": "Tune parameters", "rationale": "Apply configuration changes.", "team": "OPS", "requires_maintenance_window": True, "requires_restart": True, "tasks": [ops_task]},
        ],
        "tasks": [dev_task, shared_task, ops_task],
    }


class ExecutivePlanPdfTests(unittest.TestCase):
    def test_team_filter_includes_shared_tasks(self):
        dev_plan = filter_plan_for_teams(_sample_plan(), ["DEV"])
        self.assertEqual([task["team"] for task in dev_plan["tasks"]], ["DEV", "DEV_OPS"])

        ops_plan = filter_plan_for_teams(_sample_plan(), ["OPS"])
        self.assertEqual([task["team"] for task in ops_plan["tasks"]], ["DEV_OPS", "OPS"])

    def test_pdf_is_generated_for_filtered_audience(self):
        pdf = build_executive_plan_pdf(_sample_plan(), ["DEV"])

        self.assertTrue(pdf.getvalue().startswith(b"%PDF"))
        self.assertGreater(len(pdf.getvalue()), 5_000)

    def test_requires_at_least_one_team(self):
        with self.assertRaises(ValueError):
            filter_plan_for_teams(_sample_plan(), [])

    def test_long_sql_is_wrapped_without_losing_content(self):
        sql = "CREATE INDEX CONCURRENTLY IF NOT EXISTS a_very_long_index_name ON public.order_details (product_id, order_id);"
        wrapped = _wrap_sql(sql, width=48)

        self.assertIn("\n", wrapped)
        self.assertEqual(" ".join(wrapped.split()), sql)


if __name__ == "__main__":
    unittest.main()
