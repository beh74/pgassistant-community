import unittest

from apps.home.executive_plan import build_plan_from_results, load_rules


class ExecutivePlanTests(unittest.TestCase):
    def test_plan_orders_phases_and_deduplicates_index_actions(self):
        index_recommendation = {
            "schema": "public",
            "table": "orders",
            "confidence": "safe",
            "reason": "Frequent filtered scan.",
            "recommendation_type": "create_index",
            "create_index_sql": "CREATE INDEX ON public.orders (customer_id);",
        }
        results = {
            "global_advisor": {
                "recommendations": [
                    {
                        "recommendation_id": "missing_primary_key",
                        "category_id": "DESIGN",
                        "action_type": "ALTER_TABLE",
                        "team": "DEV",
                        "priority": "HIGH",
                        "impact": 90,
                        "confidence": 90,
                        "effort": 50,
                        "schema_name": "public",
                        "table_name": "orders",
                        "object_name": "public.orders",
                        "title": "Add a primary key",
                    }
                ]
            },
            "index_advisor": {
                "results": [
                    {"queryid": "101", "actionable_recommendations": [index_recommendation]},
                    {
                        "queryid": "202",
                        "actionable_recommendations": [
                            {
                                **index_recommendation,
                                "create_index_sql": 'CREATE INDEX CONCURRENTLY "another_name" ON "public"."orders" ("customer_id");',
                            }
                        ],
                    },
                ]
            },
            "parameter_advisor": {
                "recommendations": [
                    {
                        "parameter": "work_mem",
                        "confidence": "review",
                        "reason": "Temporary files were observed.",
                        "alter_system_sql": "ALTER SYSTEM SET work_mem = '64MB';",
                    }
                ]
            },
            "autovacuum": {
                "tables": [
                    {
                        "schema_name": "public",
                        "table_name": "events",
                        "object_name": "public.events",
                        "issue_type": "vacuum_pressure",
                        "priority": "HIGH",
                        "vacuum_urgency": 2.5,
                        "script_sql": "VACUUM (ANALYZE) public.events;",
                    }
                ],
                "global_tuning": {"recommendations": []},
            },
        }

        plan = build_plan_from_results(results, "application", policy=load_rules())

        self.assertEqual([phase["number"] for phase in plan["phases"]], [0, 20, 40, 60])
        self.assertEqual([phase["team"] for phase in plan["phases"]], ["OPS", "DEV", "DEV_OPS", "OPS"])
        self.assertFalse(plan["phases"][0]["requires_maintenance_window"])
        self.assertTrue(plan["phases"][3]["requires_maintenance_window"])
        self.assertTrue(plan["phases"][3]["requires_restart"])
        index_task = next(task for task in plan["tasks"] if task["workstream"] == "INDEX_STRATEGY")
        self.assertEqual(index_task["recommendation_count"], 1)
        self.assertEqual(index_task["sql_count"], 1)
        self.assertEqual(index_task["query_ids"], ["101", "202"])
        self.assertEqual(index_task["scope_name"], "public.orders")
        self.assertEqual(index_task["title"], "Consolidate index recommendations by table")
        design_task = next(task for task in plan["tasks"] if task["workstream"] == "SCHEMA_DESIGN")
        self.assertEqual(design_task["team"], "DEV")
        self.assertEqual(design_task["title"], "Correct schema design issues")
        self.assertEqual(design_task["recommendation_groups"][0]["scope_name"], "public.orders")

    def test_statistics_refresh_precedes_regular_autovacuum_tuning(self):
        results = {
            "autovacuum": {
                "tables": [
                    {
                        "schema_name": "public",
                        "table_name": "customers",
                        "object_name": "public.customers",
                        "issue_type": "never_analyzed",
                        "never_analyzed": True,
                        "priority": "HIGH",
                        "script_sql": "ANALYZE public.customers;",
                    },
                    {
                        "schema_name": "public",
                        "table_name": "orders",
                        "object_name": "public.orders",
                        "issue_type": "stale_vacuum",
                        "priority": "MEDIUM",
                        "script_sql": "VACUUM public.orders;",
                    },
                ],
                "global_tuning": {"recommendations": []},
            }
        }

        plan = build_plan_from_results(results, "application", policy=load_rules())

        self.assertEqual([phase["number"] for phase in plan["phases"]], [10, 50])

    def test_configuration_is_deduplicated_across_advisors(self):
        results = {
            "global_advisor": {
                "recommendations": [
                    {
                        "recommendation_id": "Important PostgreSQL settings disabled or suboptimal",
                        "category_id": "CONFIGURATION",
                        "action_type": "CONFIG_CHANGE",
                        "team": "OPS",
                        "object_name": "autovacuum",
                        "title": "Enable autovacuum",
                        "improvement_sql": "ALTER SYSTEM SET autovacuum = on; SELECT pg_reload_conf();",
                    }
                ]
            },
            "autovacuum": {
                "tables": [],
                "global_tuning": {
                    "load_level": "medium",
                    "recommendations": [
                        {
                            "parameter": "autovacuum",
                            "rationale": "Autovacuum must stay enabled.",
                            "sql": "ALTER SYSTEM SET autovacuum = on;",
                        }
                    ],
                },
            },
        }

        plan = build_plan_from_results(results, "application", policy=load_rules())

        self.assertEqual(plan["summary"]["recommendations_collected"], 2)
        self.assertEqual(plan["summary"]["recommendations_after_deduplication"], 1)
        self.assertEqual(plan["tasks"][0]["phase"], 0)
        self.assertEqual(plan["tasks"][0]["title"], "Enable autovacuum immediately")
        self.assertEqual(plan["tasks"][0]["recommendations"][0]["title"], "Enable autovacuum")

    def test_global_advisor_maintenance_window_is_exposed_on_phase(self):
        results = {
            "global_advisor": {
                "recommendations": [
                    {
                        "recommendation_id": "maintenance_action",
                        "category_id": "MAINTENANCE",
                        "action_type": "VACUUM",
                        "team": "OPS",
                        "title": "Perform maintenance",
                        "requires_maintenance_window": True,
                    }
                ]
            }
        }

        plan = build_plan_from_results(results, "application", policy=load_rules())

        self.assertTrue(plan["phases"][0]["requires_maintenance_window"])
        self.assertFalse(plan["phases"][0]["requires_restart"])


if __name__ == "__main__":
    unittest.main()
