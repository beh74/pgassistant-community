import unittest

from apps.home.schema_helper import (
    _build_database_architecture,
    _build_llm_prompt,
    _detect_wal_archive_tool,
    _resolve_and_merge_table_workload,
)


class SchemaWorkloadTest(unittest.TestCase):
    def test_detects_common_wal_archive_tools_without_returning_the_command(self):
        self.assertEqual(
            _detect_wal_archive_tool("pgbackrest --stanza=main archive-push %p"),
            "pgBackRest",
        )
        self.assertEqual(
            _detect_wal_archive_tool("barman-cloud-wal-archive s3://bucket server %p"),
            "Barman",
        )
        self.assertEqual(_detect_wal_archive_tool("wal-g wal-push %p"), "WAL-G")

    def test_classifies_standalone_archived_and_replicated_architectures(self):
        standalone = _build_database_architecture({"archive_mode": "off"})
        archived = _build_database_architecture(
            {
                "archive_mode": "on",
                "archive_command": "pgbackrest archive-push %p",
            }
        )
        cluster = _build_database_architecture(
            {
                "archive_mode": "on",
                "is_in_recovery": True,
                "primary_conninfo": "host=primary password=secret",
                "restore_command": "wal-g wal-fetch %f %p",
            }
        )

        self.assertEqual(standalone["type"], "Standalone PostgreSQL")
        self.assertEqual(
            archived["type"], "Standalone PostgreSQL with WAL archiving"
        )
        self.assertEqual(archived["archive_tool"], "pgBackRest")
        self.assertEqual(cluster["type"], "Replicated PostgreSQL cluster")
        self.assertEqual(cluster["server_role"], "Standby")
        self.assertTrue(cluster["standby_source_configured"])
        self.assertNotIn("primary_conninfo", cluster)

    def test_llm_prompt_requests_an_evidence_based_database_usage_profile(self):
        prompt = _build_llm_prompt("## Top table workload from pg_stat_statements")

        self.assertIn("how the database appears to be used in practice", prompt)
        self.assertIn("read-heavy, write-heavy, or mixed", prompt)
        self.assertIn("frequent inexpensive operations", prompt)
        self.assertIn("structural centrality", prompt)
        self.assertIn("workload centrality", prompt)
        self.assertIn("query_count is the number of distinct captured statements", prompt)
        self.assertIn("Clearly label every inferred usage pattern as an inference", prompt)
        self.assertIn("What is the role of this database?", prompt)
        self.assertIn("before any heading", prompt)
        self.assertIn("If the available evidence is insufficient", prompt)
        self.assertIn("- Observed database usage", prompt)
        self.assertIn("- Database architecture", prompt)

    def test_resolves_and_merges_qualified_and_unqualified_tables_before_limit(self):
        workload = [
            {
                "table": "orders",
                "operations": [
                    {
                        "operation": "select",
                        "query_count": 2,
                        "calls": 500,
                        "rows": 1000,
                        "total_exec_time": 50,
                    }
                ],
            },
            {
                "table": "public.orders",
                "operations": [
                    {
                        "operation": "update",
                        "query_count": 1,
                        "calls": 20,
                        "rows": 20,
                        "total_exec_time": 40,
                    }
                ],
            },
            {
                "table": "public.orders_view",
                "operations": [
                    {
                        "operation": "select",
                        "query_count": 1,
                        "calls": 1000,
                        "rows": 1000,
                        "total_exec_time": 100,
                    }
                ],
            },
        ]

        result = _resolve_and_merge_table_workload(
            workload,
            {"public.orders": {}},
            limit=20,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table"], "public.orders")
        self.assertEqual(result[0]["query_count"], 3)
        self.assertEqual(result[0]["calls"], 520)
        self.assertEqual(
            [item["operation"] for item in result[0]["operations"]],
            ["select", "update"],
        )


if __name__ == "__main__":
    unittest.main()
