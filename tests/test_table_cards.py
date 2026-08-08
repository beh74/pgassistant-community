import json
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class TableCardsQueryTest(unittest.TestCase):
    def test_table_size_query_collects_fillfactor_and_hot_update_metrics(self):
        queries = json.loads((REPO_ROOT / "queries.json").read_text(encoding="utf-8"))
        table_size = next(query for query in queries["sql"] if query.get("id") == "table_size")
        sql = table_size["sql"]

        self.assertIn("pg_options_to_table(member.reloptions)", sql)
        self.assertIn("st.n_tup_upd", sql)
        self.assertIn("st.n_tup_hot_upd", sql)
        self.assertIn("AS non_hot_updates", sql)
        self.assertIn("AS hot_update_pct", sql)
        self.assertIn("AS fillfactor", sql)
        self.assertIn("dbs.stats_reset", sql)
        self.assertIn("WHEN n_dead_tup = 0 THEN 'NO_BLOAT'", sql)


class TableCardsTemplateTest(unittest.TestCase):
    def setUp(self):
        self.template = (REPO_ROOT / "apps" / "templates" / "home" / "tables_cards.html").read_text(
            encoding="utf-8"
        )

    def test_displays_fillfactor_and_hot_update_metrics(self):
        self.assertIn("Fillfactor", self.template)
        self.assertIn("HOT / non-HOT", self.template)
        self.assertIn("data-hot-pct", self.template)
        self.assertIn('sort === "hot_pct"', self.template)
        self.assertIn("Fillfactor advisor", self.template)
        self.assertIn("{% if updates|int > 0 %}", self.template)
        self.assertIn("Candidate for a controlled fillfactor experiment", self.template)
        self.assertIn("Investigate other causes", self.template)
        self.assertNotIn('class="table-update-stats"', self.template)
        self.assertNotIn("No update activity", self.template)
        self.assertIn('id="fillfactorAdvisorModal"', self.template)
        self.assertIn("Details &amp; SQL", self.template)
        self.assertIn("fillfactor-modal-metrics", self.template)
        self.assertIn("Rows updated since PostgreSQL statistics were last reset", self.template)
        self.assertIn("HOT updates avoid new index entries", self.template)
        self.assertIn("Share of all updates that were HOT", self.template)
        self.assertIn("Check it", self.template)
        self.assertIn("/api/v1/fillfactor_checks/", self.template)
        self.assertIn("Indexed-column updates", self.template)
        self.assertIn("ALTER TABLE", self.template)
        self.assertIn("SET (fillfactor =", self.template)
        self.assertIn('classList.toggle("d-none", adviceCase === "no_activity")', self.template)
        self.assertIn('value="NO_BLOAT">No bloat', self.template)
        self.assertIn("table-status-no-bloat", self.template)
        self.assertIn(
            "/topqueries.html?schema={{ schema|urlencode }}&amp;tablename={{ table|urlencode }}",
            self.template,
        )
        self.assertNotIn("/relative_queries.html?", self.template)

    def test_handles_mixed_partition_fillfactors(self):
        self.assertIn("Mixed fillfactor", self.template)
        self.assertIn('"Mixed " + fillfactorMin + "–" + fillfactorMax', self.template)
        self.assertIn("Leaf partitions use fillfactors from", self.template)


if __name__ == "__main__":
    unittest.main()
