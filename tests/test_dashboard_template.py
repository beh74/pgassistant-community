from pathlib import Path
import unittest

from flask import Flask, render_template, session


def dashboard_fixture():
    return {
        "version": "17.6", "uptime": "12 days", "size": "24 GB",
        "shared_buffers": "4 GB", "cache": 99.42,
        "connexions": 80, "max_connexions": "100", "conflicts": 0,
        "architecture": {"type": "Primary with replicas", "server_role": "Primary", "connected_replicas": 2, "wal_archiving": True},
        "profile": [
            {"query_type": "update", "total_calls": 10, "percentage_calls": 10, "total_exec_time": 9000, "percentage_exec_time": 90},
            {"query_type": "select", "total_calls": 90, "percentage_calls": 90, "total_exec_time": 1000, "percentage_exec_time": 10},
        ],
        "table_size": [{"schemaname": "public", "table_name": "orders", "size": "12 GB", "row_count": 1000000}],
        "top_clients": [{"client_addr": "10.0.0.20", "total_connections": 20, "active_connections": 3}],
    }


class DashboardTemplateTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1]
        self.app = Flask(__name__, template_folder=str(root / "apps/templates"))
        self.app.config.update(SECRET_KEY="test", ASSETS_ROOT="/static/assets")

    def render(self, info):
        with self.app.test_request_context():
            session.update(db_name="example", db_host="localhost", db_connected=True)
            return render_template("home/dashboard.html", dbinfo=info, segment="dashboard.html")

    def test_calls_and_time_have_independent_rankings(self):
        html = self.render(dashboard_fixture())
        calls = html.split('<span>Workload profile by calls</span>')[1].split('<span>Workload profile by time</span>')[0]
        time = html.split('<span>Workload profile by time</span>')[1].split('<span>Largest tables</span>')[0]
        self.assertLess(calls.index('>select</div>'), calls.index('>update</div>'))
        self.assertLess(time.index('>update</div>'), time.index('>select</div>'))
        self.assertNotIn('>#2 ', html)
        self.assertIn('schema=public&amp;tablename=orders', html)
        self.assertIn('80% of configured limit', html)

    def test_missing_statistics_are_not_reported_as_zero(self):
        info = dashboard_fixture()
        info.update(cache=None, profile=[], table_size=[], top_clients=[], connexions=None, max_connexions=None)
        html = self.render(info)
        self.assertIn('No cache statistics available', html)
        self.assertIn('Usage unavailable', html)
        self.assertIn('No table size information available', html)
        self.assertIn('No client activity available', html)
        self.assertNotIn('Needs attention', html)
        info['cache'] = 0
        self.assertIn('0.00%', self.render(info))

    def test_cluster_context_and_search_controls(self):
        info = dashboard_fixture()
        info.update(multi_db=True, databases=[{"name": "example", "owner": "postgres", "encoding": "UTF8", "size_bytes": 1234, "size_pretty": "1234 bytes"}])
        html = self.render(info)
        self.assertIn('Cluster overview', html)
        self.assertIn('id="cluster-db-search"', html)
        self.assertIn('id="cluster-db-sort"', html)
        labels = ['PostgreSQL version', 'Cluster size', 'Cluster hit cache', 'Server connections']
        positions = [html.index('dashboard-kpi-label">' + label) for label in labels]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('Uptime: 12 days', html)
        self.assertIn('data-bs-target="#postgresVersionAdvisorModal"', html)
        self.assertNotIn('dashboardDevTotal', html)
        self.assertNotIn('Collected', html)


if __name__ == "__main__":
    unittest.main()
