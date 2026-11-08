from pathlib import Path
import re
import unittest

from flask import Flask, render_template, session


def dashboard_fixture():
    return {
        "version": "17.6", "uptime": "12 days", "size": "24 GB", "size_bytes": 24 * 1024**3,
        "shared_buffers": "4 GB", "cache": 99.42,
        "connexions": 80, "max_connexions": "100", "conflicts": 0,
        "architecture": {"type": "Primary with replicas", "server_role": "Primary", "connected_replicas": 2, "wal_archiving": True},
        "profile": [
            {"query_type": "update", "total_calls": 10, "percentage_calls": 10, "total_exec_time": 9000, "percentage_exec_time": 90},
            {"query_type": "select", "total_calls": 90, "percentage_calls": 90, "total_exec_time": 1000, "percentage_exec_time": 10},
        ],
        "table_size": [{"schemaname": "public", "table_name": "orders", "size": "12 GB", "size_bytes": 12 * 1024**3, "row_count": 1000000}],
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
        calls = html.split('id="workload-calls"')[1].split('id="workload-time"')[0]
        time = html.split('id="workload-time"')[1].split('aria-labelledby="dashboard-largest-tables-title"')[0]
        self.assertLess(calls.index('>select</div>'), calls.index('>update</div>'))
        self.assertLess(time.index('>update</div>'), time.index('>select</div>'))
        self.assertNotIn('>#2 ', html)
        self.assertIn('schema=public&amp;tablename=orders', html)
        self.assertIn('80% of configured limit', html)

    def test_numeric_strings_sort_correctly_in_cards_and_details(self):
        info = dashboard_fixture()
        info['profile'] = [
            dict(query_type=name, total_calls=calls, total_exec_time=time,
                 percentage_calls=0, percentage_exec_time=0)
            for name, calls, time in [
                ('select', '100', '9.5'), ('insert', '9', '100.1'),
                ('update', 80, 20), ('delete', '20', '80.2'), ('other', '3', '3'),
            ]
        ]
        html = self.render(info)
        labels = re.findall(r'class="dashboard-profile-label">(\w+)</div>', html)
        calls = ['select', 'update', 'delete', 'insert', 'other']
        time = ['insert', 'delete', 'update', 'select', 'other']
        self.assertEqual(labels, calls[:4] + time[:4] + calls + time)

    def test_workload_tabs_keep_details_available_for_small_profiles(self):
        html = self.render(dashboard_fixture())
        for metric, modal in [('calls', 'profileCallsModal'), ('time', 'profileTimeModal')]:
            self.assertIn(f'data-bs-target="#workload-{metric}"', html)
            self.assertIn(f'aria-labelledby="workload-{metric}-tab"', html)
            self.assertIn(f'data-bs-target="#{modal}"', html)
            self.assertIn(f'id="{modal}"', html)
        self.assertIn('Where to focus', html)

    def test_largest_tables_use_bytes_and_each_tables_own_database(self):
        info = dashboard_fixture()
        html = self.render(info)
        for label in ['Relation', 'Total size', 'Estimated rows', 'Share of database']:
            self.assertIn(label, html)
        self.assertIn('50.0%', html)
        info.update(multi_db=True, size_bytes=1000)
        info['table_size'][0].update(database='example', database_size_bytes=200,
                                     size_bytes=50)
        html = self.render(info)
        self.assertIn('25.0%', html)
        self.assertIn('&amp;db=example', html)
        info['table_size'][0]['size_bytes'] = 0.1
        self.assertIn('&lt;0.1%', self.render(info))
        info['table_size'][0]['database_size_bytes'] = 0
        self.assertNotIn('25.0%', self.render(info))

    def test_percentages_are_the_primary_sort_key(self):
        info = dashboard_fixture()
        # Deliberately conflicting totals distinguish percentage sorting from total sorting.
        info['profile'] = [
            dict(query_type=name, total_calls=total, total_exec_time=total,
                 percentage_calls=percentage, percentage_exec_time=percentage)
            for name, percentage, total in [
                ('transaction', '0', 31), ('select', '62', 10),
                ('other', '6', 2600), ('ddl', 0, 21),
                ('session', '4', 1900), ('maintenance', 27, 12300),
            ]
        ]
        html = self.render(info)
        labels = re.findall(r'class="dashboard-profile-label">(\w+)</div>', html)
        expected = ['select', 'maintenance', 'other', 'session', 'transaction', 'ddl']
        self.assertEqual(labels, expected[:4] * 2 + expected * 2)

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
