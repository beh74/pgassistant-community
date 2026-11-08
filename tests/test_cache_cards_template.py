from pathlib import Path
import unittest
from flask import Flask, render_template, session


class CacheCardsTemplateTests(unittest.TestCase):
    def render(self, rows):
        app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'apps/templates'))
        app.config.update(SECRET_KEY='test', ASSETS_ROOT='/static/assets', PGASSISTANT_VERSION='test')
        with app.test_request_context('/cache_table.html'):
            return render_template('home/cache_table.html', rows=rows,
                description={'description': 'Cache efficiency', 'sql': 'SELECT 1;', 'reference': ''})

    def test_zero_and_missing_ratios_remain_distinct_and_links_include_schema(self):
        html = self.render([{'schemaname': 'sales', 'table_name': 'orders',
                            'table_cache_hit_ratio': 0, 'index_cache_hit_ratio': None}])
        self.assertIn('0.00', html)
        self.assertIn('No statistics available', html)
        self.assertIn('schema=sales&amp;tablename=orders', html)
        self.assertIn('data-table="0"', html)
        self.assertIn('data-index=""', html)
        self.assertNotIn('new DataTable', html)

    def test_table_names_and_sql_are_escaped(self):
        html = self.render([{'schemaname': 'public', 'table_name': '<script>alert(1)</script>',
                            'table_cache_hit_ratio': 99.5, 'index_cache_hit_ratio': 97}])
        self.assertIn('&lt;script&gt;', html)
        self.assertNotIn('<script>alert(1)</script>', html)

    def test_empty_view_keeps_controls_and_query(self):
        html = self.render([])
        self.assertIn('id="cache-search"', html)
        self.assertIn('id="cache-sort"', html)
        self.assertIn('No table matches this view.', html)
        self.assertIn('SELECT 1;', html)

    def test_connection_panel_uses_consistent_values_without_redundant_status(self):
        app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'apps/templates'))
        app.secret_key = 'test'
        with app.test_request_context():
            session.update(db_host='host.example', db_connected=True, db_name='northwind', db_user='postgres', db_port=5432)
            html = render_template('includes/db_connection_panel.html', multi_db_filter=False)
        self.assertEqual(html.count('class="pga-sidebar-db-value"'), 3)
        self.assertNotIn('>Connected</span>', html)
