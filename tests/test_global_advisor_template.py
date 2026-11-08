from pathlib import Path
import re
import unittest

from flask import Flask, render_template
from apps.home.global_advisor_models import GlobalRecommendation, RecommendationCategory, AdvisorTeam


class GlobalAdvisorTemplateTests(unittest.TestCase):
    def render(self, recommendations):
        app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'apps/templates'))
        app.config.update(SECRET_KEY='test', ASSETS_ROOT='/static/assets')
        with app.test_request_context():
            return render_template('home/advisor_summary_tabs.html', recommendations=recommendations, errors=[])

    def test_grouping_priority_order_and_unique_evidence_ids(self):
        def rec(identifier, rank, table):
            return GlobalRecommendation(rank=rank, category_id=next(iter(RecommendationCategory)),
                source='test', recommendation_id=identifier, label=identifier, table_name=table,
                team=AdvisorTeam.DEV, recommendation_note='Review <script>bad</script>',
                improvement_sql='SELECT 1;', estimated_rows=10)
        html = self.render([rec('low', 30, 'a'), rec('high', 90, 'b'), rec('high', 85, 'c')])
        self.assertEqual(html.count('class="ga-finding-group"'), 2)
        self.assertLess(html.index('class="ga-review-label">high'), html.index('class="ga-review-label">low'))
        self.assertIn('class="text-end ga-group-count">2', html)
        self.assertEqual(html.count('<details class="ga-finding-group">'), 2)
        self.assertEqual(html.count('<summary class="ga-group-summary">'), 2)
        self.assertNotIn('>Inspect</button>', html)
        self.assertNotIn('class="collapse" id="gaEvidence', html)
        ids = re.findall(r'\sid="([^"]+)"', html)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertIn('&lt;script&gt;bad&lt;/script&gt;', html)
        self.assertIn('SELECT 1;', html)
        self.assertIn('data-priority="HIGH"', html)

    def test_empty_analysis_keeps_filters_and_export(self):
        html = self.render([])
        self.assertIn('No recommendation matches', html)
        self.assertIn('id="gaTeamFilter"', html)
        self.assertIn('id="gaAllSqlModal"', html)
        self.assertNotIn('class="ga-finding-group"', html)
