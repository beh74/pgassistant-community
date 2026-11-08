from pathlib import Path
import unittest
from flask import Flask, render_template
from apps.home.executive_plan import build_plan_from_results


class ExecutivePlanTemplateTests(unittest.TestCase):
    def render(self, plan, history=False):
        app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'apps/templates'))
        app.config.update(SECRET_KEY='test', ASSETS_ROOT='/static/assets')
        app.add_url_rule('/report', endpoint='home_blueprint.executive_plan_report_route', view_func=lambda: '')
        app.add_url_rule('/history', endpoint='home_blueprint.api_executive_plan_history', view_func=lambda: '')
        with app.test_request_context():
            return render_template('home/executive_plan.html', plan=plan, collector_history_enabled=history)

    def test_empty_plan_keeps_context_report_and_history(self):
        plan = build_plan_from_results({}, 'example')
        html = self.render(plan, history=True)
        self.assertIn('No implementation work identified', html)
        self.assertIn('No actionable recommendation', html)
        self.assertIn('Decision context', html)
        self.assertIn('Work packages by team', html)
        self.assertIn('id="executive-history-tab"', html)
        self.assertIn('id="executive-report-form"', html)

    def test_loading_container_is_preserved_for_async_generation(self):
        html = self.render(None)
        self.assertIn('data-loading="true"', html)
        self.assertIn('Building your Executive Plan', html)
        self.assertNotIn('id="executive-sequence-title"', html)
