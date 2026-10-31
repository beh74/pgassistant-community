import unittest
from unittest.mock import patch, MagicMock

from flask import Flask
from apps.home import route_api, query_parameter_advisor


class PgTuneV1DataSizeTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = 'test'
        self.app.register_blueprint(route_api.blueprint)
        self.client = self.app.test_client()
        self.config = {'db_name': 'test'}

    def test_default_and_explicit_values(self):
        for value in (None, 'less_ram', 'mid_ram', 'greater_ram'):
            payload = {'db_config': self.config}
            if value is not None:
                payload['data_size_class'] = value
            with self.subTest(value=value), patch.object(
                route_api.query_parameter_advisor, 'analyze_query_parameter_workload',
                return_value={'success': True},
            ) as analyze:
                response = self.client.post('/api/v1/query_parameter_advisor', json=payload)
                self.assertEqual(response.status_code, 200)
                analyze.assert_called_once_with(self.config, data_size_class=value or 'mid_ram')

    def test_get_query_parameter_uses_session_connection(self):
        with self.client.session_transaction() as session:
            session.update(self.config)
        with patch.object(route_api.query_parameter_advisor, 'analyze_query_parameter_workload',
                          return_value={'success': True}) as analyze:
            response = self.client.get('/api/v1/query_parameter_advisor?data_size_class=less_ram')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(analyze.call_args.kwargs['data_size_class'], 'less_ram')

    def test_invalid_values_return_400_without_analysis(self):
        for value in ('invalid', '', [], {}, False, 5):
            with self.subTest(value=value), patch.object(
                route_api.query_parameter_advisor, 'analyze_query_parameter_workload',
            ) as analyze:
                response = self.client.post('/api/v1/query_parameter_advisor', json={
                    'db_config': self.config, 'data_size_class': value,
                })
                self.assertEqual(response.status_code, 400)
                analyze.assert_not_called()

    def test_baseline_passes_data_size_to_tuner(self):
        for value in ('less_ram', 'mid_ram', 'greater_ram'):
            with self.subTest(value=value), patch.object(
                query_parameter_advisor.pgtune_resource_detector, 'detect_postgresql_resources',
                return_value={'cpu': 2, 'memory_mb': 65536},
            ), patch.object(query_parameter_advisor.pgtune, 'pgTune') as tuner:
                query_parameter_advisor._run_pgtune_baseline(
                    MagicMock(), 18, {'max_connections': 200}, data_size_class=value,
                )
                tuner.assert_called_once_with('18', 2, '65536MB', 'ssd', 'web', 200,
                                              data_size_class=value)
