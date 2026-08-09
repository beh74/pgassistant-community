import importlib.util
import io
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError


MODULE_PATH = Path(__file__).resolve().parents[1] / "apps" / "home" / "pg_version.py"
SPEC = importlib.util.spec_from_file_location("pg_version_cache_test_module", MODULE_PATH)
pg_version = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = pg_version
SPEC.loader.exec_module(pg_version)


def _payload(latest_minor="5"):
    return [
        {
            "major": 17,
            "latestMinor": latest_minor,
            "supported": True,
            "relDate": "2026-01-01",
        }
    ]


class PostgreSQLVersionFileCacheTest(unittest.TestCase):
    def test_repeated_callers_share_the_file_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "versions.json"
            response = io.BytesIO(json.dumps(_payload()).encode("utf-8"))

            with patch.object(
                pg_version,
                "DEFAULT_CACHE_PATH",
                cache_path,
            ), patch.object(
                pg_version,
                "urlopen",
                return_value=response,
            ) as mocked_urlopen:
                first = pg_version.get_postgresql_upgrade_recommendation("17.4")
                second = pg_version.get_postgresql_upgrade_recommendation("17.4")

            self.assertEqual(first.latest_minor_version, "17.5")
            self.assertEqual(second.latest_minor_version, "17.5")
            mocked_urlopen.assert_called_once()

    def test_uses_a_file_cache_younger_than_30_days(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "versions.json"
            cache_path.write_text(json.dumps(_payload()), encoding="utf-8")

            with patch.object(pg_version, "urlopen") as mocked_urlopen:
                result = pg_version._fetch_postgresql_versions(cache_path=cache_path)

            self.assertEqual(result, _payload())
            mocked_urlopen.assert_not_called()

    def test_refreshes_a_file_cache_after_30_days(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "versions.json"
            cache_path.write_text(json.dumps(_payload("4")), encoding="utf-8")
            stale_time = time.time() - pg_version.CACHE_TTL_SECONDS - 1
            os.utime(cache_path, (stale_time, stale_time))
            response = io.BytesIO(json.dumps(_payload("5")).encode("utf-8"))

            with patch.object(pg_version, "urlopen", return_value=response):
                result = pg_version._fetch_postgresql_versions(cache_path=cache_path)

            self.assertEqual(result, _payload("5"))
            self.assertEqual(
                json.loads(cache_path.read_text(encoding="utf-8")),
                _payload("5"),
            )

    def test_uses_a_stale_cache_when_refresh_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "versions.json"
            cache_path.write_text(json.dumps(_payload("4")), encoding="utf-8")
            stale_time = time.time() - pg_version.CACHE_TTL_SECONDS - 1
            os.utime(cache_path, (stale_time, stale_time))

            with patch.object(pg_version, "urlopen", side_effect=URLError("offline")):
                result = pg_version._fetch_postgresql_versions(cache_path=cache_path)

            self.assertEqual(result, _payload("4"))


if __name__ == "__main__":
    unittest.main()
