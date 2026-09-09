import subprocess
import unittest
from pathlib import Path


class PgTuneScriptTests(unittest.TestCase):
    def run_pgtune(self, cpu_count):
        root = Path(__file__).resolve().parents[1]
        return subprocess.run(
            [
                "bash", str(root / "pgtune.sh"),
                "-v", "19", "-u", str(cpu_count), "-m", "8GB",
                "-s", "ssd", "-t", "web", "-c", "100",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_postgresql_19_is_accepted(self):
        result = self.run_pgtune(4)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("max_connections == 100", result.stdout)
        self.assertIn("shared_buffers ==", result.stdout)

    def test_single_cpu_clamps_worker_settings(self):
        result = self.run_pgtune(1)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("max_worker_processes == 1", result.stdout)
        self.assertIn("max_parallel_workers == 1", result.stdout)
        self.assertIn("max_parallel_workers_per_gather == 0", result.stdout)
        self.assertIn("max_parallel_maintenance_workers == 0", result.stdout)

    def test_two_cpus_clamp_worker_settings(self):
        result = self.run_pgtune(2)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("max_worker_processes == 2", result.stdout)
        self.assertIn("max_parallel_workers == 2", result.stdout)
        self.assertIn("max_parallel_workers_per_gather == 1", result.stdout)
        self.assertIn("max_parallel_maintenance_workers == 1", result.stdout)


if __name__ == "__main__":
    unittest.main()
