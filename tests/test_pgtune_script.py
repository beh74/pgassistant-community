import subprocess
import unittest
from pathlib import Path


class PgTuneScriptTests(unittest.TestCase):
    def run_pgtune(self, cpu_count, memory="8GB"):
        root = Path(__file__).resolve().parents[1]
        return subprocess.run(
            [
                "bash", str(root / "pgtune.sh"),
                "-v", "19", "-u", str(cpu_count), "-m", memory,
                "-s", "ssd", "-t", "web", "-c", "100",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_memory_in_mb_accepts_container_and_large_host_sizes(self):
        for memory in ("512MB", "11674MB", "24576MB", "10238976MB"):
            with self.subTest(memory=memory):
                result = self.run_pgtune(2, memory)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("shared_buffers ==", result.stdout)

    def test_equivalent_units_produce_identical_settings(self):
        for mb, gb in (("16384MB", "16GB"), ("10238976MB", "9999GB")):
            with self.subTest(memory=mb):
                in_mb, in_gb = self.run_pgtune(2, mb), self.run_pgtune(2, gb)
                self.assertEqual(in_mb.returncode, 0, in_mb.stderr)
                self.assertEqual(in_gb.returncode, 0, in_gb.stderr)
                self.assertEqual(in_mb.stdout, in_gb.stdout)

    def test_memory_outside_supported_range_is_rejected(self):
        for memory in ("511MB", "10238977MB", "0GB", "10000GB"):
            with self.subTest(memory=memory):
                result = self.run_pgtune(2, memory)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("input error", result.stderr)

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
