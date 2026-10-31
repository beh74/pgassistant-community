import importlib.util
import subprocess
import unittest
from pathlib import Path


class PgTuneScriptTests(unittest.TestCase):
    def run_pgtune(self, cpu_count, memory="8GB", extra_args=()):
        root = Path(__file__).resolve().parents[1]
        return subprocess.run(
            [
                "bash", str(root / "pgtune.sh"),
                "-v", "19", "-u", str(cpu_count), "-m", memory,
                "-s", "ssd", "-t", "web", "-c", "100", *extra_args,
            ],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_reported_pg18_64gb_scenario(self):
        result = self.run_pgtune(2, "64GB", ("-v", "18", "-c", "200"))
        self.assertEqual(result.returncode, 0, result.stderr)
        settings = dict(line.split(" == ") for line in result.stdout.splitlines())
        for name, value in {
            "maintenance_work_mem": "4GB", "jit": "off",
            "wal_compression": "lz4", "autovacuum_work_mem": "1GB",
            "io_method": "io_uring", "maintenance_io_concurrency": "200",
        }.items():
            self.assertEqual(settings[name], value)
        self.assertIn("liburing", result.stderr)

    def test_modern_parameters_are_version_gated(self):
        for version in ("9.5", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19"):
            with self.subTest(version=version):
                result = self.run_pgtune(2, "64GB", ("-v", version))
                self.assertEqual(result.returncode, 0, result.stderr)
                settings = dict(line.split(" == ") for line in result.stdout.splitlines())
                major = int(version.split(".")[0])
                self.assertEqual("jit" in settings, 12 <= major < 19)
                self.assertEqual("io_method" in settings, major >= 18)
                self.assertEqual("maintenance_io_concurrency" in settings, major >= 13)
                self.assertEqual("vacuum_buffer_usage_limit" in settings, major == 16)
                if major >= 10:
                    self.assertEqual(settings['wal_compression'], 'lz4' if 15 <= major < 19 else 'on')
                else:
                    self.assertNotIn('wal_compression', settings)

    def test_maintenance_and_autovacuum_budgets(self):
        for memory, cpu, expected_maintenance, expected_workers, expected_cost in (
            ("8GB", 2, "512MB", None, None),
            ("64GB", 16, "4GB", "4", "267"),
            ("256GB", 32, "8GB", "5", "333"),
        ):
            with self.subTest(memory=memory, cpu=cpu):
                result = self.run_pgtune(cpu, memory)
                settings = dict(line.split(" == ") for line in result.stdout.splitlines())
                self.assertEqual(settings['maintenance_work_mem'], expected_maintenance)
                self.assertEqual(settings.get('autovacuum_max_workers'), expected_workers)
                self.assertEqual(settings.get('autovacuum_vacuum_cost_limit'), expected_cost)
                self.assertEqual(settings.get('autovacuum_work_mem'), None if memory == '8GB' else '1GB')

    def test_jit_preserves_analytical_profile_defaults(self):
        result = self.run_pgtune(2, "64GB", ("-v", "18", "-t", "dw"))
        settings = dict(line.split(" == ") for line in result.stdout.splitlines())
        self.assertNotIn('jit', settings)
        self.assertEqual(settings['maintenance_work_mem'], '8GB')

    def test_python_wrapper_passes_data_size_to_script(self):
        root = Path(__file__).resolve().parents[1]
        spec = importlib.util.spec_from_file_location("pgtune_wrapper", root / "apps/home/pgtune.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        tuner = module.pgTune("18", 4, "8GB", "hdd", "web", 100, "less_ram")
        settings = tuner.get_pg_tune()
        self.assertEqual(settings["random_page_cost"], "1.1")
        self.assertEqual(module.pgTune("18", 4, "8GB", "hdd", "web", 100).data_size_class,
                         "mid_ram")
        with self.assertRaises(ValueError):
            module.pgTune("18", 4, "8GB", "hdd", "web", 100, "invalid")

    def test_data_size_defaults_to_mid_ram(self):
        self.assertEqual(self.run_pgtune(4).stdout,
                         self.run_pgtune(4, extra_args=("-d", "mid_ram")).stdout)

    def test_data_size_adjusts_work_mem(self):
        def work_mem(size):
            output = self.run_pgtune(4, extra_args=("-d", size)).stdout
            value = dict(line.split(" == ") for line in output.splitlines())["work_mem"]
            units = {"kB": 1, "MB": 1024, "GB": 1024**2}
            return int(value[:-2]) * units[value[-2:]]
        baseline = work_mem("mid_ram")
        self.assertEqual(work_mem("less_ram"), baseline * 130 // 100)
        self.assertEqual(work_mem("greater_ram"), baseline * 90 // 100)

    def test_data_size_and_workload_adjust_random_page_cost(self):
        for storage, workload, size, expected in (
            ("hdd", "web", "less_ram", "1.1"),
            ("ssd", "dw", "mid_ram", "4"),
            ("san", "dw", "greater_ram", "4"),
            ("ssd", "dw", "less_ram", "1.1"),
            ("ssd", "web", "mid_ram", "1.1"),
            ("hdd", "web", "greater_ram", "4"),
        ):
            with self.subTest(storage=storage, workload=workload, size=size):
                result = self.run_pgtune(4, extra_args=("-s", storage, "-t", workload, "-d", size))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"random_page_cost == {expected}\n", result.stdout)

    def test_invalid_data_size_is_rejected(self):
        result = self.run_pgtune(4, extra_args=("-d", "unknown"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("input error", result.stderr)

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

    def test_huge_pages_threshold(self):
        for memory, expected in (("16383MB", "off"), ("16384MB", "try"), ("32GB", "try")):
            with self.subTest(memory=memory):
                result = self.run_pgtune(4, memory)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"huge_pages == {expected}", result.stdout)

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
