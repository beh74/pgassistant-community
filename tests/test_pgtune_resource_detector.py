import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "apps" / "home" / "pgtune_resource_detector.py"
SPEC = importlib.util.spec_from_file_location("pgtune_resource_detector_test_module", MODULE_PATH)
pgtune_resource_detector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pgtune_resource_detector)
detect_resources = pgtune_resource_detector.detect_resources


def reader(files):
    return lambda path: files.get(path)


class ResourceDetectorTests(unittest.TestCase):
    def test_detects_host_resources_without_cgroup_limits(self):
        result = detect_resources(reader({
            "/proc/meminfo": "MemTotal:       8388608 kB\n",
            "/proc/cpuinfo": "processor : 0\nprocessor : 1\nprocessor : 2\nprocessor : 3\n",
            "/proc/self/cgroup": "0::/\n",
        }))

        self.assertEqual(result, {"cpu": 4, "memory_mb": 8192, "environment": "host"})

    def test_detects_docker_and_cgroup_v2_limits(self):
        result = detect_resources(reader({
            "/proc/meminfo": "MemTotal:       16777216 kB\n",
            "/proc/cpuinfo": "".join(f"processor : {i}\n" for i in range(8)),
            "/proc/self/cgroup": "0::/docker/abc\n",
            "/proc/1/cgroup": "0::/docker/abc\n",
            "/.dockerenv": "",
            "/sys/fs/cgroup/docker/abc/memory.max": str(4 * 1024**3),
            "/sys/fs/cgroup/docker/abc/cpu.max": "150000 100000",
            "/sys/fs/cgroup/docker/abc/cpuset.cpus.effective": "0-3",
        }))

        self.assertEqual(result, {"cpu": 2, "memory_mb": 4096, "environment": "docker"})

    def test_detects_kubernetes_from_cgroup_name(self):
        result = detect_resources(reader({
            "/proc/meminfo": "MemTotal:       4194304 kB\n",
            "/proc/cpuinfo": "processor : 0\nprocessor : 1\n",
            "/proc/self/cgroup": "0::/kubepods.slice/pod123\n",
        }))

        self.assertEqual(result["environment"], "kubernetes")

    def test_uses_cpuset_when_it_is_more_restrictive_than_quota(self):
        result = detect_resources(reader({
            "/proc/meminfo": "MemTotal:       4194304 kB\n",
            "/proc/cpuinfo": "".join(f"processor : {i}\n" for i in range(8)),
            "/proc/self/cgroup": "0::/service\n",
            "/sys/fs/cgroup/service/cpu.max": "600000 100000",
            "/sys/fs/cgroup/service/cpuset.cpus.effective": "0,2-3",
        }))

        self.assertEqual(result["cpu"], 3)
        self.assertEqual(result["environment"], "cgroup")

    def test_detects_a_virtual_machine(self):
        result = detect_resources(reader({
            "/proc/meminfo": "MemTotal:       4194304 kB\n",
            "/proc/cpuinfo": "processor : 0\nprocessor : 1\n",
            "/proc/self/cgroup": "0::/\n",
            "/sys/class/dmi/id/product_name": "VMware Virtual Platform\n",
        }))

        self.assertEqual(result["environment"], "virtual-machine")
