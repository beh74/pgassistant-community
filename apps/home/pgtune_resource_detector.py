"""Detect the resources available to the remote PostgreSQL server process."""

import math
import re
from typing import Callable, Optional


FileReader = Callable[[str], Optional[str]]


def _parse_memtotal(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    match = re.search(r"^MemTotal:\s+(\d+)\s+kB$", value, re.MULTILINE)
    return int(match.group(1)) * 1024 if match else None


def _parse_cpuset(value: Optional[str]) -> Optional[int]:
    if not value or not value.strip():
        return None
    cpus = set()
    try:
        for part in value.strip().split(","):
            bounds = part.split("-", 1)
            if len(bounds) == 1:
                cpus.add(int(bounds[0]))
            else:
                cpus.update(range(int(bounds[0]), int(bounds[1]) + 1))
    except ValueError:
        return None
    return len(cpus) or None


def _positive_int(value: Optional[str]) -> Optional[int]:
    try:
        parsed = int((value or "").strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _cgroup_v2_directories(cgroup_text: Optional[str]) -> list[str]:
    relative = ""
    for line in (cgroup_text or "").splitlines():
        fields = line.split(":", 2)
        if len(fields) == 3 and fields[0] == "0" and fields[1] == "":
            relative = fields[2].strip("/")
            break

    directories = []
    parts = relative.split("/") if relative else []
    for length in range(len(parts), -1, -1):
        suffix = "/".join(parts[:length])
        path = "/sys/fs/cgroup"
        if suffix:
            path += "/" + suffix
        if path not in directories:
            directories.append(path)
    return directories


def _cgroup_v1_directories(cgroup_text: Optional[str], controller: str) -> list[str]:
    directories = []
    for line in (cgroup_text or "").splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3 or controller not in fields[1].split(","):
            continue
        relative = fields[2].strip("/")
        controllers = [fields[1], controller]
        for controller_dir in controllers:
            path = f"/sys/fs/cgroup/{controller_dir}"
            if relative:
                path += "/" + relative
            if path not in directories:
                directories.append(path)
    return directories


def _finite_memory_limit(value: Optional[str]) -> Optional[int]:
    if not value or value.strip() == "max":
        return None
    return _positive_int(value)


def _cpu_quota_v2(value: Optional[str]) -> Optional[int]:
    fields = (value or "").split()
    if len(fields) != 2 or fields[0] == "max":
        return None
    quota = _positive_int(fields[0])
    period = _positive_int(fields[1])
    if not quota or not period:
        return None
    return max(1, math.ceil(quota / period))


def _detect_environment(file_reader: FileReader, cgroup_text: str, limited: bool) -> str:
    dockerenv = file_reader("/.dockerenv")
    containerenv = file_reader("/run/.containerenv")
    evidence = "\n".join(
        filter(None, [cgroup_text, file_reader("/proc/1/cgroup") or ""])
    ).lower()

    if "kubepods" in evidence or "kube" in evidence:
        return "kubernetes"
    if dockerenv is not None or "docker" in evidence:
        return "docker"
    if containerenv is not None or "libpod" in evidence or "podman" in evidence:
        return "podman"
    if "lxc" in evidence:
        return "lxc"
    if limited or any(token in evidence for token in ("containerd", "machine.slice")):
        return "cgroup"
    virtualization = " ".join(filter(None, [
        file_reader("/sys/class/dmi/id/product_name") or "",
        file_reader("/sys/class/dmi/id/sys_vendor") or "",
    ])).lower()
    if any(token in virtualization for token in (
        "amazon ec2", "bhyve", "google compute", "kvm", "openstack",
        "parallels", "qemu", "virtualbox", "vmware", "xen",
    )):
        return "virtual-machine"
    return "host"


def detect_resources(file_reader: FileReader) -> dict:
    """Return CPU, memory in MiB, and environment for a PostgreSQL process."""
    mem_total = _parse_memtotal(file_reader("/proc/meminfo"))
    cpu_total = len(
        re.findall(r"^processor\s*:", file_reader("/proc/cpuinfo") or "", re.MULTILINE)
    ) or None
    if mem_total is None or cpu_total is None:
        raise RuntimeError("Unable to read host CPU or memory information from PostgreSQL.")

    cgroup_text = file_reader("/proc/self/cgroup") or ""
    memory_limits = []
    cpu_limits = []
    cpuset_limits = []

    for directory in _cgroup_v2_directories(cgroup_text):
        memory_limit = _finite_memory_limit(file_reader(f"{directory}/memory.max"))
        if memory_limit:
            memory_limits.append(memory_limit)
        cpu_limit = _cpu_quota_v2(file_reader(f"{directory}/cpu.max"))
        if cpu_limit:
            cpu_limits.append(cpu_limit)
        cpuset_limit = _parse_cpuset(file_reader(f"{directory}/cpuset.cpus.effective"))
        if cpuset_limit:
            cpuset_limits.append(cpuset_limit)

    # Common cgroup v1 paths. These also cover containers where the cgroup
    # namespace exposes the process cgroup as the controller root.
    memory_v1_dirs = _cgroup_v1_directories(cgroup_text, "memory")
    memory_v1_dirs.extend(("/sys/fs/cgroup/memory", "/sys/fs/cgroup"))
    for base in memory_v1_dirs:
        limit = _finite_memory_limit(file_reader(f"{base}/memory.limit_in_bytes"))
        if limit and limit < (1 << 60):
            memory_limits.append(limit)
    cpu_v1_dirs = _cgroup_v1_directories(cgroup_text, "cpu")
    cpu_v1_dirs.extend(("/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpu,cpuacct"))
    for base in cpu_v1_dirs:
        quota = _positive_int(file_reader(f"{base}/cpu.cfs_quota_us"))
        period = _positive_int(file_reader(f"{base}/cpu.cfs_period_us"))
        if quota and period:
            cpu_limits.append(max(1, math.ceil(quota / period)))
    cpuset_v1_dirs = _cgroup_v1_directories(cgroup_text, "cpuset")
    cpuset_v1_dirs.extend(("/sys/fs/cgroup/cpuset", "/sys/fs/cgroup"))
    for base in cpuset_v1_dirs:
        count = _parse_cpuset(file_reader(f"{base}/cpuset.cpus"))
        if count:
            cpuset_limits.append(count)

    effective_memory = min([mem_total] + memory_limits)
    effective_cpu = min([cpu_total] + cpu_limits + cpuset_limits)
    limited = effective_memory < mem_total or effective_cpu < cpu_total

    return {
        "cpu": effective_cpu,
        "memory_mb": max(1, effective_memory // (1024 * 1024)),
        "environment": _detect_environment(file_reader, cgroup_text, limited),
    }


def detect_postgresql_resources(connection) -> dict:
    """Read Linux resource files through PostgreSQL's pg_read_file()."""
    permission_denied = False

    def read_file(path: str) -> Optional[str]:
        nonlocal permission_denied
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT pg_read_file(%s, 0, 1048576, true)", (path,))
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as exc:  # psycopg errors vary across supported versions
            connection.rollback()
            if "permission denied" in str(exc).lower() or "must be superuser" in str(exc).lower():
                permission_denied = True
            return None
        finally:
            cursor.close()

    try:
        return detect_resources(read_file)
    except RuntimeError as exc:
        if permission_denied:
            raise RuntimeError(
                "The connected PostgreSQL user may not have the permissions "
                "required to detect server resources."
            ) from exc
        raise
