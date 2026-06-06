from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


POSTGRESQL_VERSIONS_URL = "https://www.postgresql.org/versions.json"
CACHE_TTL_SECONDS = 24 * 60 * 60

_versions_cache: tuple[float, list[dict[str, Any]]] | None = None


@dataclass(frozen=True)
class PostgreSQLUpgradeRecommendation:
    installed_version: str
    major_version: str
    latest_minor_version: str
    latest_release_date: str | None
    supported: bool
    end_of_life_date: str | None
    upgrade_recommended: bool
    recommendation_level: str
    recommendation: str


def _parse_postgresql_version(
    version_string: str,
) -> tuple[str, str]:
    """
    Extract the PostgreSQL version and its major branch.

    Accepted examples:
        "14.8"
        "PostgreSQL 14.8"
        "14.8 (Ubuntu 14.8-1.pgdg22.04+1)"
        "9.6.24"
        "PostgreSQL 16.3 on x86_64-pc-linux-gnu"

    Returns:
        A tuple containing:
            - normalized installed version
            - PostgreSQL major branch
    """
    if not isinstance(version_string, str):
        raise TypeError(
            "version_string must be a string, "
            f"received {type(version_string).__name__}."
        )

    version_string = version_string.strip()

    if not version_string:
        raise ValueError(
            "PostgreSQL version must not be empty."
        )

    match = re.search(
        r"(?<!\d)(\d+)(?:\.(\d+))(?:\.(\d+))?",
        version_string,
    )

    if not match:
        raise ValueError(
            f"Unable to extract a PostgreSQL version from: "
            f"{version_string!r}"
        )

    first = int(match.group(1))
    second = int(match.group(2))
    third = match.group(3)

    if first >= 10:
        # PostgreSQL 10 and later use MAJOR.MINOR.
        installed_version = f"{first}.{second}"
        major_version = str(first)
    else:
        # PostgreSQL 9.x and earlier use MAJOR.MINOR.PATCH.
        if third is None:
            raise ValueError(
                "PostgreSQL versions older than 10 must include three "
                "components, for example 9.6.24."
            )

        installed_version = f"{first}.{second}.{int(third)}"
        major_version = f"{first}.{second}"

    return installed_version, major_version


def _version_sort_key(version: str) -> tuple[int, ...]:
    """
    Convert a PostgreSQL version into a tuple suitable for comparison.
    """
    return tuple(
        int(part)
        for part in version.split(".")
    )


def _fetch_postgresql_versions(
    *,
    timeout_seconds: float = 5.0,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch PostgreSQL version information from the official PostgreSQL website.

    Results are cached in memory for 24 hours to avoid one HTTP request for
    every analyzed database.
    """
    global _versions_cache

    now = time.monotonic()

    if (
        not force_refresh
        and _versions_cache is not None
        and now - _versions_cache[0] < CACHE_TTL_SECONDS
    ):
        return _versions_cache[1]

    request = Request(
        POSTGRESQL_VERSIONS_URL,
        headers={
            "Accept": "application/json",
            "User-Agent": "pgAssistant/version-check",
        },
    )

    try:
        with urlopen(
            request,
            timeout=timeout_seconds,
        ) as response:
            payload = json.load(response)

    except (
        HTTPError,
        URLError,
        TimeoutError,
        json.JSONDecodeError,
    ) as exc:
        raise RuntimeError(
            "Unable to retrieve PostgreSQL release information from "
            "postgresql.org."
        ) from exc

    if not isinstance(payload, list):
        raise RuntimeError(
            "Unexpected response received from the PostgreSQL "
            "versions endpoint."
        )

    required_fields = {
        "major",
        "latestMinor",
        "supported",
    }

    for release in payload:
        if not isinstance(release, dict):
            raise RuntimeError(
                "Unexpected release entry received from "
                "postgresql.org."
            )

        if not required_fields.issubset(release):
            raise RuntimeError(
                "The PostgreSQL versions response does not contain "
                "the expected fields."
            )

    _versions_cache = (now, payload)

    return payload


def get_postgresql_upgrade_recommendation(
    version_string: str,
    *,
    timeout_seconds: float = 5.0,
) -> PostgreSQLUpgradeRecommendation:
    """
    Compare an installed PostgreSQL version with the latest release available
    for the same major branch.

    The function also detects unsupported PostgreSQL major versions and
    includes the publication date of the latest release when available.
    """
    installed_version, major_version = (
        _parse_postgresql_version(version_string)
    )

    releases = _fetch_postgresql_versions(
        timeout_seconds=timeout_seconds,
    )

    release_info = next(
        (
            release
            for release in releases
            if str(release["major"]) == major_version
        ),
        None,
    )

    if release_info is None:
        raise ValueError(
            f"PostgreSQL major version {major_version} was not found "
            "in the official PostgreSQL version list."
        )

    latest_minor = str(
        release_info["latestMinor"]
    )

    latest_version = (
        f"{major_version}.{latest_minor}"
    )

    latest_release_date = release_info.get(
        "relDate"
    )

    end_of_life_date = release_info.get(
        "eolDate"
    )

    supported = bool(
        release_info["supported"]
    )

    installed_key = _version_sort_key(
        installed_version
    )

    latest_key = _version_sort_key(
        latest_version
    )

    release_date_note = (
        f", released on {latest_release_date}"
        if latest_release_date
        else ""
    )

    if installed_key > latest_key:
        return PostgreSQLUpgradeRecommendation(
            installed_version=installed_version,
            major_version=major_version,
            latest_minor_version=latest_version,
            latest_release_date=latest_release_date,
            supported=supported,
            end_of_life_date=end_of_life_date,
            upgrade_recommended=False,
            recommendation_level="REVIEW",
            recommendation=(
                f"Installed PostgreSQL version {installed_version} is newer "
                f"than the latest official release recorded for branch "
                f"{major_version}, which is PostgreSQL "
                f"{latest_version}{release_date_note}. Verify whether this "
                "is a beta, release candidate, development, or "
                "vendor-specific build."
            ),
        )

    if not supported:
        if installed_key < latest_key:
            recommendation = (
                f"PostgreSQL {installed_version} is running on the "
                f"unsupported {major_version} branch. The final minor "
                f"release for this branch is PostgreSQL "
                f"{latest_version}{release_date_note}. Upgrade at least to "
                f"PostgreSQL {latest_version} as an immediate remediation, "
                "then plan a major upgrade to a supported PostgreSQL version."
            )
        else:
            recommendation = (
                f"PostgreSQL {installed_version} is the latest minor release "
                f"of the unsupported {major_version} branch"
                f"{release_date_note}. Plan a major upgrade to a supported "
                "PostgreSQL version."
            )

        if end_of_life_date:
            recommendation += (
                f" End-of-life date: {end_of_life_date}."
            )

        return PostgreSQLUpgradeRecommendation(
            installed_version=installed_version,
            major_version=major_version,
            latest_minor_version=latest_version,
            latest_release_date=latest_release_date,
            supported=False,
            end_of_life_date=end_of_life_date,
            upgrade_recommended=True,
            recommendation_level="HIGH",
            recommendation=recommendation,
        )

    if installed_key < latest_key:
        return PostgreSQLUpgradeRecommendation(
            installed_version=installed_version,
            major_version=major_version,
            latest_minor_version=latest_version,
            latest_release_date=latest_release_date,
            supported=True,
            end_of_life_date=end_of_life_date,
            upgrade_recommended=True,
            recommendation_level="MEDIUM",
            recommendation=(
                f"PostgreSQL {installed_version} is not the latest minor release "
                f"available for branch {major_version}. Upgrade to PostgreSQL "
                f"{latest_version}{release_date_note} to benefit from the latest "
                "bug, security, reliability, and data-integrity fixes. Review the "
                "intermediate release notes before upgrading."
            ),
        )

    return PostgreSQLUpgradeRecommendation(
        installed_version=installed_version,
        major_version=major_version,
        latest_minor_version=latest_version,
        latest_release_date=latest_release_date,
        supported=True,
        end_of_life_date=end_of_life_date,
        upgrade_recommended=False,
        recommendation_level="NONE",
        recommendation=(
            f"PostgreSQL {installed_version} is the latest minor release "
            f"available for the supported {major_version} branch"
            f"{release_date_note}."
        ),
    )