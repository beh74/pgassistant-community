import os
from dataclasses import dataclass
from typing import Dict, List, Any, Optional

import hvac
import yaml


class VaultConfigError(Exception):
    """Raised when Vault configuration is invalid."""


class VaultSecretError(Exception):
    """Raised when a Vault secret cannot be read or normalized."""


@dataclass
class VaultSettings:
    url: str
    mount_point: str
    root_path: str
    role_id: str
    secret_id: str
    field_mapping: Dict[str, str]


class VaultPgAssistantClient:
    """
    Vault client for pgAssistant using:
    - AppRole auth
    - KV v2 secrets
    - YAML field mapping for DB connection properties
    """

    REQUIRED_LOGICAL_FIELDS = {"host", "port", "dbname", "username", "password"}

    def __init__(self, config_file: str):
        self.settings = self._load_settings(config_file)
        self.client = self._build_authenticated_client()

    def _load_settings(self, config_file: str) -> VaultSettings:
        """
        Load YAML config + environment variables.
        """
        if not os.path.exists(config_file):
            raise VaultConfigError(f"Config file not found: {config_file}")

        with open(config_file, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        vault_cfg = raw.get("vault", {})
        mapping = raw.get("mapping", {})

        missing_mapping = self.REQUIRED_LOGICAL_FIELDS - set(mapping.keys())
        if missing_mapping:
            raise VaultConfigError(
                f"Missing required mapping keys: {', '.join(sorted(missing_mapping))}"
            )

        url = vault_cfg.get("url")
        if not url:
            raise VaultConfigError("Missing 'vault.url' in YAML config")

        mount_point = vault_cfg.get("mount_point", "secret")

        role_id = os.environ.get("PGASSISTANT_VAULT_ROLE_ID")
        secret_id = os.environ.get("PGASSISTANT_VAULT_SECRET_ID")
        root_path = os.environ.get("PGASSISTANT_VAULT_ROOT_PATH")

        if not role_id:
            raise VaultConfigError("Missing environment variable: PGASSISTANT_VAULT_ROLE_ID")
        if not secret_id:
            raise VaultConfigError("Missing environment variable: PGASSISTANT_VAULT_SECRET_ID")
        if not root_path:
            raise VaultConfigError("Missing environment variable: PGASSISTANT_VAULT_ROOT_PATH")

        return VaultSettings(
            url=url,
            mount_point=mount_point,
            root_path=root_path.strip("/"),
            role_id=role_id,
            secret_id=secret_id,
            field_mapping=mapping,
        )

    def _build_authenticated_client(self) -> hvac.Client:
        """
        Authenticate to Vault using AppRole.
        """
        client = hvac.Client(url=self.settings.url)

        auth_response = client.auth.approle.login(
            role_id=self.settings.role_id,
            secret_id=self.settings.secret_id,
        )

        if not client.is_authenticated():
            raise VaultConfigError("Vault authentication failed with AppRole")

        return client

    def _normalize_secret(self, secret_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a raw Vault secret to pgAssistant's normalized format.
        """
        mapping = self.settings.field_mapping

        normalized = {
            "host": secret_data.get(mapping["host"]),
            "port": secret_data.get(mapping["port"]),
            "dbname": secret_data.get(mapping["dbname"]),
            "username": secret_data.get(mapping["username"]),
            "password": secret_data.get(mapping["password"]),
        }

        missing = [k for k, v in normalized.items() if v in (None, "")]
        if missing:
            raise VaultSecretError(
                f"Missing mapped fields in Vault secret: {', '.join(missing)}"
            )

        # Normalize port to int when possible
        try:
            normalized["port"] = int(normalized["port"])
        except (TypeError, ValueError):
            raise VaultSecretError(
                f"Invalid port value: {normalized['port']!r}"
            )

        return normalized

    def list_entries(self) -> List[str]:
        """
        List all entries under the configured root path.

        Example:
            root_path = 'pgsql/dev'
            returns ['database1', 'mydb2']
        """
        try:
            result = self.client.secrets.kv.v2.list_secrets(
                path=self.settings.root_path,
                mount_point=self.settings.mount_point,
            )
            keys = result.get("data", {}).get("keys", [])
            return sorted(keys)
        except Exception as e:
            raise VaultSecretError(
                f"Unable to list Vault entries under '{self.settings.root_path}': {e}"
            ) from e

    def list_entry_paths(self) -> List[str]:
        """
        Return full relative paths under the configured root path.

        Example:
            root_path = 'pgsql/dev'
            keys = ['database1', 'mydb2']
            returns ['pgsql/dev/database1', 'pgsql/dev/mydb2']
        """
        return [f"{self.settings.root_path}/{key}".rstrip("/") for key in self.list_entries()]

    def read_entry_raw(self, entry_name: str) -> Dict[str, Any]:
        """
        Read a raw KV entry under the configured root path.

        Example:
            entry_name='database1'
            reads 'pgsql/dev/database1'
        """
        entry_name = entry_name.strip("/")
        path = f"{self.settings.root_path}/{entry_name}"

        try:
            result = self.client.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=self.settings.mount_point,
            )
            return result.get("data", {}).get("data", {})
        except Exception as e:
            raise VaultSecretError(f"Unable to read Vault entry '{path}': {e}") from e

    def read_entry(self, entry_name: str) -> Dict[str, Any]:
        """
        Read and normalize one entry using the YAML field mapping.

        Returns:
            {
                'host': ...,
                'port': ...,
                'dbname': ...,
                'username': ...,
                'password': ...
            }
        """
        raw_data = self.read_entry_raw(entry_name)
        return self._normalize_secret(raw_data)

    def read_entry_by_full_path(self, full_path: str) -> Dict[str, Any]:
        """
        Read and normalize one entry using its full relative path.

        Example:
            full_path='pgsql/dev/database1'
        """
        full_path = full_path.strip("/")

        root = self.settings.root_path
        prefix = f"{root}/"

        if full_path == root:
            raise VaultSecretError("The root path itself is not a readable entry")

        if not full_path.startswith(prefix):
            raise VaultSecretError(
                f"Path '{full_path}' is outside configured root path '{root}'"
            )

        entry_name = full_path[len(prefix):]
        return self.read_entry(entry_name)