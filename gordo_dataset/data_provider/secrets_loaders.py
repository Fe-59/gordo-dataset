import os

from typing import Optional
from abc import ABCMeta, abstractmethod
from collections import defaultdict

from gordo_dataset.file_system import FileSystem
from gordo_dataset.file_system.adl1 import ADLGen1FileSystem
from gordo_dataset.file_system.adl2 import ADLGen2FileSystem
from gordo_dataset.file_system.azure import ADLSecret
from gordo_dataset.exceptions import ConfigException


class ADLSecretsLoader(metaclass=ABCMeta):
    """
    Base class for ``ADLSecret`` loader.  Such class can be used for loading
    ``ADLSecret`` from different kinds of sources: files, environment variables, etc.
    """

    @abstractmethod
    def get_secret(self, storage_type: str, storage_name: str) -> Optional[ADLSecret]:
        ...


class ADLEnvSecretsLoader(ADLSecretsLoader):
    """
    Loading ``ADLSecret`` from environment variables.

    Examples
    --------
    >>> secrets_loader = ADLEnvSecretsLoader().from_env("fs", "storage", "TEST_ENV_VAR")
    >>> secrets_loader.get_secret("fs", "storage")
    """

    def __init__(self):
        self._secrets_envs = defaultdict(dict)

    def from_env(self, storage_type: str, storage_name: str, env_var: str):
        self._secrets_envs[storage_type][storage_name] = env_var
        return self

    def get_secret(self, storage_type: str, storage_name: str) -> Optional[ADLSecret]:
        if storage_type not in self._secrets_envs:
            raise ConfigException("Unknown storage type '%s'" % storage_type)
        if storage_name not in self._secrets_envs[storage_type]:
            raise ConfigException(
                "Unknown storage name '%s' for type '%s'" % (storage_type, storage_name)
            )
        env_var_name = self._secrets_envs[storage_type][storage_name]
        env_var = os.environ.get(env_var_name)
        if not env_var:
            return None
        data = env_var.split(":")
        if len(data) != 3:
            raise ValueError(
                "Environment variable %s has %d fields, but 3 is required"
                % (env_var_name, len(data))
            )
        tenant_id, client_id, client_secret = data
        return ADLSecret(tenant_id, client_id, client_secret)
