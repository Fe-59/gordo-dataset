import os

from typing import Optional
from abc import ABCMeta, abstractmethod
from collections import defaultdict

import gordo_dataset.file_system.adl1 as adl1
import gordo_dataset.file_system.adl2 as adl2

from gordo_dataset.file_system import FileSystem
from gordo_dataset.file_system.adl2 import ADLGen2FileSystem
from gordo_dataset.exceptions import ConfigException
from gordo_dataset.data_provider.secrets_loaders import (
    ADLSecretsLoader,
    ADLEnvSecretsLoader,
)


DEFAULT_STORAGE_TYPE = "adl1"

DEFAULT_SECRETS_LOADER: ADLSecretsLoader = ADLEnvSecretsLoader().from_env(
    "adl1", "dataplatformdlsprod", "DL_SERVICE_AUTH_STR"
).from_env("adl2", "omniadlseun", "DL2_SERVICE_AUTH_STR")


def create_storage(
    storage_type: Optional[str] = None,
    secrets_loader: Optional[ADLSecretsLoader] = None,
    **kwargs
) -> FileSystem:
    """
    Create ``FileSystem`` instance from the config

    Parameters
    ----------
    storage_type: Optional[str]
        Storage type only supported `adl1`, `adl2` values
    secrets_loader: Optional[ADLSecretsLoader]

    kwargs

    Returns
    -------

    """
    if storage_type is None:
        storage_type = DEFAULT_STORAGE_TYPE
    if secrets_loader is None:
        secrets_loader = DEFAULT_SECRETS_LOADER
    if not isinstance(secrets_loader, ADLSecretsLoader):
        raise ConfigException(
            "secrets_loader should be instance of ADLSecretsLoader and not %s type",
            type(secrets_loader),
        )
    storage: FileSystem
    if storage_type == "adl1":
        if "store_name" not in kwargs:
            kwargs["store_name"] = "dataplatformdlsprod"
        kwargs["adl_secret"] = secrets_loader.get_secret(
            storage_type, kwargs["store_name"]
        )
        storage = adl1.ADLGen1FileSystem.create_from_env(**kwargs)
    elif storage_type == "adl2":
        if "account_name" not in kwargs:
            kwargs["account_name"] = "omniadlseun"
        if "file_system_name" not in kwargs:
            kwargs["file_system_name"] = "dls"
        kwargs["adl_secret"] = secrets_loader.get_secret(
            storage_type, kwargs["account_name"]
        )
        storage = adl2.ADLGen2FileSystem.create_from_env(**kwargs)
    else:
        raise ConfigException("Unknown storage type '%s'" % storage_type)
    return storage
