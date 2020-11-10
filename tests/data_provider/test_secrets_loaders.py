import pytest

from mock import patch

from gordo_dataset.data_provider.secrets_loaders import ADLSecretsLoader, ADLEnvSecretsLoader
from gordo_dataset.exceptions import ConfigException


def test_adl_env_secrets_loader():
    with patch("os.environ.get") as get_mock:
        get_mock.return_value = "tenant_id:client_id:client_secret"
        secrets_loader = ADLEnvSecretsLoader().from_env("fs", "storage", "STORAGE_SECRET")
        adl_secret = secrets_loader.get_secret("fs", "storage")
        get_mock.assert_called_once_with("STORAGE_SECRET")
        assert adl_secret.tenant_id == "tenant_id"
        assert adl_secret.client_id == "client_id"
        assert adl_secret.client_secret == "client_secret"


def test_adl_env_secrets_loader_config_exception():
    secrets_loader = ADLEnvSecretsLoader().from_env("fs", "storage", "STORAGE_SECRET")
    with pytest.raises(ConfigException):
        secrets_loader.get_secret("wrong_fs", "storage")
    with pytest.raises(ConfigException):
        secrets_loader.get_secret("fs", "wrong_storage")


def test_adl_env_secrets_loader_empty_env():
    with patch("os.environ.get") as get_mock:
        get_mock.return_value = None
        secrets_loader = ADLEnvSecretsLoader().from_env("fs", "storage", "STORAGE_SECRET")
        assert secrets_loader.get_secret("fs", "storage") is None


def test_adl_env_secrets_loader_malformed_env_val():
    with patch("os.environ.get") as get_mock:
        get_mock.return_value = "tenant_id:client_id"
        secrets_loader = ADLEnvSecretsLoader().from_env("fs", "storage", "STORAGE_SECRET")
        with pytest.raises(ValueError):
            secrets_loader.get_secret("fs", "storage")
