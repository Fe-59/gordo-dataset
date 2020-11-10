import pytest

from mock import patch, MagicMock, Mock
from typing import Optional

from gordo_dataset.data_provider.storages import create_storage
from gordo_dataset.file_system.azure import ADLSecret
from gordo_dataset.data_provider.secrets_loaders import ADLSecretsLoader
from gordo_dataset.exceptions import ConfigException


class TestSecretsLoader(ADLSecretsLoader):
    def __init__(self, adl_secret: ADLSecret):
        self.adl_secret = adl_secret

    def get_secret(self, storage_type: str, storage_name: str) -> Optional[ADLSecret]:
        return self.adl_secret


@pytest.fixture
def adl_secret():
    return ADLSecret("tenant_id", "client_id", "client_secret")


@pytest.fixture
def secrets_loader(adl_secret):
    return TestSecretsLoader(adl_secret)


def test_create_storage_adl1(secrets_loader, adl_secret):
    with patch(
        "gordo_dataset.file_system.adl1.ADLGen1FileSystem", MagicMock()
    ) as adl1_mock:
        create_storage("adl1", secrets_loader=secrets_loader)
        adl1_mock.create_from_env.assert_called_once_with(
            store_name="dataplatformdlsprod", adl_secret=adl_secret
        )


def test_create_storage_adl2(secrets_loader, adl_secret):
    with patch(
        "gordo_dataset.file_system.adl2.ADLGen2FileSystem", MagicMock()
    ) as adl2_mock:
        create_storage("adl2", secrets_loader=secrets_loader)
        adl2_mock.create_from_env.assert_called_once_with(
            account_name="omniadlseun", file_system_name="dls", adl_secret=adl_secret
        )


def test_create_storage_exception():
    with patch("gordo_dataset.file_system.adl1.ADLGen1FileSystem", MagicMock()):
        with pytest.raises(ConfigException):
            create_storage("adl1", secrets_loader=dict())
