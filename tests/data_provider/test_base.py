import pytest

from gordo_dataset.data_provider.base import GordoBaseDataProvider
from gordo_dataset.data_provider.providers import DataLakeProvider, InfluxDataProvider
from gordo_dataset.exceptions import ConfigException


def test_from_dict_default():
    config = {}
    data_provider = GordoBaseDataProvider.from_dict(config)
    assert type(data_provider) is DataLakeProvider


def test_from_dict_simple():
    config = {"type": "InfluxDataProvider", "measurement": "test"}
    data_provider = GordoBaseDataProvider.from_dict(config)
    assert type(data_provider) is InfluxDataProvider


def test_from_dict_full_import():
    config = {
        "type": "gordo_dataset.data_provider.providers.InfluxDataProvider",
        "measurement": "test",
    }
    data_provider = GordoBaseDataProvider.from_dict(config)
    assert type(data_provider) is InfluxDataProvider


@pytest.mark.parametrize(
    "wrong_type",
    [
        "WrongProvider",
        "my_module.WrongProvider",
        "gordo_dataset.data_provider.providers.WrongProvider",
    ],
)
def test_from_dict_errors(wrong_type):
    with pytest.raises(ConfigException):
        config = {"type": wrong_type}
        GordoBaseDataProvider.from_dict(config)
