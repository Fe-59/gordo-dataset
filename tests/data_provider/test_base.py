import pytest

from gordo_dataset.data_provider.base import GordoBaseDataProvider
from gordo_dataset.data_provider.providers import (
    DataLakeProvider,
    InfluxDataProvider,
    RandomDataProvider,
)
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


def test_to_dict_built_in():
    data_provider = RandomDataProvider()
    config = data_provider.to_dict()
    assert config["type"] == "RandomDataProvider"


class CustomRandomDataProvider(RandomDataProvider):
    pass


def test_to_dict_custom():
    data_provider = CustomRandomDataProvider()
    config = data_provider.to_dict()
    assert config["type"] == "tests.data_provider.test_base.CustomRandomDataProvider"
