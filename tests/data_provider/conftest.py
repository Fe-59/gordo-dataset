from gordo_dataset.data_provider.base import GordoBaseDataProvider
import pytest
import posixpath

from unittest.mock import MagicMock

from gordo_dataset.base import GordoBaseDataset
from gordo_dataset.sensor_tag import SensorTag
from gordo_dataset.utils import capture_args
from datetime import datetime
from copy import copy
from typing import Iterable, List, Optional
import pandas as pd

class DummyDataProvider(GordoBaseDataProvider):
    @capture_args
    def __init__(self, arg1) -> None:
        self.arg1 = arg1

    def load_series(
        self,
        train_start_date: datetime,
        train_end_date: datetime,
        tag_list: List[SensorTag],
        dry_run: Optional[bool] = False,
    ) -> Iterable[pd.Series]:
        yield pd.Series()

    def to_dict(self):
        if not hasattr(self, "_params"):
            raise AttributeError(
                "Failed to lookup init parameters, ensure the "
                "object's __init__ is decorated with 'capture_args'"
            )
        # Update dict with the class
        params = self._params
        module_str = self.__class__.__module__
        if module_str is None or module_str == str.__class__.__module__:
            module_str = self.__class__.__name__
        else:
            module_str = module_str + '.' + self.__class__.__name__
        params["type"] = module_str
        return params


@pytest.fixture
def mock_file_system():
    mock = MagicMock()
    mock.join.side_effect = posixpath.join
    mock.split.side_effect = posixpath.split
    return mock

@pytest.fixture
def dummy_data_provider():
    return DummyDataProvider("test_arg")

