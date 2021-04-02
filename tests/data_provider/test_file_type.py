import pytest
import os
import pandas as pd
import numpy as np

from gordo_dataset.data_provider.file_type import ParquetFileType
from gordo_dataset.data_provider.ncs_file_type import time_series_columns


@pytest.fixture
def data_file_type_path():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(base_dir, "data", "file_type")


@pytest.mark.parametrize(
    "file_name", ["right_dtypes.parquet", "all_string_types.parquet"]
)
def test_file_type_all_string_types(data_file_type_path, file_name):
    file_path = os.path.join(data_file_type_path, file_name)
    file_type = ParquetFileType(time_series_columns)
    with open(file_path, "rb") as f:
        df = file_type.read_df(f)
    assert isinstance(df.index, pd.DatetimeIndex)
    assert np.issubdtype(df["Value"].dtypes, np.number)
    assert np.issubdtype(df["Status"].dtypes, np.number)
