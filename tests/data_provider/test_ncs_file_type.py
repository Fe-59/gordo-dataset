import pytest

from gordo_dataset.exceptions import ConfigException
from gordo_dataset.data_provider.file_type import CsvFileType, ParquetFileType
from gordo_dataset.data_provider.partition import YearPartition, MonthPartition
from gordo_dataset.data_provider.ncs_file_type import (
    NcsCsvFileType,
    NcsMonthlyParquetFileType,
    NcsYearlyParquetFileType,
    load_ncs_file_types,
)


def test_ncs_csv_file_type(mock_file_system):
    ncs_file_type = NcsCsvFileType()
    assert type(ncs_file_type.file_type) is CsvFileType
    assert ncs_file_type.partition_type is YearPartition
    partition = YearPartition(2020)
    paths = list(ncs_file_type.paths(mock_file_system, "tag1", [partition]))
    assert list(paths) == [(partition, "tag1_2020.csv")]


def test_ncs_yearly_parquet_file_type(mock_file_system):
    ncs_file_type = NcsYearlyParquetFileType()
    assert type(ncs_file_type.file_type) is ParquetFileType
    assert ncs_file_type.partition_type is YearPartition
    partition = YearPartition(2020)
    paths = ncs_file_type.paths(mock_file_system, "tag1", [partition])
    assert list(paths) == [(partition, "parquet/tag1_2020.parquet")]


def test_ncs_monthly_parquet_file_type(mock_file_system):
    ncs_file_type = NcsMonthlyParquetFileType()
    assert type(ncs_file_type.file_type) is ParquetFileType
    partition = MonthPartition(2020, 3)
    paths = ncs_file_type.paths(mock_file_system, "tag1", [partition])
    assert list(paths) == [(partition, "parquet/2020/tag1_202003.parquet")]


def test_load_ncs_file_types():
    ncs_file_types = load_ncs_file_types()
    assert len(ncs_file_types) == 3
    assert type(ncs_file_types[0]) is NcsMonthlyParquetFileType
    assert type(ncs_file_types[1]) is NcsYearlyParquetFileType
    assert type(ncs_file_types[2]) is NcsCsvFileType
    ncs_file_types = load_ncs_file_types(("parquet",))
    assert len(ncs_file_types) == 1
    assert type(ncs_file_types[0]) is NcsMonthlyParquetFileType


def test_load_ncs_file_types_exception():
    with pytest.raises(ConfigException):
        load_ncs_file_types(["json"])
