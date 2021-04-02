from abc import ABCMeta, abstractmethod

from gordo_dataset.file_system import FileSystem
from .file_type import FileType, ParquetFileType, CsvFileType, TimeSeriesColumns
from .partition import Partition, YearPartition, MonthPartition

from typing import Iterable, Optional, List, Tuple, Type, Dict, cast

from ..exceptions import ConfigException

time_series_columns = TimeSeriesColumns("Time", "Value", "Status")


class NcsFileType(metaclass=ABCMeta):
    """
    Represents logic about finding files of one particular type for ``NcsLookup``
    """

    @property
    @abstractmethod
    def file_type(self) -> FileType:
        ...

    @property
    @abstractmethod
    def partition_type(self) -> Type[Partition]:
        ...

    def check_partition(self, partition: Partition):
        return isinstance(partition, self.partition_type)

    @abstractmethod
    def paths(
        self, fs: FileSystem, tag_name: str, partitions: Iterable[Partition]
    ) -> Iterable[Tuple[Partition, str]]:
        """
        Possible file paths for this file type. These paths should be relational to the tag directory

        Parameters
        ----------
        fs: FileSystem
        tag_name: str
        partitions: Iterable[Partition]

        Returns
        -------
        Iterable[Tuple[Partition, str]]

        """
        ...


class NcsMonthlyParquetFileType(NcsFileType):
    """
    NCS monthly parquet files finder
    """

    def __init__(self):
        self._file_type = ParquetFileType(time_series_columns)
        self._partition_type = YearPartition

    @property
    def file_type(self) -> FileType:
        return self._file_type

    @property
    def partition_type(self) -> Type[Partition]:
        return MonthPartition

    def paths(
        self, fs: FileSystem, tag_name: str, partitions: Iterable[Partition]
    ) -> Iterable[Tuple[Partition, str]]:
        file_extension = self._file_type.file_extension
        for partition in partitions:
            if not self.check_partition(partition):
                raise NotImplementedError()
            partition = cast(MonthPartition, partition)
            file_name = (
                f"{tag_name}_{partition.year}{partition.month:02d}{file_extension}"
            )
            path = fs.join("parquet", str(partition.year), file_name)
            yield partition, path


class NcsYearlyParquetFileType(NcsFileType):
    """
    NCS yearly parquet files finder
    """

    def __init__(self):
        self._file_type = ParquetFileType(time_series_columns)
        self._partition_type = YearPartition

    @property
    def file_type(self) -> FileType:
        return self._file_type

    @property
    def partition_type(self) -> Type[Partition]:
        return YearPartition

    def paths(
        self, fs: FileSystem, tag_name: str, partitions: Iterable[Partition]
    ) -> Iterable[Tuple[Partition, str]]:
        file_extension = self._file_type.file_extension
        for partition in partitions:
            if not self.check_partition(partition):
                raise NotImplementedError()
            partition = cast(YearPartition, partition)
            path = fs.join("parquet", f"{tag_name}_{partition.year}{file_extension}")
            yield partition, path


class NcsCsvFileType(NcsFileType):
    """
    NCS CSV files finder
    """

    def __init__(self):
        header = ["Sensor", "Value", "Time", "Status"]
        self._file_type = CsvFileType(header, time_series_columns)
        self._partition_type = YearPartition

    @property
    def file_type(self) -> FileType:
        return self._file_type

    @property
    def partition_type(self) -> Type[Partition]:
        return self._partition_type

    def paths(
        self, fs: FileSystem, tag_name: str, partitions: Iterable[Partition]
    ) -> Iterable[Tuple[Partition, str]]:
        file_extension = self._file_type.file_extension
        for partition in partitions:
            if not self.check_partition(partition):
                raise NotImplementedError()
            path = f"{tag_name}_{partition.year}{file_extension}"
            yield partition, path


ncs_file_types: Dict[str, Type[NcsFileType]] = {
    "parquet": NcsMonthlyParquetFileType,
    "yearly_parquet": NcsYearlyParquetFileType,
    "csv": NcsCsvFileType,
}

DEFAULT_TYPE_NAMES: List[str] = ["parquet", "yearly_parquet", "csv"]


def load_ncs_file_types(
    type_names: Optional[Iterable[str]] = None,
) -> List[NcsFileType]:
    """
    Returns list of ``NcsFileType`` instances from names of those types

    Parameters
    ----------
    type_names: Optional[Iterable[str]]
        List of ``NcsFileType`` names. Only supporting `parquet` and `csv` names values

    Returns
    -------
    List[NcsFileType]

    """
    if type_names is None:
        type_names = DEFAULT_TYPE_NAMES
    result = []
    for type_name in type_names:
        if type_name not in ncs_file_types:
            raise ConfigException("Can not find file type '%s'" % type_name)
        result.append(ncs_file_types[type_name]())
    return result
