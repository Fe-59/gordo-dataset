from dataclasses import dataclass
from enum import Enum
from typing import Union, Iterable
from datetime import datetime


@dataclass(frozen=True)
class YearPartition:
    year: int


@dataclass(frozen=True)
class MonthPartition:
    year: int
    month: int


Partition = Union[YearPartition, MonthPartition]


class PartitionBy(Enum):
    YEAR = "year"
    MONTH = "month"


def split_by_partitions(
    partition_by: PartitionBy, start_period: datetime, end_period: datetime
) -> Iterable[Partition]:
    if start_period > end_period:
        message = "start_period bigger then end_period."
        message += "'%s' > '%s'" % (start_period.isoformat(), end_period.isoformat())
        raise ValueError(message)
    if partition_by is PartitionBy.YEAR:
        for year in range(start_period.year, end_period.year + 1):
            yield YearPartition(year)
    elif partition_by is PartitionBy.MONTH:
        for year in range(start_period.year, end_period.year + 1):
            for month in range(1, 12 + 1):
                if (year == start_period.year and month < start_period.month) or (
                    year == end_period.year and month > end_period.month
                ):
                    continue
                yield MonthPartition(year, month)
    else:
        raise ValueError("Unknown partition_by type %s" % partition_by)
