from abc import ABCMeta, abstractmethod
from datetime import datetime, time
from enum import Enum
from typing import Dict, Iterable, cast

from dateutil.relativedelta import relativedelta


class TimeSpan(metaclass=ABCMeta):
    @abstractmethod
    def alignment(self, dt: datetime) -> datetime:
        ...

    @abstractmethod
    def add_span(self, dt: datetime) -> datetime:
        ...

    def __repr__(self):
        class_name = self.__class__.__name__
        return "%s()" % class_name


class YearTimeSpan(TimeSpan):
    def alignment(self, dt: datetime) -> datetime:
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    def add_span(self, dt: datetime) -> datetime:
        return dt + relativedelta(years=1)


class MonthTimeSpan(TimeSpan):
    def alignment(self, dt: datetime) -> datetime:
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def add_span(self, dt: datetime) -> datetime:
        return dt + relativedelta(months=1)


class TimeSpanBy(Enum):
    YEAR = 1
    MONTH = 2


_time_spans: Dict[TimeSpanBy, TimeSpan] = {
    TimeSpanBy.YEAR: YearTimeSpan(),
    TimeSpanBy.MONTH: MonthTimeSpan(),
}


def get_time_span(by: TimeSpanBy) -> TimeSpan:
    if by not in _time_spans:
        raise ValueError("Unknown time span with name '%s'" % by.name)
    return _time_spans[by]


class TimePeriod(metaclass=ABCMeta):
    @abstractmethod
    def get_period_start(self) -> datetime:
        ...

    @abstractmethod
    def get_period_end(self) -> datetime:
        ...

    def __eq__(self, other):
        if not isinstance(other, TimePeriod):
            raise ValueError(
                "Objects %s and %s is not comparable" % (repr(self), repr(other))
            )
        other = cast(TimePeriod, other)
        return (
            self.get_period_start() == other.get_period_start()
            and self.get_period_end() == other.get_period_end()
        )


class SpanTimePeriod(TimePeriod):
    def __init__(self, period_start: datetime, time_span: TimeSpan):
        self._period_start = period_start
        self._time_span = time_span

    def get_period_start(self) -> datetime:
        return self._period_start

    def get_period_end(self) -> datetime:
        return self._time_span.add_span(self._period_start) - time.resolution

    def __repr__(self):
        class_name = self.__class__.__name__
        return "%s(%s, %s)" % (
            class_name,
            repr(self._period_start),
            repr(self._time_span),
        )


class ExactTimePeriod(TimePeriod):
    def __init__(self, period_start: datetime, period_end: datetime):
        if period_start >= period_end:
            raise ValueError("period_start should be lower then period_end")
        self._period_start = period_start
        self._period_end = period_end

    def get_period_start(self) -> datetime:
        return self._period_start

    def get_period_end(self) -> datetime:
        return self._period_end

    def __repr__(self):
        class_name = self.__class__.__name__
        return "%s(%s, %s)" % (
            class_name,
            repr(self._period_start),
            repr(self._period_end),
        )


def split_period_by_span(
    time_period: TimePeriod, time_span: TimeSpan
) -> Iterable[TimePeriod]:
    start_time = time_span.alignment(time_period.get_period_start())
    period_end = time_period.get_period_end()
    current = SpanTimePeriod(start_time, time_span)
    while True:
        yield current
        new_start_time = time_span.add_span(start_time)
        if new_start_time <= start_time:
            class_name = time_span.__class__.__name__
            raise ValueError("Unexpected behavior of %s.get_span() method" % class_name)
        if new_start_time > period_end:
            break
        start_time = new_start_time
        current = SpanTimePeriod(start_time, time_span)
