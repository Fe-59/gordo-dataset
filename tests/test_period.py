import pytest

from datetime import datetime
from gordo_dataset.period import (
    YearTimeSpan,
    MonthTimeSpan,
    SpanTimePeriod,
    ExactTimePeriod,
    split_period_by_span,
)


@pytest.mark.parametrize(
    "time_span_type, dt, result",
    [
        (YearTimeSpan, datetime(2020, 3, 20, 0, 0), datetime(2020, 1, 1, 0, 0)),
        (MonthTimeSpan, datetime(2020, 3, 20, 0, 0), datetime(2020, 3, 1, 0, 0)),
    ],
)
def test_time_span_alignment(time_span_type, dt, result):
    time_span = time_span_type()
    assert time_span.alignment(dt) == result


@pytest.mark.parametrize(
    "time_span_type, dt, result",
    [
        (YearTimeSpan, datetime(2020, 3, 20, 0, 0), datetime(2021, 3, 20, 0, 0)),
        (YearTimeSpan, datetime(2020, 1, 1, 0, 0), datetime(2021, 1, 1, 0, 0)),
        (MonthTimeSpan, datetime(2020, 5, 20, 0, 0), datetime(2020, 6, 20, 0, 0)),
        (MonthTimeSpan, datetime(2020, 3, 1, 0, 0), datetime(2020, 4, 1, 0, 0)),
    ],
)
def test_time_span_add_span(time_span_type, dt, result):
    time_span = time_span_type()
    assert time_span.add_span(dt) == result


def test_span_time_period():
    time_period = SpanTimePeriod(datetime(2020, 3, 20, 0, 0), MonthTimeSpan())
    assert time_period.get_period_start() == datetime(2020, 3, 20, 0, 0)
    assert time_period.get_period_end() == datetime(2020, 4, 19, 23, 59, 59, 999999)


def test_exact_time_period():
    time_period = ExactTimePeriod(
        datetime(2020, 3, 20, 0, 0), datetime(2020, 4, 20, 0, 0)
    )
    assert time_period.get_period_start() == datetime(2020, 3, 20, 0, 0)
    assert time_period.get_period_end() == datetime(2020, 4, 20, 0, 0)


def test_exact_time_period_validation_error():
    with pytest.raises(ValueError):
        ExactTimePeriod(datetime(2020, 3, 20, 0, 0), datetime(2020, 3, 20, 0, 0))
    with pytest.raises(ValueError):
        ExactTimePeriod(datetime(2020, 5, 20, 0, 0), datetime(2020, 3, 20, 0, 0))


def test_time_period_eq():
    a = ExactTimePeriod(datetime(2020, 3, 20, 0, 0), datetime(2020, 3, 25, 0, 0))
    b = ExactTimePeriod(datetime(2020, 3, 20, 0, 0), datetime(2020, 3, 25, 0, 0))
    assert a == b
    with pytest.raises(ValueError):
        assert a == "str"


def test_split_by_year_span_different_years():
    time_period = ExactTimePeriod(
        datetime(2018, 3, 20, 0, 0), datetime(2021, 3, 20, 0, 0)
    )
    periods = list(split_period_by_span(time_period, YearTimeSpan()))
    expect = [
        SpanTimePeriod(datetime(2018, 1, 1, 0, 0), YearTimeSpan()),
        SpanTimePeriod(datetime(2019, 1, 1, 0, 0), YearTimeSpan()),
        SpanTimePeriod(datetime(2020, 1, 1, 0, 0), YearTimeSpan()),
        SpanTimePeriod(datetime(2021, 1, 1, 0, 0), YearTimeSpan()),
    ]
    assert periods == expect


def test_split_by_year_span_same_year():
    time_period = ExactTimePeriod(
        datetime(2018, 3, 20, 0, 0), datetime(2018, 6, 20, 0, 0)
    )
    periods = list(split_period_by_span(time_period, YearTimeSpan()))
    expect = [SpanTimePeriod(datetime(2018, 1, 1, 0, 0), YearTimeSpan())]
    assert periods == expect


def test_split_by_month_span_different_months():
    time_period = ExactTimePeriod(
        datetime(2018, 10, 20, 0, 0), datetime(2019, 1, 20, 0, 0)
    )
    periods = list(split_period_by_span(time_period, MonthTimeSpan()))
    expect = [
        SpanTimePeriod(datetime(2018, 10, 1, 0, 0), MonthTimeSpan()),
        SpanTimePeriod(datetime(2018, 11, 1, 0, 0), MonthTimeSpan()),
        SpanTimePeriod(datetime(2018, 12, 1, 0, 0), MonthTimeSpan()),
        SpanTimePeriod(datetime(2019, 1, 1, 0, 0), MonthTimeSpan()),
    ]
    assert periods == expect


def test_split_by_month_span_same_month():
    time_period = ExactTimePeriod(
        datetime(2018, 10, 20, 0, 0), datetime(2018, 10, 25, 0, 0)
    )
    periods = list(split_period_by_span(time_period, MonthTimeSpan()))
    expect = [SpanTimePeriod(datetime(2018, 10, 1, 0, 0), MonthTimeSpan())]
    assert periods == expect
