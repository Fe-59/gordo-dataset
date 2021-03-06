# -*- coding: utf-8 -*-

from typing import List, Iterable, Optional, Tuple, Union

import pytest
import numpy as np
import pandas as pd
import dateutil.parser
from datetime import datetime
from unittest.mock import Mock

import xarray as xr

from gordo_dataset.data_provider.base import GordoBaseDataProvider
from gordo_dataset.datasets import (
    RandomDataset,
    TimeSeriesDataset,
)
from gordo_dataset.base import GordoBaseDataset
from gordo_dataset.exceptions import InsufficientDataError
from gordo_dataset.sensor_tag import SensorTag
from gordo_dataset.utils import join_timeseries
from gordo_dataset.dataset import _get_dataset
from gordo_dataset.utils import capture_args


@pytest.fixture
def dataset():
    return RandomDataset(
        train_start_date="2017-12-25 06:00:00Z",
        train_end_date="2017-12-29 06:00:00Z",
        tag_list=[SensorTag("Tag 1", None), SensorTag("Tag 2", None)],
    )


def create_timeseries_list():
    """Create three dataframes with different resolution and different start/ends"""
    # Test for no NaNs, test for correct first and last date
    latest_start = "2018-01-03 06:00:00Z"
    earliest_end = "2018-01-05 06:00:00Z"

    index_seconds = pd.date_range(
        start="2018-01-01 06:00:00Z", end="2018-01-07 06:00:00Z", freq="S"
    )
    index_minutes = pd.date_range(
        start="2017-12-28 06:00:00Z", end=earliest_end, freq="T"
    )
    index_hours = pd.date_range(
        start=latest_start, end="2018-01-12 06:00:00Z", freq="H"
    )

    timeseries_seconds = pd.Series(
        data=np.random.randint(0, 100, len(index_seconds)),
        index=index_seconds,
        name="ts-seconds",
    )
    timeseries_minutes = pd.Series(
        data=np.random.randint(0, 100, len(index_minutes)),
        index=index_minutes,
        name="ts-minutes",
    )
    timeseries_hours = pd.Series(
        data=np.random.randint(0, 100, len(index_hours)),
        index=index_hours,
        name="ts-hours",
    )

    return (
        [timeseries_seconds, timeseries_minutes, timeseries_hours],
        latest_start,
        earliest_end,
    )


def test_random_dataset_attrs(dataset):
    """
    Test expected attributes
    """

    assert isinstance(dataset, GordoBaseDataset)
    assert hasattr(dataset, "get_data")
    assert hasattr(dataset, "get_metadata")

    X, y = dataset.get_data()
    assert isinstance(X, pd.DataFrame)

    # y can either be None or an numpy array
    assert isinstance(y, pd.DataFrame) or y is None

    metadata = dataset.get_metadata()
    assert isinstance(metadata, dict)


def test_join_timeseries():

    timeseries_list, latest_start, earliest_end = create_timeseries_list()

    assert len(timeseries_list[0]) > len(timeseries_list[1]) > len(timeseries_list[2])

    frequency = "7T"
    timedelta = pd.Timedelta("7 minutes")
    resampling_start = dateutil.parser.isoparse("2017-12-25 06:00:00Z")
    resampling_end = dateutil.parser.isoparse("2018-01-15 08:00:00Z")
    all_in_frame, metadata = join_timeseries(
        timeseries_list, resampling_start, resampling_end, frequency
    )
    metadata_keys = sorted(metadata.keys())
    assert [
        "aggregate_metadata",
        "ts-hours",
        "ts-minutes",
        "ts-seconds",
    ] == metadata_keys

    # Check that first resulting resampled, joined row is within "frequency" from
    # the real first data point
    assert all_in_frame.index[0] >= pd.Timestamp(latest_start) - timedelta
    assert all_in_frame.index[-1] <= pd.Timestamp(resampling_end)


@pytest.mark.parametrize(
    "value,n_rows,resolution,error",
    [
        # Frequency passed as zero, resulting in an ZeroDivisionError during aggregation
        (None, None, "0T", ZeroDivisionError),
        # Empty series results in an InsufficientDataError
        (None, 0, "12T", InsufficientDataError),
        # When all rows are NaNs and dropped result in InsufficientDataError
        (np.NaN, None, "12T", InsufficientDataError),
        # Rows less then or equal to `row_threshold` result in InsufficientDataError
        (None, 6, "12T", InsufficientDataError),
    ],
)
def test_join_timeseries_empty_series(value, n_rows, resolution, error):
    """
    Test that empty data scenarios raise appropriate errors
    """
    train_start_date = dateutil.parser.isoparse("2018-01-01 00:00:00+00:00")
    train_end_date = dateutil.parser.isoparse("2018-01-05 00:00:00+00:00")
    tag_list = [SensorTag(name=n, asset=None) for n in ["Tag 1", "Tag 2", "Tag 3"]]

    kwargs = {
        "train_start_date": train_start_date,
        "train_end_date": train_end_date,
        "tag_list": tag_list,
        "resolution": resolution,
        "data_provider": MockDataProvider(value=np.NaN, n_rows=n_rows),
    }

    with pytest.raises(error):
        TimeSeriesDataset(**kwargs).get_data()


def test_join_timeseries_nonutcstart():
    timeseries_list, latest_start, earliest_end = create_timeseries_list()
    frequency = "7T"
    resampling_start = dateutil.parser.isoparse("2017-12-25 06:00:00+07:00")
    resampling_end = dateutil.parser.isoparse("2018-01-12 13:07:00+07:00")
    all_in_frame, metadata = join_timeseries(
        timeseries_list, resampling_start, resampling_end, frequency
    )
    metadata_keys = sorted(metadata.keys())
    assert [
        "aggregate_metadata",
        "ts-hours",
        "ts-minutes",
        "ts-seconds",
    ] == metadata_keys
    assert len(all_in_frame) == 481


def test_join_timeseries_with_gaps():

    timeseries_list, latest_start, earliest_end = create_timeseries_list()

    assert len(timeseries_list[0]) > len(timeseries_list[1]) > len(timeseries_list[2])

    remove_from = "2018-01-03 10:00:00Z"
    remove_to = "2018-01-03 18:00:00Z"
    timeseries_with_holes = [
        ts[(ts.index < remove_from) | (ts.index >= remove_to)] for ts in timeseries_list
    ]

    frequency = "10T"
    resampling_start = dateutil.parser.isoparse("2017-12-25 06:00:00Z")
    resampling_end = dateutil.parser.isoparse("2018-01-12 07:00:00Z")

    all_in_frame, metadata = join_timeseries(
        timeseries_with_holes, resampling_start, resampling_end, frequency
    )
    metadata_keys = sorted(metadata.keys())
    assert [
        "aggregate_metadata",
        "ts-hours",
        "ts-minutes",
        "ts-seconds",
    ] == metadata_keys
    assert all_in_frame.index[0] == pd.Timestamp(latest_start)
    assert all_in_frame.index[-1] <= pd.Timestamp(resampling_end)


def test_join_timeseries_with_interpolation_method_wrong_interpolation_method():
    timeseries_list, latest_start, earliest_end = create_timeseries_list()
    resampling_start = dateutil.parser.isoparse("2017-01-01 06:00:00+07:00")
    resampling_end = dateutil.parser.isoparse("2018-02-01 13:07:00+07:00")

    with pytest.raises(ValueError):
        join_timeseries(
            timeseries_list,
            resampling_start,
            resampling_end,
            resolution="10T",
            interpolation_method="wrong_method",
            interpolation_limit="8H",
        )


def test_join_timeseries_with_interpolation_method_wrong_interpolation_limit():
    timeseries_list, latest_start, earliest_end = create_timeseries_list()
    resampling_start = dateutil.parser.isoparse("2017-01-01 06:00:00+07:00")
    resampling_end = dateutil.parser.isoparse("2018-02-01 13:07:00+07:00")

    with pytest.raises(ValueError):
        join_timeseries(
            timeseries_list,
            resampling_start,
            resampling_end,
            resolution="10T",
            interpolation_method="ffill",
            interpolation_limit="1T",
        )


def test_join_timeseries_with_interpolation_method_linear_interpolation():
    timeseries_list, latest_start, earliest_end = create_timeseries_list()
    resampling_start = dateutil.parser.isoparse("2017-01-01 06:00:00+07:00")
    resampling_end = dateutil.parser.isoparse("2018-02-01 13:07:00+07:00")

    all_in_frame, metadata = join_timeseries(
        timeseries_list,
        resampling_start,
        resampling_end,
        resolution="10T",
        interpolation_method="linear_interpolation",
        interpolation_limit="8H",
    )
    metadata_keys = sorted(metadata.keys())
    assert [
        "aggregate_metadata",
        "ts-hours",
        "ts-minutes",
        "ts-seconds",
    ] == metadata_keys
    assert len(all_in_frame) == 337


def test_join_timeseries_with_interpolation_method_linear_interpolation_no_limit():
    timeseries_list, latest_start, earliest_end = create_timeseries_list()
    resampling_start = dateutil.parser.isoparse("2017-01-01 06:00:00+07:00")
    resampling_end = dateutil.parser.isoparse("2018-02-01 13:07:00+07:00")

    all_in_frame, metadata = join_timeseries(
        timeseries_list,
        resampling_start,
        resampling_end,
        resolution="10T",
        interpolation_method="linear_interpolation",
        interpolation_limit=None,
    )
    metadata_keys = sorted(metadata.keys())
    assert [
        "aggregate_metadata",
        "ts-hours",
        "ts-minutes",
        "ts-seconds",
    ] == metadata_keys
    assert len(all_in_frame) == 4177


def test_row_filter():
    """Tests that row_filter filters away rows"""
    kwargs = dict(
        data_provider=MockDataProvider(),
        tag_list=[
            SensorTag("Tag 1", "asset"),
            SensorTag("Tag 2", "asset"),
            SensorTag("Tag 3", "asset"),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
        asset="asset",
    )
    X, _ = TimeSeriesDataset(**kwargs).get_data()
    assert 83 == len(X)

    X, _ = TimeSeriesDataset(row_filter="`Tag 1` < 5000", **kwargs).get_data()
    assert 8 == len(X)

    X, _ = TimeSeriesDataset(
        row_filter="`Tag 1` / `Tag 3` < 0.999", **kwargs
    ).get_data()
    assert 3 == len(X)


def test_aggregation_methods():
    """Tests that it works to set aggregation method(s)"""

    kwargs = dict(
        data_provider=MockDataProvider(),
        tag_list=[
            SensorTag("Tag 1", None),
            SensorTag("Tag 2", None),
            SensorTag("Tag 3", None),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
    )

    # Default aggregation gives no extra columns
    X, _ = TimeSeriesDataset(**kwargs).get_data()
    assert (83, 3) == X.shape

    # The default single aggregation method gives the tag-names as columns
    assert list(X.columns) == ["Tag 1", "Tag 2", "Tag 3"]

    # Using two aggregation methods give a multi-level column with tag-names
    # on top and aggregation_method as second level
    X, _ = TimeSeriesDataset(aggregation_methods=["mean", "max"], **kwargs).get_data()

    assert (83, 6) == X.shape
    assert list(X.columns) == [
        ("Tag 1", "mean"),
        ("Tag 1", "max"),
        ("Tag 2", "mean"),
        ("Tag 2", "max"),
        ("Tag 3", "mean"),
        ("Tag 3", "max"),
    ]


def test_metadata_statistics():
    """Tests that it works to set aggregation method(s)"""

    kwargs = dict(
        data_provider=MockDataProvider(),
        tag_list=[
            SensorTag("Tag 1", None),
            SensorTag("Tag 2", None),
            SensorTag("Tag 3", None),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
    )

    # Default aggregation gives no extra columns
    dataset = TimeSeriesDataset(**kwargs)
    X, _ = dataset.get_data()
    assert (83, 3) == X.shape
    metadata = dataset.get_metadata()
    assert isinstance(metadata["x_hist"], dict)
    assert len(metadata["x_hist"].keys()) == 3


def test_time_series_no_resolution():
    kwargs = dict(
        data_provider=MockDataProvider(),
        tag_list=[
            SensorTag("Tag 1", None),
            SensorTag("Tag 2", None),
            SensorTag("Tag 3", None),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
    )

    no_resolution, _ = TimeSeriesDataset(resolution=None, **kwargs).get_data()
    wi_resolution, _ = TimeSeriesDataset(resolution="10T", **kwargs).get_data()
    assert len(no_resolution) > len(wi_resolution)


@pytest.mark.parametrize(
    "tag_list",
    [
        [SensorTag("Tag 1", None), SensorTag("Tag 2", None), SensorTag("Tag 3", None)],
        [SensorTag("Tag 1", None)],
    ],
)
@pytest.mark.parametrize(
    "target_tag_list",
    [
        [SensorTag("Tag 2", None), SensorTag("Tag 1", None), SensorTag("Tag 3", None)],
        [SensorTag("Tag 1", None)],
        [SensorTag("Tag10", None)],
        [],
    ],
)
def test_timeseries_target_tags(tag_list, target_tag_list):
    start = dateutil.parser.isoparse("2017-12-25 06:00:00Z")
    end = dateutil.parser.isoparse("2017-12-29 06:00:00Z")
    tsd = TimeSeriesDataset(
        start,
        end,
        tag_list=tag_list,
        target_tag_list=target_tag_list,
        data_provider=MockDataProvider(),
    )
    X, y = tsd.get_data()

    assert len(X) == len(y)

    # If target_tag_list is empty, it defaults to tag_list
    if target_tag_list:
        assert y.shape[1] == len(target_tag_list)
    else:
        assert y.shape[1] == len(tag_list)

    # Ensure the order in maintained
    assert [tag.name for tag in target_tag_list or tag_list] == y.columns.tolist()

    # Features should match the tag_list
    assert X.shape[1] == len(tag_list)

    # Ensure the order in maintained
    assert [tag.name for tag in tag_list] == X.columns.tolist()


class MockDataProvider(GordoBaseDataProvider):
    def __init__(self, value=None, n_rows=None, **kwargs):
        """With value argument for generating different types of data series (e.g. NaN)"""
        self.value = value
        self.n_rows = n_rows
        self.last_tag_list = None

    def can_handle_tag(self, tag):
        return True

    def load_series(
        self,
        train_start_date: datetime,
        train_end_date: datetime,
        tag_list: List[SensorTag],
        dry_run: Optional[bool] = False,
        **kwargs,
    ) -> Iterable[pd.Series]:
        self.last_tag_list = tag_list
        index = pd.date_range(train_start_date, train_end_date, freq="s")
        for i, name in enumerate(sorted([tag.name for tag in tag_list])):
            # If value not passed, data for each tag are staggered integer ranges
            data = [self.value if self.value else i for i in range(i, len(index) + i)]
            series = pd.Series(index=index, data=data, name=name)
            yield series[: self.n_rows] if self.n_rows else series


def test_timeseries_dataset_compat():
    """
    There are accepted keywords in the config file when using type: TimeSeriesDataset
    which don't actually match the kwargs of the dataset's __init__; for compatibility
    :func:`gordo_dataset.datasets.compat` should adjust for these differences.
    """
    dataset = TimeSeriesDataset(
        data_provider=MockDataProvider(),
        train_start_date="2017-12-25 06:00:00Z",
        train_end_date="2017-12-29 06:00:00Z",
        tags=[SensorTag("Tag 1", None)],
    )
    assert dataset.train_start_date == dateutil.parser.isoparse("2017-12-25 06:00:00Z")
    assert dataset.train_end_date == dateutil.parser.isoparse("2017-12-29 06:00:00Z")
    assert dataset.tag_list == [SensorTag("Tag 1", None)]


@pytest.mark.parametrize("n_samples_threshold, filter_value", [(10, 5000), (0, 100)])
def test_insufficient_data_after_row_filtering(n_samples_threshold, filter_value):
    """
    Test that dataframe after row_filter scenarios raise appropriate
    InsufficientDataError
    """

    kwargs = dict(
        data_provider=MockDataProvider(),
        tag_list=[
            SensorTag("Tag 1", "asset"),
            SensorTag("Tag 2", "asset"),
            SensorTag("Tag 3", "asset"),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
        n_samples_threshold=n_samples_threshold,
        asset="asset",
    )

    with pytest.raises(InsufficientDataError):
        TimeSeriesDataset(row_filter=f"`Tag 1` < {filter_value}", **kwargs).get_data()


@pytest.mark.parametrize(
    "n_samples_threshold, high_threshold, low_threshold",
    [(10, 5000, -1000), (0, 100, 0)],
)
def test_insufficient_data_after_global_filtering(
    n_samples_threshold, high_threshold, low_threshold
):
    """
    Test that dataframe after row_filter scenarios raise appropriate
    InsufficientDataError
    """

    kwargs = dict(
        data_provider=MockDataProvider(),
        tag_list=[
            SensorTag("Tag 1", None),
            SensorTag("Tag 2", None),
            SensorTag("Tag 3", None),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
        n_samples_threshold=n_samples_threshold,
        high_threshold=high_threshold,
        low_threshold=low_threshold,
    )

    with pytest.raises(InsufficientDataError):
        TimeSeriesDataset(**kwargs).get_data()


def test_insufficient_data_after_known_filter_periods_filtering():
    """
    Test that dataframe after row_filter scenarios raise appropriate
    InsufficientDataError
    """

    kwargs = dict(
        data_provider=MockDataProvider(),
        tag_list=[
            SensorTag("Tag 1", None),
            SensorTag("Tag 2", None),
            SensorTag("Tag 3", None),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
        n_samples_threshold=10,
        known_filter_periods=[
            "~('2017-12-25 07:00:00+00:00' <= index <= '2017-12-29 06:00:00+00:00')"
        ],
    )

    with pytest.raises(InsufficientDataError):
        TimeSeriesDataset(**kwargs).get_data()


def test_insufficient_data_after_automatic_filtering():
    """
    Test that dataframe after row_filter scenarios raise appropriate
    InsufficientDataError
    """

    kwargs = dict(
        data_provider=MockDataProvider(),
        tag_list=[
            SensorTag("Tag 1", None),
            SensorTag("Tag 2", None),
            SensorTag("Tag 3", None),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
        n_samples_threshold=84,
        filter_periods={"filter_method": "median"},
    )

    with pytest.raises(InsufficientDataError):
        TimeSeriesDataset(**kwargs).get_data()


def test_trigger_tags():
    data_provider = MockDataProvider()
    dataset = TimeSeriesDataset(
        data_provider=data_provider,
        tag_list=[
            SensorTag("Tag 1", "asset"),
            SensorTag("Tag 2", "asset"),
        ],
        target_tag_list=[
            SensorTag("Tag 5", "asset"),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
        row_filter="`Tag 3` > 0 & `Tag 4` > 1",
        asset="asset",
    )
    X, y = dataset.get_data()
    assert X is not None
    assert y is not None
    assert set(data_provider.last_tag_list) == {
        SensorTag("Tag 1", "asset"),
        SensorTag("Tag 2", "asset"),
        SensorTag("Tag 3", "asset"),
        SensorTag("Tag 4", "asset"),
        SensorTag("Tag 5", "asset"),
    }
    assert set(X.columns.values) == {"Tag 1", "Tag 2"}
    assert set(y.columns.values) == {"Tag 5"}


def test_get_dataset_with_full_import():
    dataset = _get_dataset(
        {
            "type": "gordo_dataset.datasets.RandomDataset",
            "train_start_date": "2017-12-25 06:00:00Z",
            "train_end_date": "2017-12-29 06:00:00Z",
            "tag_list": [SensorTag("Tag 1", None), SensorTag("Tag 2", None)],
        }
    )
    assert type(dataset) is RandomDataset


def test_process_metadata():
    data_provider = MockDataProvider()
    dataset = TimeSeriesDataset(
        data_provider=data_provider,
        tag_list=[
            SensorTag("Tag 1", None),
            SensorTag("Tag 2", None),
        ],
        target_tag_list=[
            SensorTag("Tag 5", None),
        ],
        train_start_date=dateutil.parser.isoparse("2017-12-25 06:00:00Z"),
        train_end_date=dateutil.parser.isoparse("2017-12-29 06:00:00Z"),
        row_filter="`Tag 3` > 0 & `Tag 4` > 1",
        process_metadata=False,
        asset="asset",
    )
    dataset.get_data()
    assert dataset._metadata == {}


class DatasetForTest(GordoBaseDataset):
    @capture_args
    def __init__(self):
        super(DatasetForTest, self).__init__()

    def get_data(
        self,
    ) -> Tuple[
        Union[np.ndarray, pd.DataFrame, xr.DataArray],
        Union[np.ndarray, pd.DataFrame, xr.DataArray],
    ]:
        return np.array([]), np.array([])


def test_legacy_to_dict():
    dataset = RandomDataset(
        "2017-12-25 06:00:00Z",
        "2017-12-29 06:00:00Z",
        [SensorTag("Tag 1", None), SensorTag("Tag 2", None)],
    )
    config = dataset.to_dict()
    assert config["type"] == "RandomDataset"


def test_to_dict_for_test_dataset():
    dataset = DatasetForTest()
    config = dataset.to_dict()
    assert config["type"] == "tests.test_dataset.DatasetForTest"
