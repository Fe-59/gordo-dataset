import pytest

from datetime import datetime
from gordo_dataset.base import GordoBaseDataset
from gordo_dataset.datasets import TimeSeriesDataset
from gordo_dataset.sensor_tag import SensorTag

from dateutil.tz import tzutc


def test_from_dict():
    train_start_date = datetime(2020, 1, 1, tzinfo=tzutc())
    train_end_date = datetime(2020, 3, 1, tzinfo=tzutc())
    tag_list = [SensorTag("tag1", "asset"), SensorTag("tag2", "asset")]

    config = {
        "type": "TimeSeriesDataset",
        "train_start_date": train_start_date,
        "train_end_date": train_end_date,
        "tag_list": tag_list,
    }
    dataset = GordoBaseDataset.from_dict(config)
    assert type(dataset) is TimeSeriesDataset
    assert dataset.train_start_date == train_start_date
    assert dataset.train_end_date == train_end_date
    assert dataset.tag_list == tag_list


def test_from_dict_with_empty_type():
    train_start_date = datetime(2020, 1, 1, tzinfo=tzutc())
    train_end_date = datetime(2020, 3, 1, tzinfo=tzutc())
    tag_list = [SensorTag("tag1", "asset"), SensorTag("tag2", "asset")]

    config = {
        "train_start_date": train_start_date,
        "train_end_date": train_end_date,
        "tag_list": tag_list,
    }
    dataset = GordoBaseDataset.from_dict(config)
    assert type(dataset) is TimeSeriesDataset
    assert dataset.train_start_date == train_start_date
    assert dataset.train_end_date == train_end_date
    assert dataset.tag_list == tag_list


def test_to_dict():
    train_start_date = datetime(2020, 1, 1, tzinfo=tzutc())
    train_end_date = datetime(2020, 3, 1, tzinfo=tzutc())
    tag_list = [SensorTag("tag1", "asset"), SensorTag("tag2", "asset")]

    dataset = TimeSeriesDataset(
        train_start_date=train_start_date,
        train_end_date=train_end_date,
        tag_list=tag_list,
    )
    config = dataset.to_dict()
    assert config["train_start_date"] == "2020-01-01T00:00:00+00:00"
    assert config["train_end_date"] == "2020-03-01T00:00:00+00:00"
    assert config["tag_list"] == tag_list
    assert config["type"] == "gordo_dataset.datasets.TimeSeriesDataset"
