import functools
import inspect

from collections import namedtuple
import logging
from typing import Iterable, Union, List, Callable, Dict, Optional, Tuple
from datetime import datetime

import pandas as pd
import numpy as np

from influxdb import DataFrameClient, InfluxDBClient
from .exceptions import InsufficientDataError


def capture_args(method: Callable):
    """
    Decorator that captures args and kwargs passed to a given method.
    This assumes the decorated method has a self, which has a dict of
    kwargs assigned as an attribute named _params.

    Parameters
    ----------
    method: Callable
        Some method of an object, with 'self' as the first parameter.

    Returns
    -------
    Any
        Returns whatever the original method would return
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):

        sig_params = inspect.signature(method).parameters.items()

        # Get the default values for the method signature
        params = {
            param: value.default
            for param, value in sig_params
            if value.default is not inspect.Parameter.empty and param != "self"
        }

        # Update with arg values provided
        arg_map = dict()
        for arg_val, arg_key in zip(
            args, (arg for arg in inspect.getfullargspec(method).args if arg != "self")
        ):
            arg_map[arg_key] = arg_val

        # Update params with args/kwargs provided in the current call
        params.update(arg_map)
        params.update(kwargs)

        self._params = params
        return method(self, *args, **kwargs)

    return wrapper


# Prediction result representation, name=str, predictions=dataframe, error_messages=List[str]
PredictionResult = namedtuple("PredictionResult", "name predictions error_messages")


def _parse_influx_uri(uri: str) -> Tuple[str, str, str, str, str, str]:
    """
    Parse an influx URI

    Parameters
    ----------
    uri: str
        Format: <username>:<password>@<host>:<port>/<optional-path>/<db_name>

    Returns
    -------
    (str, str, str, str, str, str)
        username, password, host, port, path, database
    """
    username, password, host, port, *path, db_name = (
        uri.replace("/", ":").replace("@", ":").split(":")
    )
    path_str = "/".join(path) if path else ""
    return username, password, host, port, path_str, db_name


def influx_client_from_uri(
    uri: str,
    api_key: Optional[str] = None,
    api_key_header: Optional[str] = "Ocp-Apim-Subscription-Key",
    recreate: bool = False,
    dataframe_client: bool = False,
    proxies: Dict[str, str] = {"https": "", "http": ""},
) -> Union[InfluxDBClient, DataFrameClient]:
    """
    Get a InfluxDBClient or DataFrameClient from a SqlAlchemy like URI

    Parameters
    ----------
    uri: str
        Connection string format: <username>:<password>@<host>:<port>/<optional-path>/<db_name>
    api_key: str
        Any api key required for the client connection
    api_key_header: str
        The name of the header the api key should be assigned
    recreate: bool
        Re/create the database named in the URI
    dataframe_client: bool
        Return a DataFrameClient instead of a standard InfluxDBClient
    proxies: dict
        A mapping of any proxies to pass to the influx client

    Returns
    -------
    Union[InfluxDBClient, DataFrameClient]
    """

    username, password, host, port, path, db_name = _parse_influx_uri(uri)

    Client = DataFrameClient if dataframe_client else InfluxDBClient

    client = Client(
        host=host,
        port=port,
        database=db_name,
        username=username,
        password=password,
        path=path,
        ssl=bool(api_key),
        proxies=proxies,
    )
    if api_key:
        client._headers[api_key_header] = api_key
    if recreate:
        client.drop_database(db_name)
        client.create_database(db_name)
    return client


def join_timeseries(
    series_iterable: Iterable[pd.Series],
    resampling_startpoint: datetime,
    resampling_endpoint: datetime,
    resolution: str,
    aggregation_methods: Union[str, List[str], Callable] = "mean",
    interpolation_method: str = "linear_interpolation",
    interpolation_limit: str = "8H",
) -> Tuple[pd.DataFrame, dict]:
    """

    Parameters
    ----------
    series_iterable: Iterable[pd.Series]
        An iterator supplying series with time index
    resampling_startpoint: datetime.datetime
        The starting point for resampling. Most data frames will not have this
        in their datetime index, and it will be inserted with a NaN as the value.
        The resulting NaNs will be removed, so the only important requirement for this is
        that this resampling_startpoint datetime must be before or equal to the first
        (earliest) datetime in the data to be resampled.
    resampling_endpoint: datetime.datetime
        The end point for resampling. This datetime must be equal to or after the last datetime in the
        data to be resampled.
    resolution: str
        The bucket size for grouping all incoming time data (e.g. "10T")
        Available strings come from https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
    aggregation_methods: Union[str, List[str], Callable]
        Aggregation method(s) to use for the resampled buckets. If a single
        resample method is provided then the resulting dataframe will have names
        identical to the names of the series it got in. If several
        aggregation-methods are provided then the resulting dataframe will
        have a multi-level column index, with the series-name as the first level,
        and the aggregation method as the second level.
        See :py:func::`pandas.core.resample.Resampler#aggregate` for more
        information on possible aggregation methods.
    interpolation_method: str
        How should missing values be interpolated. Either forward fill (`ffill`) or by linear
        interpolation (default, `linear_interpolation`).
    interpolation_limit: str
        Parameter sets how long from last valid data point values will be interpolated/forward filled.
        Default is eight hours (`8H`).
        If None, all missing values are interpolated/forward filled.

    Returns
    -------
    pd.DataFrame
        A dataframe without NaNs, a common time index, and one column per
        element in the dataframe_generator. If multiple aggregation methods
        are provided then the resulting dataframe will have a multi-level column
        index with series-names as top-level and aggregation-method as second-level.
    dict
        Matadata information

    """
    resampled_series = []
    missing_data_series = []
    metadata = dict()

    for series in series_iterable:
        metadata[series.name] = dict(original_length=len(series))
        try:
            resampled = _resample(
                series,
                resampling_startpoint=resampling_startpoint,
                resampling_endpoint=resampling_endpoint,
                resolution=resolution,
                aggregation_methods=aggregation_methods,
                interpolation_method=interpolation_method,
                interpolation_limit=interpolation_limit,
            )
        except IndexError:
            missing_data_series.append(series.name)
        else:
            resampled_series.append(resampled)
            metadata[series.name].update(dict(resampled_length=len(resampled)))
    if missing_data_series:
        raise InsufficientDataError(
            f"The following features are missing data: {missing_data_series}"
        )

    joined_df = pd.concat(resampled_series, axis=1, join="inner")

    # Before returning, delete all rows with NaN, they were introduced by the
    # insertion of NaNs in the beginning of all timeseries
    dropped_na = joined_df.dropna()

    metadata["aggregate_metadata"] = dict(
        joined_length=len(joined_df), dropped_na_length=len(dropped_na)
    )
    return dropped_na, metadata


def _resample(
    series: pd.Series,
    resampling_startpoint: datetime,
    resampling_endpoint: datetime,
    resolution: str,
    aggregation_methods: Union[str, List[str], Callable] = "mean",
    interpolation_method: str = "linear_interpolation",
    interpolation_limit: str = "8H",
):
    """
    Takes a single series and resamples it.
    See :class:`gordo_dataset.base.GordoBaseDataset.join_timeseries`
    """

    startpoint_sametz = resampling_startpoint.astimezone(tz=series.index[0].tzinfo)
    endpoint_sametz = resampling_endpoint.astimezone(tz=series.index[0].tzinfo)

    if series.index[0] > startpoint_sametz:
        # Insert a NaN at the startpoint, to make sure that all resampled
        # indexes are the same. This approach will "pad" most frames with
        # NaNs, that will be removed at the end.
        startpoint = pd.Series([np.NaN], index=[startpoint_sametz], name=series.name)
        series = startpoint.append(series)
        logging.debug(f"Appending NaN to {series.name} " f"at time {startpoint_sametz}")

    elif series.index[0] < resampling_startpoint:
        msg = (
            f"Error - for {series.name}, first timestamp "
            f"{series.index[0]} is before the resampling start point "
            f"{startpoint_sametz}"
        )
        logging.error(msg)
        raise RuntimeError(msg)

    if series.index[-1] < endpoint_sametz:
        endpoint = pd.Series([np.NaN], index=[endpoint_sametz], name=series.name)
        series = series.append(endpoint)
        logging.debug(f"Appending NaN to {series.name} " f"at time {endpoint_sametz}")
    elif series.index[-1] > endpoint_sametz:
        msg = (
            f"Error - for {series.name}, last timestamp "
            f"{series.index[-1]} is later than the resampling end point "
            f"{endpoint_sametz}"
        )
        logging.error(msg)
        raise RuntimeError(msg)

    logging.debug("Head (3) and tail(3) of dataframe to be resampled:")
    logging.debug(series.head(3))
    logging.debug(series.tail(3))

    resampled = series.resample(resolution, label="left").agg(aggregation_methods)
    # If several aggregation methods are provided, agg returns a dataframe
    # instead of a series. In this dataframe the column names are the
    # aggregation methods, like "max" and "mean", so we have to make a
    # multi-index with the series-name as the top-level and the
    # aggregation-method as the lower-level index.
    # For backwards-compatibility we *dont* return a multi-level index
    # when we have a single resampling method.
    if isinstance(resampled, pd.DataFrame):  # Several aggregation methods provided
        resampled.columns = pd.MultiIndex.from_product(
            [[series.name], resampled.columns], names=["tag", "aggregation_method"]
        )

    if interpolation_method not in ["linear_interpolation", "ffill"]:
        raise ValueError(
            "Interpolation method should be either linear_interpolation of ffill"
        )

    if interpolation_limit is not None:
        limit = int(
            pd.Timedelta(interpolation_limit).total_seconds()
            / pd.Timedelta(resolution).total_seconds()
        )

        if limit <= 0:
            raise ValueError("Interpolation limit must be larger than given resolution")
    else:
        limit = None

    if interpolation_method == "linear_interpolation":
        return resampled.interpolate(limit=limit).dropna()

    else:
        return resampled.fillna(method=interpolation_method, limit=limit).dropna()
