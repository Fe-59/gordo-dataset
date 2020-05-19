import functools
import inspect

from typing import Callable, Tuple, Union, Optional, Dict
from collections import namedtuple

from influxdb import DataFrameClient, InfluxDBClient


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
