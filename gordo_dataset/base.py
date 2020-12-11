# -*- coding: utf-8 -*-

import abc
import logging
from typing import Union, Dict, Any, Tuple
from copy import copy

import pandas as pd
import numpy as np
import xarray as xr


logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    pass


class GordoBaseDataset:
    def __init__(self):
        self._metadata: Dict[Any, Any] = dict()
        # provided by @capture_args on child's __init__
        if not hasattr(self, "_params"):
            self._params = dict()

    @abc.abstractmethod
    def get_data(
        self,
    ) -> Tuple[Union[np.ndarray, pd.DataFrame, xr.Dataset], Union[np.ndarray, pd.DataFrame, xr.Dataset]]:
        """
        Return X, y data as numpy or pandas' dataframes given current state
        """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """
        Serialize this object into a dict representation, which can be used to
        initialize a new object using :func:`~GordoBaseDataset.from_dict`

        Returns
        -------
        dict
                """
        if not hasattr(self, "_params"):
            raise AttributeError(
                "Failed to lookup init parameters, ensure the "
                "object's __init__ is decorated with 'capture_args'"
            )
        # Update dict with the class
        params = self._params
        params["type"] = self.__class__.__name__
        for key, value in params.items():
            if hasattr(value, "to_dict"):
                params[key] = value.to_dict()
        return params

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, config: Dict[str, Any]) -> "GordoBaseDataset":
        """
        Construct the dataset using a config from :func:`~GordoBaseDataset.to_dict`
        """
        from gordo_dataset import datasets

        config = copy(config)
        Dataset = getattr(datasets, config.pop("type", "TimeSeriesDataset"))
        if Dataset is None:
            raise TypeError(f"No dataset of type '{config['type']}'")

        # TODO: Here for compatibility, but @compate should take care of it, remove later
        if "tags" in config:
            config["tag_list"] = config.pop("tags")
        config.setdefault("target_tag_list", config["tag_list"])
        return Dataset(**config)

    @abc.abstractmethod
    def get_metadata(self):
        """
        Get metadata about the current state of the dataset
        """


