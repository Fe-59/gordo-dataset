# -*- coding: utf-8 -*-

import abc
import importlib

from datetime import datetime
from copy import copy
from typing import Iterable, List, Optional

import pandas as pd

from gordo_dataset.sensor_tag import SensorTag
from gordo_dataset.exceptions import ConfigException


class GordoBaseDataProvider(object):
    @abc.abstractmethod
    def load_series(
        self,
        train_start_date: datetime,
        train_end_date: datetime,
        tag_list: List[SensorTag],
        dry_run: Optional[bool] = False,
        **kwargs,
    ) -> Iterable[pd.Series]:
        """
        Load the required data as an iterable of series where each
        contains the values of the tag with time index

        Parameters
        ----------
        train_start_date: datetime
            Datetime object representing the start of fetching data
        train_end_date: datetime
            Datetime object representing the end of fetching data
        tag_list: List[SensorTag]
            List of tags to fetch, where each will end up being its own dataframe
        dry_run: Optional[bool]
            Set to true to perform a "dry run" of the loading.
            Up to the implementations to determine what that means.
        kwargs: Dict
            With these - additional data might be passed by data_provider.

        Returns
        -------
        Iterable[pd.Series]
        """
        ...

    @abc.abstractmethod
    def can_handle_tag(self, tag: SensorTag):
        """
        Returns true if the dataprovider thinks it can possibly read this tag.
        Typically checks if the asset part of the tag is known to the reader.

        Parameters
        ----------
        tag: SensorTag - Dictionary with a "tag" key and optional "asset"

        Returns
        -------
        bool

        """
        ...

    @abc.abstractmethod
    def to_dict(self):
        """
        Serialize this object into a dict representation, which can be used to
        initialize a new object after popping 'type' from the dict.

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
        params_type = ""
        if hasattr(self, "__module__"):
            # Keep back-compatibility
            if self.__module__ != "gordo_dataset.data_provider.providers":
                params_type = self.__module__ + "."
        params_type += self.__class__.__name__
        params["type"] = params_type
        return params

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, config: dict) -> "GordoBaseDataProvider":
        provider_type = "DataLakeProvider"
        if "type" in config:
            config = copy(config)
            provider_type = config.pop("type")

        module = None
        if "." in provider_type:
            module_name, class_name = provider_type.rsplit(".", 1)

            # TODO validate module_name
            try:
                module = importlib.import_module(module_name)
            except ImportError as e:
                raise ConfigException(
                    f"Unable to import module '{module_name}': {str(e)}"
                )
        else:
            from gordo_dataset.data_provider import providers

            module_name, class_name = "gordo_dataset.data_provider", provider_type
            module = providers

        try:
            Provider = getattr(module, class_name)
        except AttributeError:
            raise ConfigException(
                f"Unable to find data provider '{class_name}' in module {module_name}"
            )

        return Provider(**config)
