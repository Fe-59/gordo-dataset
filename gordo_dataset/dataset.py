# -*- coding: utf-8 -*-
import importlib
from .exceptions import ConfigException


def _get_dataset(config):
    """
    Return a GordoBaseDataSet object of a certain type, given a config dict
    """
    dataset_config = dict(config)
    kind = dataset_config.pop("type", "")
    if "." in kind:
        module_name, class_name = kind.rsplit(".", 1)
        # TODO validate module_name
        module = importlib.import_module(module_name)
        if not hasattr(module, class_name):
            raise ConfigException(
                "Unable to find class %s in module %s" % (module_name, class_name)
            )
        Dataset = getattr(module, class_name)
    else:
        import gordo_dataset.datasets as datasets

        if not kind:
            kind = "TimeSeriesDataset"
        if not hasattr(datasets, kind):
            raise ConfigException(
                "Unable to find class %s in module gordo_dataset.datasets" % kind
            )
        Dataset = getattr(datasets, kind)
    if Dataset is None:
        raise ConfigException(f'Dataset type "{kind}" is not supported!')

    return Dataset(**dataset_config)
