# -*- coding: utf-8 -*-
import importlib


def _get_dataset(config):
    """
    Return a GordoBaseDataSet object of a certain type, given a config dict
    """
    dataset_config = dict(config)
    kind = dataset_config.pop("type", "")
    if '.' in kind:
        module_name, class_name = kind.rsplit(".", 1)
        # TODO validate module_name
        Dataset = getattr(importlib.import_module(module_name), class_name)
    else:
        import gordo_dataset.datasets as datasets
        Dataset = getattr(datasets, kind)
    if Dataset is None:
        raise ValueError(f'Dataset type "{kind}" is not supported!')

    return Dataset(**dataset_config)
