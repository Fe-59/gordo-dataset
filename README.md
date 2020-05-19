# Gordo dataset

gordo dataset library essential to build datasets and data providers for [gordo](https://github.com/equinor/gordo) projects.

## Usage

### Data provider

Extend [GordoBaseDataProvider](gordo_dataset/data_provider/base.py) to adapt it to your data source.

See examples [NcsReader](gordo_data_set/data_provider/ncs_reader.py) that reads either parquet or csv files from Azure Datalake v1.

### Dataset

Extend [GordoBaseDataset](gordo_dataset/base.py).

See example for [TimeSeriesDataset](gordo_dataset/datasets.py) that arranges the data into consecutive times series.

### Install

`pip install gordo-dataset`

### Uninstall

`pip uninstall gordo-datset`
