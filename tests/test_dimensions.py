from gordo_dataset.dimensions import get_data_dimensions


def test_get_data_dimensions():
    assert get_data_dimensions(3) == ["data_0", "data_1", "data_2"]
