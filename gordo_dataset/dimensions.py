from typing import List

TIME_DIMENSION = "time"


def get_data_dimensions(n_dimensions: int) -> List[str]:
    return ["data_%d" % v for v in range(n_dimensions)]
