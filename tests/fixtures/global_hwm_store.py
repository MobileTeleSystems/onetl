import pytest
from etl_entities.hwm_store import MemoryHWMStore


@pytest.fixture(scope="function", autouse=True)
def global_hwm_store(request):  # noqa: WPS325
    test_function = request.function
    entities = set(test_function.__name__.split("_")) if test_function else set()

    if "strategy" in entities:
        with MemoryHWMStore():
            yield None

    else:
        yield None
