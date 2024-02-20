import time
from utils.decorator_utils import DecoratorUtils
import pytest
import logging

def test_timing_decorator_records_time():
    @DecoratorUtils.timing_decorator
    def test_func(sleep_time):
        """A test function that simply sleeps for the specified amount of time."""
        time.sleep(sleep_time)

    test_func(0.1)
    test_func(0.2)

    func_id = f"{test_func.__module__}.DecoratorUtils.test_func"
    assert func_id in DecoratorUtils.performance_data
    data = DecoratorUtils.performance_data[func_id]

    assert data['calls'] == 2
    assert 0.1 <= min(data['times']) <= 0.2
    assert sum(data['times']) >= 0.3  # Combined sleep time

def test_calculate_statistics_aggregates_correctly():
    @DecoratorUtils.timing_decorator
    def test_func(sleep_time):
        """A test function that simply sleeps for the specified amount of time."""
        time.sleep(sleep_time)

    test_func(0.1)
    test_func(0.2)

    func_id = f"{test_func.__module__}.DecoratorUtils.test_func"
    
    stats = DecoratorUtils.calculate_statistics()

    assert func_id in stats

    stat = stats[func_id]
    assert stat['calls'] == 2
    assert stat['total_time'] >= 0.3
    assert 0.15 <= stat['average_time'] <= 0.2
    assert 0.1 <= stat['p99_time'] <= 0.2

@pytest.fixture(autouse=True)
def reset_performance_data():
    """A fixture to reset the performance data before each test function is run."""
    DecoratorUtils.performance_data = {}

def test_exception_logging_decorator_handles_exceptions(caplog):
    caplog.set_level(logging.ERROR)

    @DecoratorUtils.exception_logging_decorator
    def test_func_raise_exception():
        """A test function that raises an exception."""
        raise ValueError("Test exception")

    error_message = test_func_raise_exception()
    assert "Error in 'test_func_raise_exception': Test exception" in caplog.text
    assert "Error in 'test_func_raise_exception': Test exception" == error_message

def test_exception_logging_decorator_returns_value():
    @DecoratorUtils.exception_logging_decorator
    def test_func_return_value():
        """A test function that returns a value."""
        return "Success"

    result = test_func_return_value()
    assert result == "Success"