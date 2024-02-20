import time
import functools
from typing import Callable, Dict, List, Union, Any
import logging

class DecoratorUtils:
    """
    A utility class for performance monitoring of methods. It provides a class
    method decorator to measure execution time of methods and aggregates
    performance statistics including call count, total execution time, and
    percentile times such as p99.
    """
    performance_data: Dict[str, Dict[str, Union[List[float], int, float]]] = {}
    logger: logging.Logger = logging.getLogger(__name__)

    @classmethod
    def timing_decorator(cls, func: Callable) -> Callable:
        """
        A decorator to measure the execution time of methods. It wraps a method
        to add timing and logging functionality.

        Args:
            func (Callable): The function to be wrapped by the timing decorator.

        Returns:
            Callable: A wrapper function that adds timing functionality to the input function.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Generate a unique identifier for the function using its module, class, and name.
            func_id = f"{func.__module__}.{cls.__name__}.{func.__name__}"

            start_time = time.time()
            cls.logger.debug(f"Starting '{func_id}' at {start_time}")
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            cls.logger.debug(f"Ending '{func_id}' at {end_time}, duration: {elapsed_time}s")
            
            # Initialize and update performance data.
            if func_id not in cls.performance_data:
                cls.performance_data[func_id] = {'calls': 0, 'total_time': 0.0, 'times': []}
            cls.performance_data[func_id]['calls'] += 1
            cls.performance_data[func_id]['total_time'] += elapsed_time
            cls.performance_data[func_id]['times'].append(elapsed_time)

            return result
        return wrapper

    @classmethod
    def calculate_statistics(cls) -> Dict[str, Dict[str, Union[int, float]]]:
        """
        Calculates and logs the performance statistics for all methods decorated
        with the timing_decorator. Computes the total call count, total execution
        time, average execution time, and the 99th percentile execution time for
        each method.

        Returns:
            Dict[str, Dict[str, Union[int, float]]]: A dictionary containing the
            calculated statistics for each decorated function.
        """
        stats = {}

        for func_name, data in cls.performance_data.items():
            calls = data['calls']
            total_time = data['total_time']
            average_time = total_time / calls
            sorted_times = sorted(data['times'])
            p99_index = max(int(calls * 0.99) - 1, 0)
            p99_time = sorted_times[p99_index]

            stats[func_name] = {
                "calls": calls,
                "total_time": total_time,
                "average_time": average_time,
                "p99_time": p99_time
            }
            
            cls.logger.debug(
                f"'{func_name}': Call count={calls}, Total execution time={total_time:.4f}s, "
                f"Average execution time={average_time:.4f}s, p99 execution time={p99_time:.4f}s"
            )
        
        return stats
    
    @classmethod
    def exception_logging_decorator(cls, func: Callable) -> Callable:
        """
        A decorator that wraps a function to log and return an error message
        if the function raises an exception. If the function executes successfully,
        it returns the function's return value.

        Args:
            func (Callable): The function to be wrapped.

        Returns:
            Callable: A wrapper function that adds exception handling and logging.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Catch the exception，log it and return error message
                error_message = f"Error in '{func.__name__}': {str(e)}"
                cls.logger.error(error_message)
                return error_message
        return wrapper
