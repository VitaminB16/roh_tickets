import os
import json
import asyncio
import collections.abc

LIST_LIKE_TYPES = (list, tuple, set, frozenset, collections.abc.KeysView)


def force_list(x):
    """
    Force x to be a list
    """
    if isinstance(x, LIST_LIKE_TYPES):
        return x
    else:
        return [x]


def jprint(x):
    """
    Pretty print a json object
    """
    print(json.dumps(x, indent=3))


class JSON:
    """
    Class for operating json files
    """

    def __init__(self, path):
        self.path = path

    def load(self):
        """
        Load a json file as a dict
        """
        if not os.path.isfile(self.path):
            return {}
        with open(self.path, "r") as f:
            return json.load(f)

    def write(self, data, **kwargs):
        """
        Write a json file
        """
        kwargs.setdefault("indent", 3)
        kwargs.setdefault("sort_keys", True)
        with open(self.path, "w") as f:
            json.dump(data, f, **kwargs)


def async_retry(wait_fixed=0.1, stop_max_attempt_number=3):
    """
    Decorator to retry async function execution.

    Args:
        stop_max_attempt_number: Number of retry attempts.
        wait_fixed: Fixed waiting time.
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(stop_max_attempt_number):
                try:
                    return await func(*args, **kwargs)
                except KeyError:
                    if attempt < stop_max_attempt_number - 1:
                        await asyncio.sleep(wait_fixed)
                    else:
                        print("Max attempts reached!")
                        raise  # If last attempt, raise the exception

        return wrapper

    return decorator
