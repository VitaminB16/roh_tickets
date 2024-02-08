import os
import json
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
