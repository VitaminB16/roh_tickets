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


def load_json(path):
    """
    Load a json file
    """
    if not os.path.isfile(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def write_json(path, data):
    """
    Write a json file
    """
    with open(path, "w") as f:
        json.dump(data, f, indent=3)
