import os
import json
import asyncio
import collections.abc

from cloud.utils import log
from cloud.platform import PLATFORM
from python_roh.src.config import PYTHON_ROH_REPO_URL

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
    log(json.dumps(x, indent=3))


def is_series(obj):
    """Check if an object is a pandas Series without importing pandas."""
    return type(obj).__name__ == "Series"


def is_dataframe(obj):
    """Check if an object is a pandas DataFrame without importing pandas."""
    return type(obj).__name__ == "DataFrame"


class JSON:
    """
    Class for operating json files
    """

    def __init__(self, path):
        self.path = path

    def load(self, allow_empty=True, PLATFORM=PLATFORM):
        """
        Load a json file as a dict
        """
        log("Loading", self.path)
        if allow_empty and not PLATFORM.exists(self.path):
            return {}
        with PLATFORM.open(self.path, "r") as f:
            return json.load(f)

    def write(self, data, **kwargs):
        """
        Write a json file
        """
        kwargs.setdefault("indent", 3)
        kwargs.setdefault("sort_keys", True)
        PLATFORM.makedirs(os.path.dirname(self.path), exist_ok=True)
        with PLATFORM.open(self.path, "w") as f:
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
                        log("Max attempts reached!")
                        raise  # If last attempt, raise the exception

        return wrapper

    return decorator


def ensure_types(df, types_dict):
    """
    Ensure the types of the columns of the df
    """
    for c, c_type in types_dict.items():
        if c in df.columns:
            df[c] = df[c].astype(c_type)
    return df


def dtype_str_to_type(dtype_str):
    """
    Map a string representation of a dtype to its corresponding Python type
    or return the string if it's a Pandas-specific type.
    """
    python_types = {
        "int": int,
        "int64": int,
        "float": float,
        "float64": float,
        "str": str,
        "bool": bool,
    }
    return python_types.get(dtype_str, dtype_str)


def enforce_schema_on_series(series, schema):
    """Enforce a schema on a pandas Series."""
    if callable(schema):
        # For callable schemas, apply directly
        return schema(series)
    elif isinstance(schema, dict):
        # For dictionary schemas, use map (assuming intention is to map values based on keys)
        return series.map(lambda x: schema.get(x, x))
    elif isinstance(schema, (type, str)):
        # For type schemas, cast the Series to the specified type
        return series.astype(schema)
    else:
        raise TypeError("Unsupported schema type.")


def enforce_schema_on_list(lst, schema):
    """Enforce a schema on a list."""
    if callable(schema):
        return [schema(x) for x in lst]
    elif isinstance(schema, dict):
        return [schema.get(x, x) for x in lst]
    elif isinstance(schema, type):
        return [schema(x) for x in lst]
    elif isinstance(schema, str):
        schema = dtype_str_to_type(schema)
        for i, x in enumerate(lst):
            try:
                lst[i] = schema(x)
            except ValueError:
                pass
        return lst
    else:
        raise TypeError("Unsupported schema type.")


def enforce_one_schema(data, col_schema):
    """
    Enforce a schema on a dataframe column or a list of data.
    Args:
    - data (Series|List): The data to enforce the schema on.
    - col_schema (type|dict|callable|list): The schema to enforce.
    """

    if isinstance(col_schema, list):
        # Attempt to enforce each schema in the list until one succeeds
        for schema in col_schema:
            try:
                return enforce_one_schema(data, schema)
            except Exception:
                continue
        else:
            raise ValueError(f"Could not enforce schema {col_schema} on {data}")
    else:
        if is_series(data):
            return enforce_schema_on_series(data, col_schema)
        else:
            data = force_list(data)
            return enforce_schema_on_list(data, col_schema)


def enforce_schema(df, schema={}, dtypes={}, errors="raise"):
    """
    Enforce a schema on a dataframe or dictionary
    """
    if schema is None:
        schema = {}
    schema = {**dtypes, **schema}  # schema takes precedence over dtypes
    if schema == {}:
        return df
    for col, col_schema in schema.items():
        if col not in df:
            df[col] = None
        try:
            df[col] = enforce_one_schema(df[col], col_schema)
        except Exception as e:
            log(f"Error enforcing schema {col_schema} on {col}: {e}")
            if errors == "raise":
                raise e
    return df


def purge_image_cache(repo_url=PYTHON_ROH_REPO_URL):
    """
    Purge the image cache of a github repository. This is useful when the image is updated and the old one is still cached.
    """
    import requests
    from bs4 import BeautifulSoup

    # Get all images from the repository
    repo_html = requests.get(repo_url, headers={"User-Agent": "Mozilla/5.0"}).text
    soup = BeautifulSoup(repo_html, "html.parser")
    image_elements = soup.find_all("img")
    # Extract all urls
    image_urls = [el["src"] for el in image_elements if "src" in el.attrs]
    image_urls.extend([el["srcset"] for el in image_elements if "srcset" in el.attrs])
    image_urls = list(set(image_urls))
    image_urls = [url for url in image_urls if "http" in url]
    for image_url in image_urls:
        try:
            response = requests.request("PURGE", image_url)
            log(f"Purged {image_url} - {response.status_code}")
        except Exception as e:
            log(f"Failed to purge {image_url} - {e}")
    return


def run_command(command):
    """
    Utility function to run a shell command and return its output, with error handling.
    """
    import subprocess

    result = subprocess.run(command, capture_output=True, text=True, check=True)
    if result.returncode != 0:
        raise Exception(
            f"Command '{' '.join(command)}' failed with error: {result.stderr}"
        )
    return result.stdout.split("\n")


# def install_font_on_gcf(font_directory="/tmp", font_path=None):
#     """
#     Install a font on Google Cloud Functions
#     """

#     log("Current fonts:")
#     for i in range(30):
#         log(1)

#     current_fonts = run_command(["fc-list"])

#     log("Install the font")
#     run_command(["fc-cache", "-f", "-v", "/tmp/"])

#     log("New fonts:")
#     new_fonts = run_command(["fc-list"])
#     log(new_fonts)

#     new_fonts = [f for f in new_fonts if f not in current_fonts]
#     log(new_fonts)
#     return


def rgb_to_hex(rgb):
    """
    [rgb(0,0,0), rgb(255,255,255)] -> ['#000000', '#ffffff']
    """
    rgb = [x.replace("rgb", "").replace(")", "").replace("(", "") for x in rgb]
    rgb = [x.split(",") for x in rgb]
    rgb = [[int(x) for x in y] for y in rgb]
    rgb = ["#" + "".join([f"{x:02x}" for x in y]) for y in rgb]
    return rgb


def hex_to_rgb(hex):
    """
    ['#000000', '#ffffff'] -> [rgb(0,0,0), rgb(255,255,255)]
    """
    hex = [x.replace("#", "") for x in hex]
    hex = [[int(x[i : i + 2], 16) for i in (0, 2, 4)] for x in hex]
    hex = [f"rgb({x[0]},{x[1]},{x[2]})" for x in hex]
    return hex


if __name__ == "__main__":
    purge_image_cache()
