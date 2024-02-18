import os
import json
import asyncio
import collections.abc

from cloud.platform import PLATFORM

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

    def load(self, allow_empty=True, PLATFORM=PLATFORM):
        """
        Load a json file as a dict
        """
        print("Loading", self.path)
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
                        print("Max attempts reached!")
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


def enforce_one_schema(df_col, col_schema):
    """
    Enforce a schema on a dataframe
    """
    if isinstance(col_schema, type(lambda x: x)):
        # e.g. lambda x: x.strip()
        df_col = col_schema(df_col)
    elif isinstance(col_schema, dict):
        # e.g. {"a": int, "b": str}
        df_col = df_col.map(col_schema)
    elif isinstance(col_schema, type):
        # e.g. int, str, float
        df_col = df_col.astype(col_schema)
    elif isinstance(col_schema, (list)):
        # If it is a list of possible schemas, try each one until one works
        for schema in col_schema:
            try:
                df_col = enforce_one_schema(df_col, schema)
                break
            except Exception:
                pass
    return df_col


def enforce_schema(df, schema):
    """
    Enforce a schema on a dataframe
    """
    if schema is None:
        return df
    for col, col_schema in schema.items():
        if col not in df.columns:
            df[col] = None
        df[col] = enforce_one_schema(df[col], col_schema)
    return df


def purge_image_cache(repo_url="https://github.com/VitaminB16/roh_tickets"):
    """
    Purge the image cache of a github repository. This is useful when the image is updated and the old one is still cached.
    """
    import requests
    from bs4 import BeautifulSoup

    repo_html = requests.get(repo_url).text
    soup = BeautifulSoup(repo_html, "html.parser")
    image_elements = soup.find_all("img")
    # Extract all urls
    image_urls = [el["src"] for el in image_elements if "src" in el.attrs]
    image_urls.extend([el["srcset"] for el in image_elements if "srcset" in el.attrs])
    image_urls = list(set(image_urls))
    for image_url in image_urls:
        try:
            response = requests.request("PURGE", image_url)
            print(f"Purged {image_url} - {response.status_code}")
        except Exception as e:
            pass
    return


if __name__ == "__main__":
    purge_image_cache()
