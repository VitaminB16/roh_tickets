import os
import pandas as pd


class BasePlatform:
    def __init__(self):
        self.fs = None
        self.fs_prefix = ""


def parquet_filters_to_sql(filters):
    """
    Convert parquet filters to SQL
    """
    if filters is None:
        return None
    if isinstance(filters, list):
        output = [parquet_filters_to_sql(x) for x in filters]
        output = " AND ".join(output)
        return output
    col, op, value = filters
    if isinstance(value, list):
        value = ", ".join([f"'{x}'" for x in value])
        value = f"({value})"
    return f"{col} {op} {value}"


class GCPPlatform(BasePlatform):
    """Google Cloud Platform specific methods"""

    def __init__(self):

        self._fs = None
        self.name = "GCP"
        self.fs_prefix = "gs://"

    @property
    def fs(self):
        """Return the GCS file system object to overcome the fork-safety issue with gcsfs."""
        if self._fs is None:
            import gcsfs

            self._fs = gcsfs.GCSFileSystem()
        return self._fs

    def open(self, path, mode, allow_empty=False, **kwargs):
        if allow_empty and not self.exists(path):
            return None
        return self.fs.open(path, mode, **kwargs)

    def makedirs(self, path, exist_ok=True):
        self.fs.makedirs(path, exist_ok=exist_ok)

    def exists(self, path):
        return self.fs.exists(path)

    def read_table(self, table=None, filters=None, allow_empty=False, **kwargs):
        query = f"SELECT * FROM {table}"
        if filters:
            filters = parquet_filters_to_sql(filters)
            query += f" WHERE {filters}"
        try:
            df = pd.read_gbq(query)
        except:
            if allow_empty:
                return pd.DataFrame()
            raise
        return df

    def create_table(self, table, df, **kwargs):
        df.to_gbq(table, if_exists="replace", **kwargs)

    def insert_rows(self, df, table, if_exists="append", **kwargs):
        df.to_gbq(table, if_exists=if_exists, **kwargs)


class LocalPlatform(BasePlatform):
    """Local file system specific methods"""

    def __init__(self):
        self.fs = None
        self.name = "Local"
        self.fs_prefix = ""

    def open(self, path, mode, allow_empty=False, **kwargs):
        if allow_empty and not self.exists(path):
            return None
        return open(path, mode)

    def makedirs(self, path, exist_ok=True):
        os.makedirs(path, exist_ok=exist_ok)

    def isfile(self, path):
        return os.path.isfile(path)

    def exists(self, path):
        return os.path.exists(path)


def Platform():
    """Return the platform object based on the PLATFORM environment variable. Defaults to local."""
    platform_name = os.getenv("PLATFORM", "local").lower()
    platform = {
        "local": LocalPlatform,
        "gcp": GCPPlatform,
    }.get(platform_name)
    return platform()


# Initialised platform object
PLATFORM = Platform()
