import os
import uuid
import asyncio
import pandas as pd
import pyarrow as pa
from retrying import retry
import pyarrow.parquet as pq
from typing import Any, List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor

from python_roh.src.utils import force_list


class Parquet:
    """
    Writes a DataFrame to a parquet file using pyarrow and asyncio.
    It allows for a better naming schema of the parquet partitions.
    """

    def __init__(self, path: str, schema: pa.Schema = None, **kwargs):
        self.path = path
        self.schema = schema
        self.executor = ThreadPoolExecutor(max_workers=os.cpu_count())

    def write(
        self, df: pd.DataFrame, partition_cols: List[str] = None, **kwargs: Any
    ) -> Tuple[Dict[str, str], Dict[str, bool]]:
        if partition_cols:
            asyncio.run(self._write_partitions(df, partition_cols, **kwargs))
        else:
            df.to_parquet(self.path, index=False, engine="pyarrow", **kwargs)
        return True

    async def _write_partitions(self, df, partition_cols, **kwargs):
        df_without_partition_cols = df.drop(columns=partition_cols)
        tasks = []
        for grp, _df in df.groupby(partition_cols, sort=False):
            path = self._construct_partition_path(grp, partition_cols)
            task = asyncio.create_task(
                self._to_parquet_async(_df[partition_cols], path, **kwargs)
            )
            tasks.append(task)
        await asyncio.gather(*tasks)

    def _construct_partition_path(self, grp, partition_cols):
        grp = (grp,) if not isinstance(grp, tuple) else grp
        path_parts = [self.path] + [
            f"{col}={val}" for col, val in zip(partition_cols, grp)
        ]
        filename = self.partition_name_func(grp)
        full_path = os.path.join(*path_parts, filename).replace(" ", "_")
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        return full_path

    @staticmethod
    def partition_name_func(keys, add_uuid: bool = False) -> str:
        filename = (
            "_".join(map(str, keys))
            + (f"_{uuid.uuid4()}" if add_uuid else "")
            + ".parquet"
        )
        return filename

    async def _to_parquet_async(self, df, path, **kwargs):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor,
            to_parquet,
            df,
            path,
            "pyarrow",
            "gzip",
            False,
            **kwargs,
        )

    def read(
        self,
        allow_empty=True,
        schema=None,
        filters=None,
        **kwargs,
    ):
        filters = self.generate_filters(filters)
        self.log(filters)
        df = pq.read_table(
            self.path,
            filters=filters,
            schema=schema,
            **kwargs,
        ).to_pandas()

        if df.empty and not allow_empty:
            raise ValueError(f"File {self.path} is empty, and allow_empty is False")

        df = self.fix_column_types(df, filters)
        return df

    def generate_filters(self, filters):
        """Generate the filters"""
        if filters is None:
            return None
        if isinstance(filters, list):
            return filters

        file_filters = []
        for column, value in filters.items():
            value = force_list(value)
            file_filters.append((column, "in", value))
        return file_filters

    def fix_column_types(self, df, filters):
        """
        Ensure nothing strange happens with the column types of the df
        """
        if (not filters) or df.empty:
            return df
        for c, c_type in [(x[0], type(x[1])) for x in filters]:
            df[c] = df[c].astype(c_type)
        return df


@retry(wait_fixed=0.1, stop_max_attempt_number=1000)
def to_parquet(
    df,
    path: str,
    engine: str = "pyarrow",
    compression: str = "gzip",
    index: bool = False,
):
    df.to_parquet(path, engine=engine, compression=compression, index=index)
