import os
import uuid
import glob
import asyncio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from urllib.parse import unquote
from typing import Any, List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor

from cloud.utils import log
from python_roh.src.utils import force_list, async_retry, enforce_schema
from python_roh.src.config import (
    PARQUET_SCHEMAS,
    PYARROW_SCHEMAS,
    PLATFORM,
    PARQUET_TABLE_RELATIONS,
    PRODUCTIONS_PARQUET_LOCATION,
)


class Parquet:
    """
    Writes a DataFrame to a parquet file using pyarrow and asyncio.
    It allows for a better naming schema of the parquet partitions, handling special characters.
    """

    def __init__(self, path: str, **kwargs):
        self.path = path
        self.executor = ThreadPoolExecutor(max_workers=os.cpu_count())

    def write(
        self,
        df: pd.DataFrame,
        partition_cols: List[str] = None,
        add_uuid: bool = False,
        schema: pa.Schema = None,
        use_threading: bool = False,
        **kwargs: Any,
    ) -> Tuple[Dict[str, str], Dict[str, bool]]:
        log(f"Writing to {self.path}")
        if partition_cols and use_threading:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            tasks = []
            with self.executor as executor:
                for grp, _df in df.groupby(partition_cols, sort=False):
                    task = loop.create_task(
                        self._write_partition(
                            grp,
                            _df,
                            partition_cols,
                            add_uuid,
                            schema,
                            **kwargs,
                        )
                    )
                    tasks.append(task)
                loop.run_until_complete(asyncio.gather(*tasks))
            loop.close()

        elif partition_cols:
            pq.write_to_dataset(
                table=pa.Table.from_pandas(df),
                root_path=self.path,
                partition_cols=partition_cols,
                schema=schema,
                basename_template="{i}.parquet",
                **kwargs,
            )
        else:
            df.to_parquet(
                self.path,
                index=False,
                engine="pyarrow",
                **kwargs,
            )

        return True

    @async_retry(wait_fixed=0.1, stop_max_attempt_number=1000)
    async def _write_partition(
        self, grp, _df, partition_cols, add_uuid, schema, **kwargs
    ):
        loop = asyncio.get_running_loop()
        if not isinstance(grp, tuple):
            grp = (grp,)
        path_parts = [self.path]
        for col, val in zip(partition_cols, grp):
            sanitized_val = self.sanitise_name(val)
            path_parts.append(f"{col}={sanitized_val}")
        path = "/".join(path_parts)
        os.makedirs(path, exist_ok=True)
        path = path + "/" + self.partition_name_func(grp, add_uuid=add_uuid)
        _df.drop(columns=partition_cols, errors="ignore", inplace=True)
        import time

        await loop.run_in_executor(
            self.executor,
            to_parquet,
            _df,
            path,
            "pyarrow",
            "gzip",
            False,
            schema,
            **kwargs,
        )

    def sanitise_name(self, name):
        # More selective sanitization to only target problematic characters
        return name.replace("/", "%2F")

    def partition_name_func(self, keys, add_uuid: bool = False) -> str:
        filename = (
            "_".join(map(str, keys))
            + (f"_{uuid.uuid4()}" if add_uuid else "")
            + ".parquet"
        )
        filename = self.sanitise_name(filename)
        return filename

    def read(
        self,
        allow_empty=True,
        schema=None,
        filters=None,
        use_bigquery=False,
        columns=None,
        read_partitions_only=False,
        **kwargs,
    ):
        print_str = f"Reading from {self.path}; filters: {filters}; use_bigquery: {use_bigquery}"
        if read_partitions_only:
            print_str += "; read_partitions_only: True"
        log(print_str)
        filters = self.generate_filters(filters)
        if use_bigquery and PLATFORM.name != "Local":
            # Obtains the data from BigQuery using external table on the Parquet
            table = PARQUET_TABLE_RELATIONS.get(self.path, None)
            df = PLATFORM.read_table(
                table=table,
                filters=filters,
                columns=columns,
                allow_empty=allow_empty,
            )
        elif read_partitions_only:
            # Only reads the partitions of the Parquet file using the filenames
            df = self.get_partitions_df()
        else:
            # Actually reads the Parquet file from storage
            schema = PYARROW_SCHEMAS.get(self.path, schema)
            try:
                df = pq.read_table(
                    self.path,
                    filters=filters,
                    schema=schema,
                    **kwargs,
                ).to_pandas()
            except FileNotFoundError:
                df = pd.DataFrame()
        if df.empty and not allow_empty:
            raise ValueError(
                f"File {self.path} is empty or not found, and allow_empty is False"
            )

        df = self.fix_column_types(df, filters)

        enforced_schema = PARQUET_SCHEMAS.get(self.path, None)
        df = enforce_schema(df, enforced_schema)
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

    def fix_column_types(self, df, filters, replace_underscore=True):
        """
        Ensure nothing strange happens with the column types of the df
        """
        if (not filters) or df.empty:
            return df
        for c, c_type in [(x[0], type(x[1])) for x in filters]:
            df[c] = df[c].astype(c_type)
            if replace_underscore and (c_type == str):
                df[c] = df[c].str.replace("_", " ")
        return df

    def get_partitions_df(self):
        """
        Get the partitions of the parquet file
        """
        all_partition_paths = self.get_all_partition_paths()
        all_partitions = [x.split("/") for x in all_partition_paths]
        all_partitions = [
            dict(y.split("=") for y in x if "=" in y) for x in all_partitions
        ]
        df = pd.DataFrame(all_partitions)
        df = df.map(unquote)
        return df

    def get_all_partition_paths(self):
        """
        Get the partitions of the parquet file
        """
        partition_cols = self.get_partition_cols()
        glob_query = os.path.join(self.path, *["*"] * len(partition_cols))
        all_partitions = PLATFORM.glob(glob_query)
        return all_partitions

    def get_partition_cols(self):
        """
        Get the partitions of the parquet file without reading the files
        """
        glob_query = os.path.join(self.path, "*")
        all_paths = PLATFORM.glob(glob_query)
        while all_paths:
            glob_query = all_paths[0] + "/*"
            all_paths = PLATFORM.glob(glob_query)
        glob_query = glob_query.split("/")
        partition_path = [x.split("=")[0] for x in glob_query if "=" in x]
        return partition_path


def to_parquet(
    df,
    path: str,
    engine: str = "pyarrow",
    compression: str = "gzip",
    index: bool = False,
    schema: pa.Schema = None,
):
    df.to_parquet(
        path,
        engine=engine,
        compression=compression,
        index=index,
        schema=schema,
    )


if __name__ == "__main__":
    Parquet(PRODUCTIONS_PARQUET_LOCATION).read(
        columns=["productionId", "title", "date", "time", "performanceId"],
        allow_empty=True,
        read_partitions_only=True,
    )
