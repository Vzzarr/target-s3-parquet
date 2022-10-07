"""S3Parquet target sink class, which handles writing streams."""


from typing import Dict, List, Optional
import awswrangler as wr
import pandas as pd
import pyarrow
from pandas import DataFrame
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
import json
from target_s3_parquet.data_type_generator import (
    generate_tap_schema,
    generate_current_target_schema,
)
from target_s3_parquet.sanitizer import (
    get_specific_type_attributes,
    apply_json_dump_to_df,
    stringify_df,
)


from datetime import datetime

STARTED_AT = datetime.now()


def stringify_schema(df: pd.DataFrame, schema_properties: dict) -> pd.DataFrame:
    attributes_names = get_specific_type_attributes(schema_properties, "object")
    df_transformed = apply_json_dump_to_df(df, attributes_names)
    return stringify_df(df_transformed)


class S3ParquetSink(BatchSink):
    """S3Parquet target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self._glue_schema = self._get_glue_schema()

    def _get_glue_schema(self):

        catalog_params = {
            "database": self.config.get("athena_database"),
            "table": self.stream_name,
        }

        if wr.catalog.does_table_exist(**catalog_params):
            return wr.catalog.table(**catalog_params)
        else:
            return DataFrame()

    max_size = 100  # Max records to write in one batch

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

        df = DataFrame(context["records"])

        partition_column = self.config.get('partition_column')
        part_cols = []
        if partition_column:
            df['date'] = pd.to_datetime(df[partition_column], format='%Y-%m-%d').dt.date
            part_cols.append('date')
        else:
            df["_sdc_started_at"] = STARTED_AT.timestamp()
            part_cols.append('_sdc_started_at')
        schema = json.loads(self.config.get('catalog'))

        # current_schema = generate_current_target_schema(self._get_glue_schema())
        self.logger.info("The self.schema is:")
        self.logger.info(self.schema)
        self.logger.info("The schema is:")
        self.logger.info(schema)
        dtype = generate_tap_schema(schema["properties"], only_string=self.config.get("stringify_schema"))
        dtype_cleaned = {}
        for k, v in dtype.items():
            dtype_cleaned[k] = v.replace(" ", "")

        self.logger.info(f"DType Definition: {dtype_cleaned}")

        # dtype = {**current_schema, **tap_schema}

        if self.config.get("stringify_schema"):
            df = stringify_schema(df, schema["properties"])

        full_path = f"{self.config.get('s3_path')}/{self.config.get('athena_database')}/{self.stream_name}"
        try:
            wr.s3.to_parquet(
                df=df,
                index=False,
                compression="snappy",
                dataset=True,
                path=full_path,
                mode="append",
                partition_cols=part_cols,
                schema_evolution=True,
                dtype=dtype_cleaned,
            )
        except pyarrow.lib.ArrowTypeError as e:
            self.logger.error("Type Mismatch Exception raised:")
            self.logger.error(e)
            df = stringify_schema(df, schema["properties"])
            dtype = generate_tap_schema(schema["properties"], only_string=True)
            discarded_path = f"{self.config.get('s3_path')}/{self.config.get('athena_database')}/{self.stream_name}/discarded/"
            wr.s3.to_parquet(
                df=df,
                index=False,
                compression="snappy",
                dataset=True,
                path=discarded_path,
                mode="append",
                partition_cols=part_cols,
                schema_evolution=True,
                dtype=dtype,
            )

        self.logger.info(f"Uploaded {len(context['records'])}")

        context["records"] = []
