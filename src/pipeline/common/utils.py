from pandera.polars import DataFrameSchema
import polars as pl

import pandera as pa


@pa.check_types
def validate_df_schema(df: pl.DataFrame, schema: type[DataFrameSchema]) -> pl.DataFrame:
    return schema.validate(df)  # type: ignore
