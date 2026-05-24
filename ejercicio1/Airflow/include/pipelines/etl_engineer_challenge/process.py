import logging

import polars as pl

from include.common.minio_client import read_object_as_bytesio
from include.common import schema_validation as sv
from include.common import quality_rules as qr
from include.pipelines.etl_engineer_challenge.config import (
    DATE_FORMATS,
    MAX_VALID_AMOUNT,
    QUALITY_FLAG_COLUMNS,
    VALID_STATUS,
)
from include.pipelines.etl_engineer_challenge.schemas import (
    EXPECTED_COLUMNS,
    FINAL_SCHEMA,
)


logger = logging.getLogger(__name__)


def read_transactions_from_s3_compatible_storage(
    bucket_name: str,
    object_name: str,
) -> pl.DataFrame:
    logger.info(f"Reading CSV from bucket: {bucket_name}")
    logger.info(f"Reading CSV object: {object_name}")

    csv_buffer = read_object_as_bytesio(
        bucket_name=bucket_name,
        object_name=object_name,
    )

    return pl.read_csv(csv_buffer)


def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {
        col: col.strip().lower().replace(" ", "_")
        for col in df.columns
    }
    return df.rename(rename_map)


def aggregate_transactions(df: pl.DataFrame) -> pl.DataFrame:
    valid_df = df.filter(
        pl.col("name").is_not_null()
        & pl.col("created_at").is_not_null()
    )

    return (
        valid_df
        .group_by(["name", "created_at"])
        .agg([
            pl.len().alias("total_transactions"),
            pl.col("amount_clean").count().alias("valid_amount_transactions"),
            pl.col("is_invalid_amount").sum().alias("invalid_amount_transactions"),
            pl.col("amount_clean").sum().alias("total_amount"),
            pl.col("amount_clean").mean().alias("avg_amount"),
            pl.col("amount_clean").min().alias("min_amount"),
            pl.col("amount_clean").max().alias("max_amount"),
            pl.col("id").n_unique().alias("unique_customers"),
            pl.col("company_id").n_unique().alias("unique_companies"),
            pl.col("is_invalid_id").sum().alias("invalid_id_count"),
            pl.col("is_invalid_status").sum().alias("invalid_status_count"),
            pl.col("is_paid_before_created").sum().alias("paid_before_created_count"),
        ])
        .sort(["created_at", "name"])
    )


def process_transactions(
    landing_bucket: str,
    landing_object_path: str,
    output_path: str,
    observed_output_path: str | None = None,
) -> dict:
    logger.info("Starting ETL process from S3 compatible storage")
    logger.info(f"Landing bucket: {landing_bucket}")
    logger.info(f"Landing object path: {landing_object_path}")
    logger.info(f"Writing master Parquet file to: {output_path}")

    sv.validate_parent_dir_exists(
        file_path=output_path,
        path_name="output_path",
    )

    df = read_transactions_from_s3_compatible_storage(
        bucket_name=landing_bucket,
        object_name=landing_object_path,
    )

    df = normalize_column_names(df)

    sv.validate_schema(
        df=df,
        expected_schema=EXPECTED_COLUMNS,
        schema_name="input_schema",
        allow_extra_columns=False,
    )

    rows_input = df.height

    # continua la logica de limpieza, calidad, agregacion y escritura

    df = qr.clean_empty_strings(df)
    df, empty_rows_removed = qr.remove_empty_rows(df)

    df = qr.parse_date_column_with_formats(
        df=df,
        column_name="created_at",
        formats=DATE_FORMATS,
    )

    df = qr.parse_date_column_with_formats(
        df=df,
        column_name="paid_at",
        formats=DATE_FORMATS,
    )

    df = qr.normalize_text_column(df, "name")
    df = qr.normalize_text_column(df, "status")

    df = qr.add_null_quality_flags(
        df=df,
        columns=["id", "name", "company_id"],
    )

    df = qr.add_id_quality_flag(df)
    df = qr.add_status_quality_flag(df, VALID_STATUS)
    df = qr.add_date_quality_flags(df)
    df = qr.add_amount_quality_flags(
        df=df,
        max_valid_amount=MAX_VALID_AMOUNT,
    )
    df = qr.add_amount_clean_column(
        df=df,
        max_valid_amount=MAX_VALID_AMOUNT,
    )

    rows_before_dedup = df.height
    df = df.unique()
    rows_after_dedup = df.height

    observed_records = qr.filter_observed_records(
        df=df,
        quality_flag_columns=QUALITY_FLAG_COLUMNS,
    )

    if observed_output_path:
        sv.validate_parent_dir_exists(
            observed_output_path,
            "observed_output_path",
        )
        observed_records.write_parquet(observed_output_path)

    aggregated_df = aggregate_transactions(df)

    sv.validate_schema(
        df=aggregated_df,
        expected_schema=FINAL_SCHEMA,
        schema_name="final_schema",
        allow_extra_columns=False,
    )

    aggregated_df.write_parquet(output_path)

    return {
        "status": "success",
        "rows_input": rows_input,
        "empty_rows_removed": empty_rows_removed,
        "rows_before_dedup": rows_before_dedup,
        "rows_after_dedup": rows_after_dedup,
        "rows_observed": observed_records.height,
        "rows_output": aggregated_df.height,
        "output_path": output_path,
        "observed_output_path": observed_output_path,
    }