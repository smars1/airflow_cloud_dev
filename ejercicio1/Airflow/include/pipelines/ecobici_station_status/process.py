from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from urllib.request import urlopen

import boto3
import polars as pl
from botocore.exceptions import ClientError

from include.common import schema_validation as sv
from include.pipelines.ecobici_station_status.config import (
    DEFAULT_FEED_NAME,
    DEFAULT_GBFS_URL,
    DEFAULT_LANGUAGE,
)
from include.pipelines.ecobici_station_status.schemas import FINAL_SCHEMA


logger = logging.getLogger(__name__)


def get_s3_client():
    endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minio")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio1234")
    region_name = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name,
    )


def read_json_from_url(url: str) -> dict:
    logger.info(f"Reading JSON from url: {url}")

    with urlopen(url, timeout=30) as response:
        payload = response.read().decode("utf-8")

    return json.loads(payload)


def resolve_feed_url(
    gbfs_url: str,
    language: str,
    feed_name: str,
) -> str:
    gbfs_payload = read_json_from_url(gbfs_url)

    feeds = gbfs_payload["data"][language]["feeds"]

    for feed in feeds:
        if feed["name"] == feed_name:
            return feed["url"]

    raise ValueError(f"Feed not found: {feed_name}")


def object_exists(
    bucket_name: str,
    object_name: str,
) -> bool:
    s3_client = get_s3_client()

    try:
        s3_client.head_object(
            Bucket=bucket_name,
            Key=object_name,
        )
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False

        raise


def read_existing_parquet_from_s3(
    bucket_name: str,
    object_name: str,
) -> pl.DataFrame | None:
    if not object_exists(bucket_name, object_name):
        logger.info("Existing parquet was not found in bronze")
        return None

    logger.info(f"Reading existing parquet from bucket: {bucket_name}")
    logger.info(f"Reading existing parquet object: {object_name}")

    s3_client = get_s3_client()

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_name,
    )

    body = response["Body"]

    try:
        data = body.read()
    finally:
        body.close()

    return pl.read_parquet(BytesIO(data))


def add_missing_columns(
    df: pl.DataFrame,
    required_columns: list[str],
) -> pl.DataFrame:
    expressions = []

    for column_name in required_columns:
        if column_name not in df.columns:
            expressions.append(pl.lit(None).alias(column_name))

    if not expressions:
        return df

    return df.with_columns(expressions)


def build_station_status_df(payload: dict) -> pl.DataFrame:
    stations = payload["data"]["stations"]

    feed_last_updated = datetime.fromtimestamp(
        payload["last_updated"],
        tz=timezone.utc,
    ).replace(tzinfo=None)

    ingestion_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

    required_columns = [
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "is_installed",
        "is_renting",
        "is_returning",
        "last_reported",
    ]

    df = pl.DataFrame(stations)
    df = add_missing_columns(df, required_columns)

    df = df.with_columns(
        [
            pl.col("station_id").cast(pl.Utf8),
            pl.col("num_bikes_available").cast(pl.Int64, strict=False),
            pl.col("num_docks_available").cast(pl.Int64, strict=False),
            pl.col("is_installed").cast(pl.Int64, strict=False),
            pl.col("is_renting").cast(pl.Int64, strict=False),
            pl.col("is_returning").cast(pl.Int64, strict=False),
            pl.from_epoch(
                pl.col("last_reported").cast(pl.Int64, strict=False),
                time_unit="s",
            )
            .cast(pl.Datetime)
            .alias("last_reported"),
            pl.lit(feed_last_updated).cast(pl.Datetime).alias("feed_last_updated"),
            pl.lit(ingestion_timestamp).cast(pl.Datetime).alias("ingestion_timestamp"),
            pl.lit(ingestion_timestamp.date()).cast(pl.Date).alias("extraction_date"),
        ]
    )

    df = df.with_columns(
        [
            (
                pl.col("num_bikes_available").is_null()
                | pl.col("num_docks_available").is_null()
                | (pl.col("num_bikes_available") < 0)
                | (pl.col("num_docks_available") < 0)
            ).alias("is_invalid_availability"),
            (
                (pl.col("is_installed") != 1)
                | (pl.col("is_renting") != 1)
                | (pl.col("is_returning") != 1)
            ).alias("is_station_unavailable"),
        ]
    )

    return df.select(list(FINAL_SCHEMA.keys()))


def append_single_parquet(
    new_df: pl.DataFrame,
    bucket_name: str,
    object_name: str,
) -> pl.DataFrame:
    existing_df = read_existing_parquet_from_s3(
        bucket_name=bucket_name,
        object_name=object_name,
    )

    if existing_df is None:
        return new_df.sort(["feed_last_updated", "station_id"])

    return (
        pl.concat([existing_df, new_df], how="vertical_relaxed")
        .unique(subset=["station_id", "feed_last_updated"])
        .sort(["feed_last_updated", "station_id"])
    )


def process_station_status(
    output_path: str,
    bronze_bucket: str,
    bronze_object_path: str,
    gbfs_url: str = DEFAULT_GBFS_URL,
    language: str = DEFAULT_LANGUAGE,
    feed_name: str = DEFAULT_FEED_NAME,
) -> dict:
    logger.info("Starting ECOBICI station status pipeline")
    logger.info(f"GBFS url: {gbfs_url}")
    logger.info(f"Language: {language}")
    logger.info(f"Feed name: {feed_name}")
    logger.info(f"Output path: {output_path}")
    logger.info(f"Bronze bucket: {bronze_bucket}")
    logger.info(f"Bronze object path: {bronze_object_path}")

    sv.validate_parent_dir_exists(
        file_path=output_path,
        path_name="output_path",
    )

    station_status_url = resolve_feed_url(
        gbfs_url=gbfs_url,
        language=language,
        feed_name=feed_name,
    )

    logger.info(f"Resolved feed url: {station_status_url}")

    payload = read_json_from_url(station_status_url)

    new_df = build_station_status_df(payload)

    final_df = append_single_parquet(
        new_df=new_df,
        bucket_name=bronze_bucket,
        object_name=bronze_object_path,
    )

    sv.validate_schema(
        df=final_df,
        expected_schema=FINAL_SCHEMA,
        schema_name="ecobici_station_status_final_schema",
        allow_extra_columns=False,
    )

    logger.info(f"Writing parquet file to: {output_path}")
    logger.info(f"Rows new: {new_df.height}")
    logger.info(f"Rows output: {final_df.height}")

    final_df.write_parquet(output_path)

    if not os.path.exists(output_path):
        raise FileNotFoundError(f"Parquet file was not created: {output_path}")

    return {
        "status": "success",
        "feed_url": station_status_url,
        "rows_new": new_df.height,
        "rows_output": final_df.height,
        "output_path": output_path,
    }