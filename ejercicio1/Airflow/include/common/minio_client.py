from __future__ import annotations

import os
from io import BytesIO

import boto3
from minio import Minio
from minio.error import S3Error


def get_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minio")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio1234")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


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


def read_object_as_bytesio(
    bucket_name: str,
    object_name: str,
) -> BytesIO:
    s3_client = get_s3_client()

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_name,
    )

    data = response["Body"].read()

    return BytesIO(data)


def create_bucket_if_not_exists(bucket_name: str) -> None:
    client = get_minio_client()

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def upload_file_to_minio(
    bucket_name: str,
    object_name: str,
    file_path: str,
) -> None:
    client = get_minio_client()

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path,
    )


def download_file_from_minio(
    bucket_name: str,
    object_name: str,
    file_path: str,
) -> None:
    client = get_minio_client()

    try:
        client.fget_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
        )
    except S3Error as exc:
        raise RuntimeError(
            f"Error downloading object {object_name} from bucket {bucket_name}"
        ) from exc