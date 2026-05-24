from __future__ import annotations

import os

import trino


def get_trino_connection() -> trino.dbapi.Connection:
    host = os.getenv("TRINO_HOST", "trino")
    port = int(os.getenv("TRINO_PORT", "8080"))
    user = os.getenv("TRINO_USER", "airflow")

    return trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
    )


def execute_trino_query(query: str) -> list:
    conn = get_trino_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()


def create_trino_schema(
    catalog: str,
    schema: str,
    location: str,
) -> None:
    query = f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
    WITH (
        location = '{location}'
    )
    """

    execute_trino_query(query)


def format_columns(columns: list[dict]) -> str:
    return ",\n        ".join(
        f"{column['name']} {column['type']}"
        for column in columns
    )


def create_external_table(
    catalog: str,
    schema: str,
    table: str,
    columns: list[dict],
    external_location: str,
    file_format: str = "PARQUET",
) -> None:
    columns_sql = format_columns(columns)

    query = f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
        {columns_sql}
    )
    WITH (
        external_location = '{external_location}',
        format = '{file_format}'
    )
    """

    execute_trino_query(query)


def validate_table_count(
    catalog: str,
    schema: str,
    table: str,
) -> dict:
    query = f"""
    SELECT COUNT(*) AS total_rows
    FROM {catalog}.{schema}.{table}
    """

    result = execute_trino_query(query)

    return {
        "table": f"{catalog}.{schema}.{table}",
        "total_rows": result[0][0] if result else 0,
    }