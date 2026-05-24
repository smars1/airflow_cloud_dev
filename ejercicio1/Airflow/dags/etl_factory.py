from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from importlib import import_module
from typing import Callable

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from include.common.minio_client import (
    create_bucket_if_not_exists,
    upload_file_to_minio,
)
from include.common.trino_client import (
    create_trino_schema,
    create_external_table,
    validate_table_count,
)


logger = logging.getLogger(__name__)

CONFIG_FOLDER = "/opt/airflow/dags/templates"


def load_json_file(file_path: str) -> dict:
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def parse_start_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def load_pipeline_callable(module_path: str, callable_name: str) -> Callable:
    module = import_module(module_path)

    if not hasattr(module, callable_name):
        raise AirflowException(
            f"Callable {callable_name} not found in module {module_path}"
        )

    return getattr(module, callable_name)


def load_trino_tables_config(config: dict) -> dict:
    trino_config = config["trino"]

    if "tables" in trino_config:
        return config

    tables_config_path = trino_config.get("tables_config_path")

    if not tables_config_path:
        raise AirflowException(
            "Missing trino tables definition. Use 'tables' or 'tables_config_path'."
        )

    tables_config = load_json_file(tables_config_path)

    if "tables" not in tables_config:
        raise AirflowException(
            f"Missing 'tables' key in Trino tables config: {tables_config_path}"
        )

    config["trino"]["tables"] = tables_config["tables"]

    return config


def validate_config(config: dict) -> None:
    required_keys = [
        "dag_id",
        "description",
        "schedule_interval",
        "start_date",
        "catchup",
        "owner",
        "datalake",
        "trino",
        "pipeline",
        "tasks",
    ]

    for key in required_keys:
        if key not in config:
            raise AirflowException(f"Missing required config key: {key}")

    for key in ["module", "callable"]:
        if key not in config["pipeline"]:
            raise AirflowException(f"Missing pipeline config key: {key}")

    datalake_required_keys = [
        "landing_bucket",
        "bronze_bucket",
        "landing_object_path",
        "bronze_object_path",
        "local_input_file",
        "local_work_parquet",
    ]

    for key in datalake_required_keys:
        if key not in config["datalake"]:
            raise AirflowException(f"Missing datalake config key: {key}")

    trino_required_keys = [
        "catalog",
        "schema",
        "schema_location",
        "tables",
    ]

    for key in trino_required_keys:
        if key not in config["trino"]:
            raise AirflowException(f"Missing trino config key: {key}")

    if not isinstance(config["trino"]["tables"], list) or not config["trino"]["tables"]:
        raise AirflowException("The trino key 'tables' must be a non-empty list")

    for table_config in config["trino"]["tables"]:
        for key in ["table", "external_location", "format", "columns"]:
            if key not in table_config:
                raise AirflowException(f"Missing trino table config key: {key}")

        if not isinstance(table_config["columns"], list) or not table_config["columns"]:
            raise AirflowException(
                f"The table {table_config['table']} must have a non-empty columns list"
            )

        for column_config in table_config["columns"]:
            for key in ["name", "type"]:
                if key not in column_config:
                    raise AirflowException(
                        f"Invalid column config in table {table_config['table']}"
                    )

    if not isinstance(config["tasks"], list) or not config["tasks"]:
        raise AirflowException("The config key 'tasks' must be a non-empty list")

    orders = []
    task_ids = []

    for task_config in config["tasks"]:
        for key in ["order", "task_id", "type"]:
            if key not in task_config:
                raise AirflowException(f"Missing task config key: {key}")

        orders.append(task_config["order"])
        task_ids.append(task_config["task_id"])

        if task_config["type"] == "python" and "callable" not in task_config:
            raise AirflowException(
                f"Task {task_config['task_id']} requires callable"
            )

    if len(orders) != len(set(orders)):
        raise AirflowException("Duplicated task order detected")

    if len(task_ids) != len(set(task_ids)):
        raise AirflowException("Duplicated task_id detected")


def build_callable_registry(
    datalake: dict,
    trino_config: dict,
    pipeline_callable: Callable,
) -> dict[str, Callable]:
    def create_minio_buckets_fn():
        create_bucket_if_not_exists(datalake["landing_bucket"])
        create_bucket_if_not_exists(datalake["bronze_bucket"])

        return {
            "landing_bucket": datalake["landing_bucket"],
            "bronze_bucket": datalake["bronze_bucket"],
        }

    def upload_csv_to_landing_fn():
        upload_file_to_minio(
            bucket_name=datalake["landing_bucket"],
            object_name=datalake["landing_object_path"],
            file_path=datalake["local_input_file"],
        )

        return {
            "bucket": datalake["landing_bucket"],
            "object_path": datalake["landing_object_path"],
            "local_file": datalake["local_input_file"],
        }

    def process_with_polars_fn():
        return pipeline_callable(
            landing_bucket=datalake["landing_bucket"],
            landing_object_path=datalake["landing_object_path"],
            output_path=datalake["local_work_parquet"],
            observed_output_path=datalake.get("local_observed_records_parquet"),
        )

    def upload_parquet_to_bronze_fn():
        upload_file_to_minio(
            bucket_name=datalake["bronze_bucket"],
            object_name=datalake["bronze_object_path"],
            file_path=datalake["local_work_parquet"],
        )

        return {
            "bucket": datalake["bronze_bucket"],
            "object_path": datalake["bronze_object_path"],
            "local_file": datalake["local_work_parquet"],
        }

    def upload_observed_records_to_bronze_fn():
        local_file = datalake.get("local_observed_records_parquet")
        object_path = datalake.get("observed_records_object_path")

        if not local_file or not object_path:
            logger.info("Observed records output is not configured")
            return {
                "status": "skipped",
                "reason": "observed records output is not configured",
            }

        if not os.path.exists(local_file):
            logger.info("Observed records file does not exist")
            return {
                "status": "skipped",
                "reason": f"file does not exist: {local_file}",
            }

        upload_file_to_minio(
            bucket_name=datalake["bronze_bucket"],
            object_name=object_path,
            file_path=local_file,
        )

        return {
            "status": "success",
            "bucket": datalake["bronze_bucket"],
            "object_path": object_path,
            "local_file": local_file,
        }

    def create_trino_schema_fn():
        create_trino_schema(
            catalog=trino_config["catalog"],
            schema=trino_config["schema"],
            location=trino_config["schema_location"],
        )

        return {
            "schema": f"{trino_config['catalog']}.{trino_config['schema']}",
            "location": trino_config["schema_location"],
        }

    def create_trino_tables_fn():
        created_tables = []

        for table_config in trino_config["tables"]:
            create_external_table(
                catalog=trino_config["catalog"],
                schema=trino_config["schema"],
                table=table_config["table"],
                columns=table_config["columns"],
                external_location=table_config["external_location"],
                file_format=table_config.get("format", "PARQUET"),
            )

            created_tables.append(
                f"{trino_config['catalog']}."
                f"{trino_config['schema']}."
                f"{table_config['table']}"
            )

        return {
            "created_tables": created_tables,
        }

    def validate_trino_tables_fn():
        validation_results = []

        for table_config in trino_config["tables"]:
            result = validate_table_count(
                catalog=trino_config["catalog"],
                schema=trino_config["schema"],
                table=table_config["table"],
            )
            validation_results.append(result)

        return {
            "validation_results": validation_results,
        }

    return {
        "create_minio_buckets": create_minio_buckets_fn,
        "upload_csv_to_landing": upload_csv_to_landing_fn,
        "process_with_polars": process_with_polars_fn,
        "upload_parquet_to_bronze": upload_parquet_to_bronze_fn,
        "upload_observed_records_to_bronze": upload_observed_records_to_bronze_fn,
        "create_trino_schema": create_trino_schema_fn,
        "create_trino_tables": create_trino_tables_fn,
        "validate_trino_tables": validate_trino_tables_fn,
    }


def create_airflow_task(
    task_config: dict,
    callable_registry: dict[str, Callable],
):
    task_id = task_config["task_id"]
    task_type = task_config["type"]

    if task_type == "empty":
        return EmptyOperator(task_id=task_id)

    if task_type == "python":
        callable_name = task_config["callable"]

        if callable_name not in callable_registry:
            raise AirflowException(
                f"Callable not registered: {callable_name}"
            )

        return PythonOperator(
            task_id=task_id,
            python_callable=callable_registry[callable_name],
        )

    raise AirflowException(f"Unsupported task type: {task_type}")


def create_dag_from_config(config: dict) -> DAG:
    config = load_trino_tables_config(config)
    validate_config(config)

    dag_id = config["dag_id"]
    datalake = config["datalake"]
    trino_config = config["trino"]

    pipeline_callable = load_pipeline_callable(
        module_path=config["pipeline"]["module"],
        callable_name=config["pipeline"]["callable"],
    )

    default_args = {
        "owner": config.get("owner", "airflow"),
        "retries": config.get("default_args", {}).get("retries", 1),
        "retry_delay": timedelta(
            seconds=config.get("default_args", {}).get("retry_delay", 10)
        ),
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=config.get("description", ""),
        schedule=config.get("schedule_interval"),
        start_date=parse_start_date(config["start_date"]),
        catchup=config.get("catchup", False),
        tags=config.get("tags", ["challenge", "etl"]),
    ) as dag:
        callable_registry = build_callable_registry(
            datalake=datalake,
            trino_config=trino_config,
            pipeline_callable=pipeline_callable,
        )

        ordered_task_configs = sorted(
            config["tasks"],
            key=lambda task_config: task_config["order"],
        )

        airflow_tasks = {}

        for task_config in ordered_task_configs:
            task_id = task_config["task_id"]

            airflow_tasks[task_id] = create_airflow_task(
                task_config=task_config,
                callable_registry=callable_registry,
            )

        for index in range(len(ordered_task_configs) - 1):
            current_task_id = ordered_task_configs[index]["task_id"]
            next_task_id = ordered_task_configs[index + 1]["task_id"]

            airflow_tasks[current_task_id] >> airflow_tasks[next_task_id]

    return dag


def discover_pipeline_configs(config_folder: str) -> list[str]:
    config_paths = []

    for root, _, files in os.walk(config_folder):
        for file_name in files:
            if file_name == "pipeline.json":
                config_paths.append(os.path.join(root, file_name))

    return config_paths


for config_path in discover_pipeline_configs(CONFIG_FOLDER):
    config_json = load_json_file(config_path)
    dag = create_dag_from_config(config_json)
    globals()[config_json["dag_id"]] = dag