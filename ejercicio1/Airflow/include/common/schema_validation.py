from pathlib import Path

import polars as pl


def validate_file_exists(file_path: str, path_name: str = "file_path") -> str:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"{path_name} does not exist: {file_path}")

    if not path.is_file():
        raise ValueError(f"{path_name} is not a file: {file_path}")

    return "success"


def validate_parent_dir_exists(file_path: str, path_name: str = "file_path") -> str:
    path = Path(file_path)
    parent_dir = path.parent

    if not parent_dir.exists():
        raise FileNotFoundError(
            f"Parent directory for {path_name} does not exist: {parent_dir}"
        )

    if not parent_dir.is_dir():
        raise ValueError(
            f"Parent path for {path_name} is not a directory: {parent_dir}"
        )

    return "success"


def validate_schema(
    df: pl.DataFrame,
    expected_schema: dict[str, pl.DataType],
    schema_name: str = "schema",
    allow_extra_columns: bool = True,
) -> str:
    received_schema = dict(zip(df.columns, df.dtypes))
    errors = []

    expected_columns = set(expected_schema.keys())
    received_columns = set(df.columns)

    missing_columns = expected_columns - received_columns
    extra_columns = received_columns - expected_columns

    for column_name in sorted(missing_columns):
        errors.append({
            "column": column_name,
            "error": "missing_column",
            "expected": str(expected_schema[column_name]),
            "received": None,
        })

    if extra_columns and not allow_extra_columns:
        for column_name in sorted(extra_columns):
            errors.append({
                "column": column_name,
                "error": "extra_column",
                "expected": None,
                "received": str(received_schema[column_name]),
            })

    for column_name, expected_dtype in expected_schema.items():
        if column_name not in received_schema:
            continue

        received_dtype = received_schema[column_name]

        if received_dtype != expected_dtype:
            errors.append({
                "column": column_name,
                "error": "invalid_dtype",
                "expected": str(expected_dtype),
                "received": str(received_dtype),
            })

    if errors:
        raise TypeError(f"Invalid {schema_name}: {errors}")

    return "success"