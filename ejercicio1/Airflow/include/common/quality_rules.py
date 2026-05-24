from __future__ import annotations

from functools import reduce

import polars as pl


def clean_empty_strings(df: pl.DataFrame) -> pl.DataFrame:
    string_cols = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if dtype == pl.Utf8
    ]

    return df.with_columns([
        pl.when(
            pl.col(col)
            .str.strip_chars()
            .str.replace_all(r"\s+", " ")
            == ""
        )
        .then(None)
        .otherwise(
            pl.col(col)
            .str.strip_chars()
            .str.replace_all(r"\s+", " ")
        )
        .alias(col)
        for col in string_cols
    ])


def remove_empty_rows(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    rows_before = df.height

    cleaned_df = df.filter(
        ~pl.all_horizontal([
            pl.col(col).is_null()
            for col in df.columns
        ])
    )

    removed_rows = rows_before - cleaned_df.height

    return cleaned_df, removed_rows


def normalize_text_column(
    df: pl.DataFrame,
    column_name: str,
) -> pl.DataFrame:
    if column_name not in df.columns:
        return df

    return df.with_columns([
        pl.col(column_name)
        .str.to_lowercase()
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.replace_all(" ", "_")
        .alias(column_name)
    ])


def parse_date_column_with_formats(
    df: pl.DataFrame,
    column_name: str,
    formats: list[str],
    output_column: str | None = None,
) -> pl.DataFrame:
    if column_name not in df.columns:
        return df

    target_column = output_column or column_name

    parsed_expressions = [
        pl.col(column_name).str.strptime(
            pl.Date,
            fmt,
            strict=False,
        )
        for fmt in formats
    ]

    parsed_column = reduce(
        lambda left, right: left.fill_null(right),
        parsed_expressions,
    )

    return df.with_columns([
        parsed_column.alias(target_column)
    ])


def add_status_quality_flag(
    df: pl.DataFrame,
    valid_status: set[str],
    column_name: str = "status",
) -> pl.DataFrame:
    if column_name not in df.columns:
        return df

    return df.with_columns([
        (
            pl.col(column_name).is_null()
            | ~pl.col(column_name).is_in(list(valid_status))
        ).alias("is_invalid_status")
    ])


def add_id_quality_flag(
    df: pl.DataFrame,
    column_name: str = "id",
) -> pl.DataFrame:
    if column_name not in df.columns:
        return df

    return df.with_columns([
        (
            pl.col(column_name).is_null()
            | (pl.col(column_name) == "*******")
            | (~pl.col(column_name).str.contains(r"^[a-fA-F0-9]{32,}$"))
        ).alias("is_invalid_id")
    ])


def add_amount_quality_flags(
    df: pl.DataFrame,
    amount_column: str = "amount",
    max_valid_amount: float = 1_000_000.0,
) -> pl.DataFrame:
    if amount_column not in df.columns:
        return df

    return df.with_columns([
        (
            pl.col(amount_column).is_null()
            | pl.col(amount_column).is_nan()
            | pl.col(amount_column).is_infinite()
            | (pl.col(amount_column) < 0)
        ).alias("is_invalid_amount"),

        (
            pl.col(amount_column).is_not_null()
            & ~pl.col(amount_column).is_nan()
            & ~pl.col(amount_column).is_infinite()
            & (pl.col(amount_column) > max_valid_amount)
        ).alias("is_suspicious_amount"),
    ])


def add_amount_clean_column(
    df: pl.DataFrame,
    amount_column: str = "amount",
    clean_column: str = "amount_clean",
    max_valid_amount: float = 1_000_000.0,
) -> pl.DataFrame:
    if amount_column not in df.columns:
        return df

    return df.with_columns([
        pl.when(
            pl.col(amount_column).is_not_null()
            & ~pl.col(amount_column).is_nan()
            & ~pl.col(amount_column).is_infinite()
            & (pl.col(amount_column) >= 0)
            & (pl.col(amount_column) <= max_valid_amount)
        )
        .then(pl.col(amount_column))
        .otherwise(None)
        .alias(clean_column)
    ])


def add_date_quality_flags(
    df: pl.DataFrame,
    created_at_column: str = "created_at",
    paid_at_column: str = "paid_at",
) -> pl.DataFrame:
    expressions = []

    if created_at_column in df.columns:
        expressions.append(
            pl.col(created_at_column).is_null().alias("is_invalid_created_at")
        )

    if paid_at_column in df.columns:
        expressions.append(
            pl.col(paid_at_column).is_null().alias("is_invalid_paid_at")
        )

    if created_at_column in df.columns and paid_at_column in df.columns:
        expressions.append(
            (
                pl.col(paid_at_column).is_not_null()
                & pl.col(created_at_column).is_not_null()
                & (pl.col(paid_at_column) < pl.col(created_at_column))
            ).alias("is_paid_before_created")
        )

    if not expressions:
        return df

    return df.with_columns(expressions)


def add_null_quality_flags(
    df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    expressions = []

    for column_name in columns:
        if column_name in df.columns:
            expressions.append(
                pl.col(column_name).is_null().alias(f"is_null_{column_name}")
            )

    if not expressions:
        return df

    return df.with_columns(expressions)


def filter_observed_records(
    df: pl.DataFrame,
    quality_flag_columns: list[str],
) -> pl.DataFrame:
    existing_flags = [
        col for col in quality_flag_columns
        if col in df.columns
    ]

    if not existing_flags:
        return df.head(0)

    return df.filter(
        pl.any_horizontal([
            pl.col(col) == True
            for col in existing_flags
        ])
    )