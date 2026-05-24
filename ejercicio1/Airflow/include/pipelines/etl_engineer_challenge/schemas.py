import polars as pl


EXPECTED_COLUMNS = {
    "id": pl.Utf8,
    "name": pl.Utf8,
    "company_id": pl.Utf8,
    "amount": pl.Float64,
    "status": pl.Utf8,
    "created_at": pl.Utf8,
    "paid_at": pl.Utf8,
}


FINAL_SCHEMA = {
    "name": pl.Utf8,
    "created_at": pl.Date,
    "total_transactions": pl.UInt32,
    "valid_amount_transactions": pl.UInt32,
    "invalid_amount_transactions": pl.UInt32,
    "total_amount": pl.Float64,
    "avg_amount": pl.Float64,
    "min_amount": pl.Float64,
    "max_amount": pl.Float64,
    "unique_customers": pl.UInt32,
    "unique_companies": pl.UInt32,
    "invalid_id_count": pl.UInt32,
    "invalid_status_count": pl.UInt32,
    "paid_before_created_count": pl.UInt32,
}