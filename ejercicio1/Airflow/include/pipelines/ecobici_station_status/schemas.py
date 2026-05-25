import polars as pl


FINAL_SCHEMA = {
    "station_id": pl.Utf8,
    "num_bikes_available": pl.Int64,
    "num_docks_available": pl.Int64,
    "is_installed": pl.Int64,
    "is_renting": pl.Int64,
    "is_returning": pl.Int64,
    "last_reported": pl.Datetime,
    "feed_last_updated": pl.Datetime,
    "ingestion_timestamp": pl.Datetime,
    "extraction_date": pl.Date,
    "is_invalid_availability": pl.Boolean,
    "is_station_unavailable": pl.Boolean,
}