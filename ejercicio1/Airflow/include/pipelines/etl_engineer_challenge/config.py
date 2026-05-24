DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%Y-%m-%d %H:%M:%S",
    "%d-%m-%Y",
]

VALID_STATUS = {
    "charged_back",
    "expired",
    "paid",
    "partially_refunded",
    "pending_payment",
    "pre_authorized",
    "refunded",
    "voided",
}

MAX_VALID_AMOUNT = 1_000_000.0

QUALITY_FLAG_COLUMNS = [
    "is_invalid_id",
    "is_null_id",
    "is_null_name",
    "is_null_company_id",
    "is_invalid_created_at",
    "is_invalid_paid_at",
    "is_paid_before_created",
    "is_invalid_status",
    "is_invalid_amount",
    "is_suspicious_amount",
]