from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable

ISO_FORMATS = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S")
PAYMENT_TYPES = {"card", "transfer", "bill_payment"}
STATUSES = {"pending", "completed", "failed", "reversed"}
METHODS = {"card", "bank_transfer", "bank_account"}
CHANNELS = {"app", "api", "batch", "branch", "pos", "unknown"}


class ContractError(ValueError):
    pass


@dataclass
class FieldError:
    field: str
    message: str


def parse_timestamp(value: str) -> str:
    for fmt in ISO_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    raise ContractError(f"invalid timestamp '{value}'")


def parse_amount(value: Any) -> float:
    try:
        amount = Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise ContractError(f"invalid amount '{value}'") from exc
    if amount <= 0:
        raise ContractError("amount must be positive")
    return float(amount)


def require_string(record: Dict[str, Any], field: str) -> str:
    value = record.get(field)
    if value is None or str(value).strip() == "":
        raise ContractError(f"missing required field '{field}'")
    return str(value).strip()


def optional_string(record: Dict[str, Any], field: str) -> str | None:
    value = record.get(field)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _validate_common(record: Dict[str, Any], required_fields: Iterable[str]) -> None:
    errors = []
    for field in required_fields:
        try:
            require_string(record, field)
        except ContractError as exc:
            errors.append(str(exc))
    payment_type = str(record.get("payment_type", "")).strip()
    status = str(record.get("status", "")).strip()
    method = str(record.get("payment_method", "")).strip()
    currency = str(record.get("currency", "")).strip().upper()
    if payment_type and payment_type not in PAYMENT_TYPES:
        errors.append(f"unsupported payment_type '{payment_type}'")
    if status and status not in STATUSES:
        errors.append(f"unsupported status '{status}'")
    if method and method not in METHODS:
        errors.append(f"unsupported payment_method '{method}'")
    if currency and len(currency) != 3:
        errors.append("currency must be a 3-letter ISO code")
    if record.get("event_timestamp"):
        try:
            record["event_timestamp"] = parse_timestamp(str(record["event_timestamp"]))
        except ContractError as exc:
            errors.append(str(exc))
    if record.get("amount") not in (None, ""):
        try:
            record["amount"] = parse_amount(record["amount"])
        except ContractError as exc:
            errors.append(str(exc))
    if errors:
        raise ContractError("; ".join(errors))
    record["currency"] = currency


def validate_v1(record: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = dict(record)
    _validate_common(
        cleaned,
        [
            "contract_version",
            "event_id",
            "source_system",
            "payment_type",
            "payment_reference",
            "customer_id",
            "amount",
            "currency",
            "event_timestamp",
            "status",
            "payment_method",
        ],
    )
    if cleaned["contract_version"] != "v1":
        raise ContractError("contract_version must be 'v1'")
    return cleaned


def validate_v2(record: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = dict(record)
    _validate_common(
        cleaned,
        [
            "contract_version",
            "event_id",
            "source_system",
            "payment_type",
            "payment_reference",
            "customer_id",
            "amount",
            "currency",
            "event_timestamp",
            "status",
            "payment_method",
            "processing_channel",
        ],
    )
    if cleaned["contract_version"] != "v2":
        raise ContractError("contract_version must be 'v2'")
    if cleaned["processing_channel"] not in CHANNELS:
        raise ContractError(
            f"unsupported processing_channel '{cleaned['processing_channel']}'"
        )
    for field in ("merchant_id", "counterparty_ref", "biller_code", "card_network"):
        cleaned[field] = optional_string(cleaned, field)
    return cleaned
