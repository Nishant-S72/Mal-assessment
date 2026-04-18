from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from .contracts import ContractError, validate_v1, validate_v2

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"
SQL_DIR = BASE_DIR / "sql"

Row = Dict[str, str]


def normalize_status(value: str) -> str:
    mapping = {
        "approved": "completed",
        "settled": "completed",
        "done": "completed",
        "processing": "pending",
        "queued": "pending",
        "declined": "failed",
        "error": "failed",
        "refunded": "reversed",
    }
    return mapping.get(value.strip().lower(), value.strip().lower())


def card_to_v1(row: Row) -> Tuple[Dict[str, Any], str]:
    record = {
        "contract_version": "v1",
        "event_id": f"card-{row['txn_ref']}",
        "source_system": "cards",
        "payment_type": "card",
        "payment_reference": row["txn_ref"],
        "customer_id": row["user_id"],
        "amount": int(row["amount_minor"]) / 100,
        "currency": row["currency_code"],
        "event_timestamp": row["auth_ts"],
        "status": normalize_status(row["state"]),
        "payment_method": "card",
        "merchant_id": row["merchant_id"],
        "counterparty_ref": None,
        "biller_code": None,
        "card_network": row["network"],
    }
    return validate_v1(record), row.get("channel", "unknown").strip().lower() or "unknown"


def transfer_to_v1(row: Row) -> Tuple[Dict[str, Any], str]:
    record = {
        "contract_version": "v1",
        "event_id": f"transfer-{row['transfer_id']}",
        "source_system": "transfers",
        "payment_type": "transfer",
        "payment_reference": row["transfer_id"],
        "customer_id": row["customer"],
        "amount": row["value"],
        "currency": row["ccy"],
        "event_timestamp": row["created_at"],
        "status": normalize_status(row["transfer_status"]),
        "payment_method": "bank_transfer",
        "merchant_id": None,
        "counterparty_ref": row["destination_account"],
        "biller_code": None,
        "card_network": None,
    }
    return validate_v1(record), row.get("initiated_via", "unknown").strip().lower() or "unknown"


def bill_to_v1(row: Row) -> Tuple[Dict[str, Any], str]:
    method = "card" if row["funding_source"].strip().lower() == "card" else "bank_account"
    record = {
        "contract_version": "v1",
        "event_id": f"bill-{row['payment_id']}",
        "source_system": "bill_payments",
        "payment_type": "bill_payment",
        "payment_reference": row["payment_id"],
        "customer_id": row["cust_id"],
        "amount": row["amount"],
        "currency": row["currency"],
        "event_timestamp": row["paid_at"],
        "status": normalize_status(row["payment_state"]),
        "payment_method": method,
        "merchant_id": None,
        "counterparty_ref": row["account_number"],
        "biller_code": row["biller"],
        "card_network": None,
    }
    return validate_v1(record), row.get("origin", "unknown").strip().lower() or "unknown"


def migrate_v1_to_v2(record: Dict[str, Any], channel: str) -> Dict[str, Any]:
    migrated = dict(record)
    migrated["contract_version"] = "v2"
    migrated["processing_channel"] = channel if channel else "unknown"
    return validate_v2(migrated)


def load_csv(path: Path, transformer: Callable[[Row], Tuple[Dict[str, Any], str]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    good_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                v1_record, channel = transformer(row)
                good_rows.append(migrate_v1_to_v2(v1_record, channel))
            except (KeyError, ValueError, ContractError) as exc:
                errors.append(
                    {
                        "file": path.name,
                        "line": line_number,
                        "error": str(exc),
                        "row": row,
                    }
                )
    return good_rows, errors


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def write_sqlite(path: Path, records: List[Dict[str, Any]]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("drop table if exists unified_payments")
        conn.execute(
            """
            create table unified_payments (
                contract_version text,
                event_id text primary key,
                source_system text,
                payment_type text,
                payment_reference text,
                customer_id text,
                amount real,
                currency text,
                event_timestamp text,
                status text,
                payment_method text,
                processing_channel text,
                merchant_id text,
                counterparty_ref text,
                biller_code text,
                card_network text
            )
            """
        )
        conn.executemany(
            """
            insert into unified_payments values (
                :contract_version, :event_id, :source_system, :payment_type,
                :payment_reference, :customer_id, :amount, :currency,
                :event_timestamp, :status, :payment_method, :processing_channel,
                :merchant_id, :counterparty_ref, :biller_code, :card_network
            )
            """,
            records,
        )
        conn.commit()
    finally:
        conn.close()


def write_queries(path: Path) -> None:
    queries = """-- Daily payment volume by product squad
select source_system, date(event_timestamp) as event_date, count(*) as payment_count, round(sum(amount), 2) as total_amount
from unified_payments
group by source_system, date(event_timestamp)
order by event_date, source_system;

-- Failed payment rate by payment type
select payment_type, round(100.0 * sum(case when status = 'failed' then 1 else 0 end) / count(*), 2) as failed_pct
from unified_payments
group by payment_type
order by failed_pct desc;

-- Customer-level payment history across all squads
select customer_id, payment_type, payment_reference, amount, currency, status, event_timestamp
from unified_payments
where customer_id = 'cust_1001'
order by event_timestamp desc;
"""
    path.write_text(queries, encoding="utf-8")


def run() -> Dict[str, int]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sources = [
        (INPUT_DIR / "cards.csv", card_to_v1),
        (INPUT_DIR / "transfers.csv", transfer_to_v1),
        (INPUT_DIR / "bill_payments.csv", bill_to_v1),
    ]
    records: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for path, transformer in sources:
        source_records, source_errors = load_csv(path, transformer)
        records.extend(source_records)
        errors.extend(source_errors)
    write_jsonl(OUTPUT_DIR / "unified_payments_v2.jsonl", records)
    write_sqlite(OUTPUT_DIR / "unified_payments.db", records)
    (OUTPUT_DIR / "pipeline_errors.json").write_text(
        json.dumps(errors, indent=2), encoding="utf-8"
    )
    write_queries(SQL_DIR / "queries.sql")
    return {"records_loaded": len(records), "errors": len(errors)}


if __name__ == "__main__":
    summary = run()
    print(json.dumps(summary, indent=2))
