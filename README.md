# Mal Unified Payments

Standalone Python 3.9+ repository that unifies Cards, Transfers, and Bill Payments squad data into one canonical payment event contract.

## What it does

- Ingests three squad-specific CSV files from `data/input/`
- Maps each format into canonical `payment_event` contract `v1`
- Validates `v1`, migrates to `v2`, and validates again
- Writes unified output to:
  - `data/output/unified_payments_v2.jsonl`
  - `data/output/unified_payments.db`
  - `data/output/pipeline_errors.json`
- Generates reusable downstream SQL in `sql/queries.sql`
- Includes a local Streamlit demo in `streamlit_app.py`

## Canonical Schema

`v2` fields:

| field | type | notes |
| --- | --- | --- |
| contract_version | string | `v2` |
| event_id | string | globally unique |
| source_system | string | `cards`, `transfers`, `bill_payments` |
| payment_type | string | `card`, `transfer`, `bill_payment` |
| payment_reference | string | source transaction identifier |
| customer_id | string | customer key |
| amount | decimal(18,2) | positive payment amount |
| currency | string | ISO 4217 alpha-3 |
| event_timestamp | timestamp | normalized UTC ISO-8601 |
| status | string | `pending`, `completed`, `failed`, `reversed` |
| payment_method | string | `card`, `bank_transfer`, `bank_account` |
| processing_channel | string | added in `v2`; `app`, `api`, `batch`, `branch`, `pos`, `unknown` |
| merchant_id | string nullable | card merchant |
| counterparty_ref | string nullable | transfer or bill account reference |
| biller_code | string nullable | biller identifier |
| card_network | string nullable | card scheme |

## Versioning

- `v1` contract does not include `processing_channel`
- `v2` adds required `processing_channel`
- The pipeline demonstrates a simple migration path by:
  1. Transforming each source row into canonical `v1`
  2. Migrating `v1 -> v2`
  3. Emitting only validated `v2` records downstream

## Setup

```bash
cd /Users/nishantsangwan/Documents/Playground/mal-unified-payments
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m payment_pipeline
streamlit run streamlit_app.py
```

## GitHub And Streamlit Cloud

Recommended repo name: `mal-unified-payments`

Once the repository is pushed to GitHub, deploy on Streamlit Community Cloud with:

1. Sign in to [Streamlit Community Cloud](https://share.streamlit.io/)
2. Choose the GitHub repository
3. Set the main file path to `streamlit_app.py`
4. Keep Python dependencies sourced from `requirements.txt`
5. Deploy from the `master` branch

## Expected Output

Successful run summary:

```python
{'records_loaded': 8, 'errors': 1}
```

The mock data intentionally includes one invalid bill payment row to show validation behavior:

- missing `customer_id`
- invalid currency code (`US`)

The pipeline records both issues in `data/output/pipeline_errors.json` and continues processing valid rows.

## Run SQL Queries

Use SQLite locally:

```bash
sqlite3 data/output/unified_payments.db < sql/queries.sql
```

## Repository Structure

```text
mal-unified-payments/
├── data/input/*.csv
├── data/output/
├── payment_pipeline/contracts.py
├── payment_pipeline/pipeline.py
├── requirements.txt
└── sql/queries.sql
```
