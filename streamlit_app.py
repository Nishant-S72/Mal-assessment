import json
import sqlite3
from pathlib import Path

import streamlit as st

from payment_pipeline.pipeline import OUTPUT_DIR, SQL_DIR, run

DB_PATH = OUTPUT_DIR / "unified_payments.db"
ERROR_PATH = OUTPUT_DIR / "pipeline_errors.json"


def load_rows(query: str, params: tuple = ()) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def load_errors() -> list[dict]:
    if not ERROR_PATH.exists():
        return []
    return json.loads(ERROR_PATH.read_text(encoding="utf-8"))


st.set_page_config(page_title="Mal Unified Payments", layout="wide")
st.title("Mal Unified Payments Demo")
st.caption("Canonical payment contract across Cards, Transfers, and Bill Payments")

summary = run()
payments = load_rows("select * from unified_payments order by event_timestamp desc")
volume = load_rows(
    """
    select source_system, count(*) as payment_count, round(sum(amount), 2) as total_amount
    from unified_payments
    group by source_system
    order by source_system
    """
)
errors = load_errors()

col1, col2, col3 = st.columns(3)
col1.metric("Valid events", summary["records_loaded"])
col2.metric("Rejected rows", summary["errors"])
col3.metric("Source systems", len(volume))

selected_sources = st.multiselect(
    "Filter source systems",
    options=[row["source_system"] for row in volume],
    default=[row["source_system"] for row in volume],
)

st.subheader("Squad Volume")
st.dataframe(volume, use_container_width=True, hide_index=True)

filtered_payments = [
    row for row in payments if row["source_system"] in set(selected_sources or [])
]
st.subheader("Unified Payment Events")
st.dataframe(filtered_payments, use_container_width=True, hide_index=True)

left, right = st.columns(2)
with left:
    st.subheader("Validation Errors")
    st.json(errors)
with right:
    st.subheader("Downstream SQL")
    st.code((SQL_DIR / "queries.sql").read_text(encoding="utf-8"), language="sql")
