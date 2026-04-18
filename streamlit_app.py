import json
import sqlite3

import pandas as pd
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
payments_df = pd.DataFrame(
    load_rows("select * from unified_payments order by event_timestamp desc")
)
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

filtered_payments = payments_df[payments_df["source_system"].isin(selected_sources)].copy()
filtered_payments["event_date"] = pd.to_datetime(
    filtered_payments["event_timestamp"]
).dt.date
daily_amounts = (
    filtered_payments.groupby(["event_date", "source_system"], as_index=False)["amount"]
    .sum()
    .rename(columns={"amount": "total_amount"})
)
status_mix = (
    filtered_payments.groupby(["payment_type", "status"], as_index=False)
    .size()
    .rename(columns={"size": "payment_count"})
)

st.subheader("Squad Volume")
st.dataframe(volume, use_container_width=True, hide_index=True)

chart_left, chart_right = st.columns(2)
with chart_left:
    st.subheader("Daily Amount Trend")
    st.line_chart(
        daily_amounts,
        x="event_date",
        y="total_amount",
        color="source_system",
        use_container_width=True,
    )
with chart_right:
    st.subheader("Status Mix By Payment Type")
    st.bar_chart(
        status_mix,
        x="payment_type",
        y="payment_count",
        color="status",
        stack=True,
        use_container_width=True,
    )

st.subheader("Unified Payment Events")
st.dataframe(filtered_payments, use_container_width=True, hide_index=True)

left, right = st.columns(2)
with left:
    st.subheader("Validation Errors")
    st.json(errors)
with right:
    st.subheader("Downstream SQL")
    st.code((SQL_DIR / "queries.sql").read_text(encoding="utf-8"), language="sql")
