-- Daily payment volume by product squad
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
