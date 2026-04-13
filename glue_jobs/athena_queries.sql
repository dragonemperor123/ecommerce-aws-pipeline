-- ============================================================
-- Ecommerce Analytics — Athena Query Library
-- Database: ecom_datalake
-- Workgroup: ecom-analytics
-- ============================================================

-- ── Revenue KPIs ─────────────────────────────────────────────
-- Daily revenue trend (last 30 days)
SELECT
    order_date,
    total_orders,
    ROUND(gross_revenue, 2)          AS gross_revenue,
    ROUND(avg_order_value, 2)        AS avg_order_value,
    unique_customers,
    ROUND(conversion_rate * 100, 1)  AS conversion_rate_pct,
    ROUND(fraud_rate * 100, 2)       AS fraud_rate_pct
FROM curated_daily_kpis
WHERE order_date >= current_date - INTERVAL '30' DAY
ORDER BY order_date DESC;


-- ── Top Products by Revenue ───────────────────────────────────
SELECT
    item.product_id,
    item.category,
    COUNT(DISTINCT order_id)          AS order_appearances,
    SUM(item.quantity)                AS units_sold,
    ROUND(SUM(item.quantity * item.unit_price), 2) AS revenue
FROM processed_orders
CROSS JOIN UNNEST(items) AS t(item)
WHERE status = 'confirmed'
  AND order_date >= current_date - INTERVAL '7' DAY
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 20;


-- ── Customer Cohort Retention ─────────────────────────────────
WITH cohorts AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM processed_orders
    WHERE status = 'confirmed'
    GROUP BY customer_id
),
activity AS (
    SELECT
        o.customer_id,
        c.cohort_month,
        DATE_TRUNC('month', o.order_date) AS activity_month,
        DATE_DIFF('month', c.cohort_month, DATE_TRUNC('month', o.order_date)) AS period_number
    FROM processed_orders o
    JOIN cohorts c ON o.customer_id = c.customer_id
    WHERE o.status = 'confirmed'
)
SELECT
    cohort_month,
    period_number,
    COUNT(DISTINCT customer_id) AS active_customers
FROM activity
GROUP BY 1, 2
ORDER BY 1, 2;


-- ── Fraud Analysis ────────────────────────────────────────────
SELECT
    order_date,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN fraud_score >= 0.5 THEN 1 ELSE 0 END) AS flagged_orders,
    ROUND(100.0 * SUM(CASE WHEN fraud_score >= 0.5 THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(AVG(fraud_score), 4) AS avg_fraud_score,
    ROUND(SUM(CASE WHEN fraud_score >= 0.5 THEN CAST(total AS DOUBLE) ELSE 0 END), 2) AS flagged_revenue
FROM processed_orders
GROUP BY order_date
ORDER BY order_date DESC
LIMIT 30;


-- ── Clickstream Funnel Analysis ───────────────────────────────
SELECT
    event_date,
    action,
    COUNT(*) AS event_count,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT customer_id) AS authenticated_users,
    ROUND(AVG(duration_ms) / 1000.0, 1) AS avg_duration_sec
FROM processed_clickstream
WHERE event_date >= current_date - INTERVAL '7' DAY
GROUP BY 1, 2
ORDER BY 1, event_count DESC;


-- ── Real-time Hourly Orders (last 24h) ────────────────────────
SELECT
    DATE_FORMAT(FROM_ISO8601_TIMESTAMP(created_at), '%Y-%m-%d %H:00') AS hour,
    COUNT(*) AS orders,
    ROUND(SUM(CAST(total AS DOUBLE)), 2) AS revenue,
    COUNT(DISTINCT customer_id) AS customers
FROM processed_orders
WHERE order_date >= current_date - INTERVAL '1' DAY
GROUP BY 1
ORDER BY 1 DESC;


-- ── Customer CLV Segments ─────────────────────────────────────
SELECT
    clv_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spend), 2) AS avg_clv,
    ROUND(AVG(order_count), 1) AS avg_orders,
    ROUND(MIN(total_spend), 2) AS min_spend,
    ROUND(MAX(total_spend), 2) AS max_spend
FROM curated_customer_clv
GROUP BY clv_segment
ORDER BY avg_clv DESC;
