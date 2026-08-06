-- 002_partition_maintenance.sql
-- Operational helper functions: partition maintenance and daily aggregation refresh.
-- Both are idempotent and safe to call repeatedly (e.g. from a monthly cron job or
-- a Flyte data-quality/maintenance task).

-- Ensures monthly partitions exist for orders from the current month through
-- `months_ahead` months in the future. 001_init_schema.sql already pre-creates
-- partitions through 2027-12 plus a DEFAULT partition, so this is for ongoing
-- maintenance once that pre-generated range is exhausted.
CREATE OR REPLACE FUNCTION create_future_order_partitions(months_ahead INTEGER DEFAULT 6)
RETURNS VOID AS $$
DECLARE
    partition_start DATE := date_trunc('month', CURRENT_DATE)::DATE;
    partition_end DATE := (date_trunc('month', CURRENT_DATE) + (months_ahead || ' months')::INTERVAL)::DATE;
    partition_name TEXT;
BEGIN
    WHILE partition_start < partition_end LOOP
        partition_name := 'orders_y' || to_char(partition_start, 'YYYY') || 'm' || to_char(partition_start, 'MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF orders FOR VALUES FROM (%L) TO (%L);',
            partition_name, partition_start, partition_start + INTERVAL '1 month'
        );
        partition_start := partition_start + INTERVAL '1 month';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Refreshes daily_sales_aggregation for a given date (defaults to today).
-- Idempotent: clears the target date's rows before recomputing.
CREATE OR REPLACE FUNCTION refresh_daily_sales_aggregation(target_date DATE DEFAULT CURRENT_DATE)
RETURNS VOID AS $$
BEGIN
    DELETE FROM daily_sales_aggregation
    WHERE date = target_date;

    INSERT INTO daily_sales_aggregation
    SELECT
        CAST(o.order_date AS DATE) AS date,
        oi.product_id,
        p.category_id,
        SUM(oi.quantity) AS units_sold,
        SUM(oi.total) AS revenue,
        COUNT(DISTINCT o.order_id) AS order_count,
        SUM(oi.total) / NULLIF(SUM(oi.quantity), 0) AS avg_unit_price
    FROM
        orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
    WHERE
        CAST(o.order_date AS DATE) = target_date
        AND o.status NOT IN ('Cancelled', 'Returned')
    GROUP BY
        CAST(o.order_date AS DATE),
        oi.product_id,
        p.category_id;
END;
$$ LANGUAGE plpgsql;
