-- 001_init_schema.sql
-- PostgreSQL schema for the e-commerce analytics platform.
-- Based on the provided database-schema.sql:
--   * orders partitions are generated programmatically for a full date range
--     (the original only had two example months with a "..." gap in between,
--     which would reject inserts for any uncovered month) plus a DEFAULT
--     partition as a safety net for dates outside the generated range.
--   * order_items intentionally stays a single (non-partitioned) table: Postgres
--     partitioned tables can't be an FK target, so order_items.order_id already
--     has no FK to orders. Partitioning order_items too would need a denormalized
--     order_date column with no clear benefit at this scale over the current
--     btree index on order_id; documented further in docs/technical-document.md.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For text search

CREATE TYPE order_status AS ENUM (
    'Pending', 'Processing', 'Shipped', 'In Transit', 'Delivered', 'Cancelled', 'Returned'
);

CREATE TYPE payment_method AS ENUM (
    'Credit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Gift Card', 'Bank Transfer'
);

-- Time dimension table for analytics
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    day_of_week SMALLINT NOT NULL,
    day_of_month SMALLINT NOT NULL,
    day_of_year SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    quarter SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL
);

CREATE TABLE product_categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    parent_id INTEGER REFERENCES product_categories(category_id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_product_categories_parent_id ON product_categories(parent_id);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    cost DECIMAL(10, 2),
    category_id INTEGER NOT NULL REFERENCES product_categories(category_id),
    sku VARCHAR(50) UNIQUE NOT NULL,
    inventory_count INTEGER NOT NULL DEFAULT 0,
    weight DECIMAL(8, 2),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_is_active ON products(is_active);
CREATE INDEX idx_products_name_trgm ON products USING gin (name gin_trgm_ops);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    street_address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    phone VARCHAR(50),
    registration_date TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_last_name ON customers(last_name);
CREATE INDEX idx_customers_location ON customers(country, state, city);

-- Master orders table, partitioned by month on order_date
CREATE TABLE orders (
    order_id SERIAL,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMP NOT NULL,
    status order_status NOT NULL DEFAULT 'Pending',
    payment_method payment_method NOT NULL,
    shipping_address VARCHAR(255) NOT NULL,
    shipping_city VARCHAR(100) NOT NULL,
    shipping_state VARCHAR(50) NOT NULL,
    shipping_zip VARCHAR(20) NOT NULL,
    shipping_country VARCHAR(50) NOT NULL,
    processing_date TIMESTAMP,
    shipping_date TIMESTAMP,
    delivery_date TIMESTAMP,
    total_amount DECIMAL(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, order_date)
) PARTITION BY RANGE (order_date);

-- Generate monthly partitions covering 2020-01 through 2027-12, plus a DEFAULT
-- catch-all so inserts never fail outright for dates outside the generated range.
DO $$
DECLARE
    start_month DATE := DATE '2020-01-01';
    end_month DATE := DATE '2028-01-01';
    partition_start DATE;
    partition_name TEXT;
BEGIN
    partition_start := start_month;
    WHILE partition_start < end_month LOOP
        partition_name := 'orders_y' || to_char(partition_start, 'YYYY') || 'm' || to_char(partition_start, 'MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF orders FOR VALUES FROM (%L) TO (%L);',
            partition_name, partition_start, partition_start + INTERVAL '1 month'
        );
        partition_start := partition_start + INTERVAL '1 month';
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS orders_default PARTITION OF orders DEFAULT;

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);

-- order_items: single table (see note at top of file for why it isn't partitioned)
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL, -- no FK: orders is partitioned, order_id alone isn't unique across it
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    discount DECIMAL(10, 2) NOT NULL DEFAULT 0,
    total DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);

-- Materialized view for product sales summary (refreshed on demand / by Flyte data-quality task)
CREATE MATERIALIZED VIEW product_sales_summary AS
SELECT
    p.product_id,
    p.name AS product_name,
    pc.name AS category_name,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.total) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT o.customer_id) AS customer_count,
    MAX(o.order_date) AS last_order_date
FROM
    products p
    JOIN product_categories pc ON p.category_id = pc.category_id
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
WHERE
    o.status NOT IN ('Cancelled', 'Returned')
GROUP BY
    p.product_id, p.name, pc.name
WITH NO DATA;

CREATE UNIQUE INDEX idx_product_sales_summary_product_id ON product_sales_summary(product_id);

-- View for customer purchase history
CREATE VIEW customer_purchase_summary AS
SELECT
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.total_amount) AS lifetime_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    CASE WHEN COUNT(DISTINCT o.order_id) > 1
        THEN (MAX(o.order_date) - MIN(o.order_date)) / (COUNT(DISTINCT o.order_id) - 1)
        ELSE NULL
    END AS avg_days_between_orders
FROM
    customers c
    JOIN orders o ON c.customer_id = o.customer_id
WHERE
    o.status NOT IN ('Cancelled', 'Returned')
GROUP BY
    c.customer_id, c.email, c.first_name, c.last_name;

-- Daily sales aggregation table (populated by the ETL transform/load stage)
CREATE TABLE daily_sales_aggregation (
    date DATE NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    category_id INTEGER NOT NULL REFERENCES product_categories(category_id),
    units_sold INTEGER NOT NULL DEFAULT 0,
    revenue DECIMAL(12, 2) NOT NULL DEFAULT 0,
    order_count INTEGER NOT NULL DEFAULT 0,
    avg_unit_price DECIMAL(10, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY (date, product_id)
);

CREATE INDEX idx_daily_sales_date ON daily_sales_aggregation(date);
CREATE INDEX idx_daily_sales_product_id ON daily_sales_aggregation(product_id);
CREATE INDEX idx_daily_sales_category_id ON daily_sales_aggregation(category_id);

-- updated_at trigger
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_product_categories_modtime
    BEFORE UPDATE ON product_categories
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_products_modtime
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_customers_modtime
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_orders_modtime
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();
