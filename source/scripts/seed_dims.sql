-- ============================================================
-- Seed dimension tables (dim_date, dim_product).
-- dim_customer + fact_sales are built by the ETL pipeline.
-- ============================================================

-- DimDate: allowed-order window (2020-01-01 .. 2026-12-31)
INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, month_name,
                         day_of_week, day_num, is_weekend)
SELECT
    to_char(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    to_char(d, 'FMMonth'),
    to_char(d, 'FMDay'),
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series('2020-01-01'::date, '2026-12-31'::date, '1 day'::interval) g(d)
ON CONFLICT (date_key) DO NOTHING;

-- DimProduct is loaded from the streaming catalogue by pkg_dim_load;
-- ensure it contains only catalogue entries (de-dup safe).
INSERT INTO dw.dim_product (product_code, product_name, category, base_price, manufacturer, is_active)
SELECT sku, product_name, LOWER(category), unit_price, manufacturer, TRUE
FROM public.products
ON CONFLICT (product_code) DO NOTHING;