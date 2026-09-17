-- ============================================================
-- DimCustomer SCD Type 2 upsert procedure.
-- Business key: normalized customer_email.
-- New attribute value (name) different from the current
-- row -> expire current, insert a new current row.
-- ============================================================
CREATE OR REPLACE FUNCTION dw.customer_fingerprint(
    p_email VARCHAR(160), p_name VARCHAR(120)
) RETURNS VARCHAR(40) AS $$
    SELECT substring(md5(lower(p_email || '|' || p_name)) for 40);
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE PROCEDURE dw.upsert_customer(p_batch_id BIGINT) AS $$
DECLARE
    r RECORD;
BEGIN
    -- For each unique current customer from clean_sales, apply SCD2 logic.
    FOR r IN
        SELECT DISTINCT
            lower(btrim(customer_email))          AS customer_id,
            btrim(customer_name)                  AS customer_name,
            min(order_date)                       AS order_date
        FROM stage.clean_sales
        WHERE customer_email IS NOT NULL
          AND btrim(customer_email) <> ''
        GROUP BY lower(btrim(customer_email)), btrim(customer_name)
        ORDER BY customer_id
    LOOP
        IF EXISTS (
            SELECT 1 FROM dw.dim_customer
            WHERE customer_id = r.customer_id AND is_current
        ) THEN
            -- unchanged? do nothing. changed -> expire + insert.
            IF NOT EXISTS (
                SELECT 1 FROM dw.dim_customer
                WHERE customer_id = r.customer_id AND is_current
                  AND btrim(customer_name) = r.customer_name
            ) THEN
                UPDATE dw.dim_customer
                   SET valid_to = r.order_date, is_current = FALSE
                 WHERE customer_id = r.customer_id AND is_current;

                INSERT INTO dw.dim_customer
                    (customer_id, customer_name, valid_from, valid_to,
                     is_current, etl_batch_id)
                VALUES (r.customer_id, r.customer_name,
                        r.order_date, NULL, TRUE, p_batch_id);
            END IF;
        ELSE
            INSERT INTO dw.dim_customer
                (customer_id, customer_name, valid_from, valid_to,
                 is_current, etl_batch_id)
            VALUES (r.customer_id, r.customer_name,
                    r.order_date, NULL, TRUE, p_batch_id);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;