-- The slice of AdventureWorksDW that Lesson 1 actually touches.
--
-- Three tables, because that is all the package reads and writes: two
-- dimensions it looks up against and one fact table it loads. Standing up the
-- whole sample warehouse would prove nothing extra and cost a gigabyte.
--
-- This is POSTGRES, while the package targets SQL Server. That retarget is the
-- bindings layer doing its job, not a shortcut: an SSIS connection manager
-- says "localhost, AdventureWorksDW2014, Integrated Security=SSPI", none of
-- which survives a move to NiFi, so the deployment target is chosen at deploy
-- time. Changing it is a bindings edit, not a converter change.

CREATE SCHEMA IF NOT EXISTS dbo;

-- Looked up on CurrencyAlternateKey (the SSIS join column), returns CurrencyKey.
CREATE TABLE IF NOT EXISTS dbo.dimcurrency (
    currencykey          integer PRIMARY KEY,
    currencyalternatekey char(3) NOT NULL UNIQUE,
    currencyname         varchar(50) NOT NULL
);

-- Looked up on FullDateAlternateKey, returns DateKey.
--
-- The alternate key is TEXT, not DATE, and that is the converter's own
-- LOOKUP_DATE_KEY_CAST diagnostic being acted on rather than ignored:
--
--   "SQL Server implicitly casts a string to date in a WHERE clause; Postgres
--    and most other engines do not, so a retargeted deployment fails with
--    'operator does not exist: date = character varying'."
--
-- The record pipeline carries dates as strings until the JDBC boundary (see
-- catalogue/types.yml), so the lookup compares a string. In AdventureWorks on
-- SQL Server this column is a date and the implicit cast hides the issue. On
-- Postgres it does not, so the natural key is stored as text here. datekey
-- remains the real surrogate key.
CREATE TABLE IF NOT EXISTS dbo.dimdate (
    datekey              integer PRIMARY KEY,
    fulldatealternatekey text NOT NULL UNIQUE
);

-- The load target. No foreign keys, deliberately: a referential fault must be
-- caught and reported per row by the pipeline, not blow up a whole batch at
-- the database layer. NIFI-FLOW makes the same call for the same reason.
CREATE TABLE IF NOT EXISTS dbo.newfactcurrencyrate (
    averagerate  real    NOT NULL,
    currencykey  integer NOT NULL,
    currencydate date    NOT NULL,
    endofdayrate real    NOT NULL,
    datekey      integer NOT NULL
);

-- Two currencies and a fortnight of dates: enough for rows that match the
-- lookups AND rows that miss them, which is what makes the no-match path
-- observable rather than theoretical.
INSERT INTO dbo.dimcurrency (currencykey, currencyalternatekey, currencyname) VALUES
    (36, 'GBP', 'United Kingdom Pound'),
    (39, 'EUR', 'Euro'),
    (98, 'USD', 'US Dollar')
ON CONFLICT DO NOTHING;

INSERT INTO dbo.dimdate (datekey, fulldatealternatekey)
SELECT to_char(d, 'YYYYMMDD')::int, to_char(d, 'YYYY-MM-DD')
FROM generate_series('2013-12-30'::date, '2014-01-12'::date, '1 day') AS g(d)
ON CONFLICT DO NOTHING;
