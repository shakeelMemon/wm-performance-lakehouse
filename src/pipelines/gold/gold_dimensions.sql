-- =====================================================================================
-- GOLD: conformed dimensions for Wealth Management reporting
-- ${wm.catalog} is substituted from the pipeline configuration.
-- =====================================================================================

-- Calendar with Canadian-bank fiscal year (FY starts 1 November; FY2026 = Nov-2025..Oct-2026)
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.dim_date
COMMENT 'Calendar dimension incl. fiscal year (Nov-Oct), business-day and month-end flags'
AS
WITH d AS (
  SELECT explode(sequence(DATE'2020-01-01', DATE'2028-12-31', INTERVAL 1 DAY)) AS date_key
),
b AS (
  SELECT date_key,
         dayofweek(date_key) NOT IN (1, 7) AS is_business_day,
         trunc(date_key, 'MM') AS month_start
  FROM d
)
SELECT
  date_key,
  year(date_key)                                            AS calendar_year,
  quarter(date_key)                                         AS calendar_quarter,
  month(date_key)                                           AS calendar_month,
  month_start,
  last_day(date_key)                                        AS month_end,
  date_format(date_key, 'MMM yyyy')                         AS month_label,
  date_format(date_key, 'E')                                AS day_name,
  is_business_day,
  date_key = max(CASE WHEN is_business_day THEN date_key END) OVER (PARTITION BY month_start)
                                                            AS is_last_business_day_of_month,
  CASE WHEN month(date_key) >= 11 THEN year(date_key) + 1 ELSE year(date_key) END
                                                            AS fiscal_year,
  CAST(ceil(((month(date_key) + 1) % 12 + 1) / 3.0) AS INT) AS fiscal_quarter,
  concat('FY', CASE WHEN month(date_key) >= 11 THEN year(date_key) + 1 ELSE year(date_key) END,
         ' Q', CAST(ceil(((month(date_key) + 1) % 12 + 1) / 3.0) AS INT))
                                                            AS fiscal_period_label
FROM b;

-- Current advisor attributes
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.dim_advisor
(CONSTRAINT pk_advisor EXPECT (advisor_id IS NOT NULL) ON VIOLATION FAIL UPDATE)
COMMENT 'Advisor dimension (current state) with branch hierarchy: advisor > branch > region'
AS
SELECT
  a.advisor_id, a.advisor_name, a.designation, a.team_type,
  CAST(a.hire_date AS DATE)                                 AS hire_date,
  round(months_between(current_date(), CAST(a.hire_date AS DATE)) / 12, 1) AS tenure_years,
  a.branch_id, b.branch_name, b.region, b.province, a.status
FROM ${wm.catalog}.wm_silver.advisors a
LEFT JOIN ${wm.catalog}.wm_silver.branches b ON a.branch_id = b.branch_id
WHERE a.__END_AT IS NULL;

-- Full advisor history for point-in-time (as-was) attribution
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.dim_advisor_history
COMMENT 'Advisor SCD2 history: use valid_from <= date < valid_to for as-was branch/region'
AS
SELECT
  a.advisor_id, a.advisor_name, a.branch_id, b.branch_name, b.region,
  CAST(a.__START_AT AS DATE)                                AS valid_from,
  coalesce(CAST(a.__END_AT AS DATE), DATE'9999-12-31')      AS valid_to,
  a.__END_AT IS NULL                                        AS is_current
FROM ${wm.catalog}.wm_silver.advisors a
LEFT JOIN ${wm.catalog}.wm_silver.branches b ON a.branch_id = b.branch_id;

-- Current household attributes + lifecycle dates
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.dim_household
(CONSTRAINT pk_household EXPECT (household_id IS NOT NULL) ON VIOLATION FAIL UPDATE)
COMMENT 'Household dimension (current) with onboarding / attrition dates and tenure band'
AS
WITH closed AS (
  SELECT household_id, min(CAST(__START_AT AS DATE)) AS closed_date
  FROM ${wm.catalog}.wm_silver.households
  WHERE status = 'Closed'
  GROUP BY household_id
)
SELECT
  h.household_id, h.household_name, h.advisor_id, h.segment, h.province, h.status,
  CAST(h.onboard_date AS DATE)                              AS onboard_date,
  c.closed_date,
  CASE
    WHEN months_between(current_date(), CAST(h.onboard_date AS DATE)) < 12 THEN '0-1 yrs'
    WHEN months_between(current_date(), CAST(h.onboard_date AS DATE)) < 60 THEN '1-5 yrs'
    WHEN months_between(current_date(), CAST(h.onboard_date AS DATE)) < 120 THEN '5-10 yrs'
    ELSE '10+ yrs'
  END                                                       AS tenure_band
FROM ${wm.catalog}.wm_silver.households h
LEFT JOIN closed c ON h.household_id = c.household_id
WHERE h.__END_AT IS NULL;

-- Current client attributes (PII stays in gold; masked in wm_semantic serving views)
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.dim_client
COMMENT 'Client dimension (current). PII columns: first_name, last_name, email'
AS
SELECT
  client_id, household_id, first_name, last_name, email, birth_year,
  CASE
    WHEN year(current_date()) - birth_year < 40 THEN 'Under 40'
    WHEN year(current_date()) - birth_year < 55 THEN '40-54'
    WHEN year(current_date()) - birth_year < 70 THEN '55-69'
    ELSE '70+'
  END                                                       AS age_band,
  risk_profile, email_opt_in, is_primary, status
FROM ${wm.catalog}.wm_silver.clients
WHERE __END_AT IS NULL;

CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.dim_account
(CONSTRAINT pk_account EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE)
COMMENT 'Account dimension: registration type, program (managed / fee-based / commission), fee schedule'
AS
SELECT
  a.account_id, a.client_id, a.household_id, a.account_type,
  CASE WHEN a.account_type = 'Non-Registered' THEN 'Non-Registered' ELSE 'Registered' END AS registration_class,
  a.program,
  CASE WHEN a.program = 'Commission' THEN 'Transactional' ELSE 'Fee-Based / Managed' END AS revenue_model,
  a.fee_bps, a.currency, a.open_date, a.close_date, a.status
FROM ${wm.catalog}.wm_silver.accounts a;

CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.dim_security
COMMENT 'Security dimension with asset-class hierarchy'
AS
SELECT
  security_id, ticker, security_name, asset_class,
  CASE WHEN asset_class LIKE '%Equity' THEN 'Equity'
       WHEN asset_class = 'Fixed Income' THEN 'Fixed Income'
       WHEN asset_class = 'Cash & Equivalents' THEN 'Cash'
       ELSE 'Alternatives' END                              AS asset_class_group,
  currency, sector
FROM ${wm.catalog}.wm_silver.securities;
