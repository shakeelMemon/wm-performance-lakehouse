-- =====================================================================================
-- GOLD: assets, flows, performance and revenue
-- =====================================================================================

-- Account x business day: market value, external flows, fees and the daily net return.
-- r_t = (MV_t - MV_{t-1} - NetExternalFlow_t) / MV_{t-1}   (flows assumed end-of-day)
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_account_daily
(
  CONSTRAINT valid_mv EXPECT (market_value_cad >= 0),
  CONSTRAINT plausible_daily_return EXPECT (daily_return IS NULL OR abs(daily_return) < 0.25)
)
CLUSTER BY (as_of_date, account_id)
COMMENT 'Grain: account x business day. Daily valuation, external flows, fees, commissions and net-of-fee daily return'
AS
WITH flows AS (
  SELECT
    account_id,
    trade_date                                                         AS as_of_date,
    sum(net_flow_cad)                                                  AS net_flow_cad,
    sum(CASE WHEN flow_direction = 'Inflow'  THEN net_flow_cad ELSE 0 END)  AS gross_inflow_cad,
    sum(CASE WHEN flow_direction = 'Outflow' THEN -net_flow_cad ELSE 0 END) AS gross_outflow_cad,
    count_if(txn_type = 'DEPOSIT')                                     AS deposit_count,
    sum(fee_revenue_cad)                                               AS fee_revenue_cad,
    sum(commission_cad)                                                AS commission_cad
  FROM ${wm.catalog}.wm_silver.transactions
  GROUP BY account_id, trade_date
),
v AS (
  SELECT account_id, as_of_date, market_value_cad, cash_cad,
         lag(market_value_cad) OVER (PARTITION BY account_id ORDER BY as_of_date) AS prior_market_value_cad
  FROM ${wm.catalog}.wm_silver.valuations
)
SELECT
  v.account_id,
  v.as_of_date,
  v.market_value_cad,
  v.cash_cad,
  v.prior_market_value_cad,
  coalesce(f.net_flow_cad, 0)                                          AS net_flow_cad,
  coalesce(f.gross_inflow_cad, 0)                                      AS gross_inflow_cad,
  coalesce(f.gross_outflow_cad, 0)                                     AS gross_outflow_cad,
  coalesce(f.deposit_count, 0)                                         AS deposit_count,
  coalesce(f.fee_revenue_cad, 0)                                       AS fee_revenue_cad,
  coalesce(f.commission_cad, 0)                                        AS commission_cad,
  CASE WHEN v.prior_market_value_cad > 0
       THEN CAST(v.market_value_cad - v.prior_market_value_cad - coalesce(f.net_flow_cad, 0) AS DOUBLE)
            / CAST(v.prior_market_value_cad AS DOUBLE)
  END                                                                  AS daily_return
FROM v
LEFT JOIN flows f ON v.account_id = f.account_id AND v.as_of_date = f.as_of_date;

-- Account x month: begin/end value, flows, TWR (geometrically linked daily returns) and Modified Dietz
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_account_monthly_performance
(CONSTRAINT plausible_monthly_twr EXPECT (twr IS NULL OR abs(twr) < 0.6))
CLUSTER BY (month_start)
COMMENT 'Grain: account x month. Begin/end MV, external flows, revenue, time-weighted return and Modified Dietz return'
AS
WITH m AS (
  SELECT
    account_id,
    trunc(as_of_date, 'MM')                                            AS month_start,
    min_by(prior_market_value_cad, as_of_date)                         AS begin_mv,
    max_by(market_value_cad, as_of_date)                               AS end_mv,
    sum(net_flow_cad)                                                  AS net_flows_cad,
    sum(gross_inflow_cad)                                              AS gross_inflows_cad,
    sum(gross_outflow_cad)                                             AS gross_outflows_cad,
    sum(deposit_count)                                                 AS deposit_count,
    sum(fee_revenue_cad)                                               AS fee_revenue_cad,
    sum(commission_cad)                                                AS commission_cad,
    sum(net_flow_cad * datediff(last_day(as_of_date), as_of_date) / day(last_day(as_of_date)))
                                                                       AS weighted_flows_cad,
    exp(sum(ln(1 + daily_return))) - 1                                 AS twr,
    count(daily_return)                                                AS days_measured
  FROM ${wm.catalog}.wm_gold.fct_account_daily
  GROUP BY account_id, trunc(as_of_date, 'MM')
)
SELECT
  m.*,
  coalesce(begin_mv, 0)                                                AS begin_mv_filled,
  end_mv - coalesce(begin_mv, 0) - net_flows_cad                       AS market_movement_cad,
  CASE WHEN coalesce(begin_mv, 0) + weighted_flows_cad > 0
       THEN CAST(end_mv - coalesce(begin_mv, 0) - net_flows_cad AS DOUBLE)
            / CAST(coalesce(begin_mv, 0) + weighted_flows_cad AS DOUBLE)
  END                                                                  AS modified_dietz_return
FROM m;

-- Daily AUM by advisor / segment / program / registration type with AS-WAS branch & region
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_aum_daily
CLUSTER BY (as_of_date)
COMMENT 'Grain: business day x advisor x segment x program x account type. AUM attributed to the branch the advisor belonged to on that day (SCD2)'
AS
WITH agg AS (
  SELECT
    d.as_of_date, h.advisor_id, h.segment, a.program, a.account_type,
    sum(d.market_value_cad)                                            AS aum_cad,
    sum(d.net_flow_cad)                                                AS net_flows_cad,
    count_if(d.market_value_cad > 0)                                   AS funded_accounts,
    count(DISTINCT CASE WHEN d.market_value_cad > 0 THEN a.household_id END) AS funded_households
  FROM ${wm.catalog}.wm_gold.fct_account_daily d
  JOIN ${wm.catalog}.wm_gold.dim_account a   ON d.account_id = a.account_id
  JOIN ${wm.catalog}.wm_gold.dim_household h ON a.household_id = h.household_id
  GROUP BY d.as_of_date, h.advisor_id, h.segment, a.program, a.account_type
)
SELECT
  agg.*,
  ah.branch_id, ah.branch_name, ah.region
FROM agg
LEFT JOIN ${wm.catalog}.wm_gold.dim_advisor_history ah
  ON agg.advisor_id = ah.advisor_id
 AND agg.as_of_date >= ah.valid_from AND agg.as_of_date < ah.valid_to;

-- AUM roll-forward (bridge): begin AUM + net new assets + market movement = end AUM
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_aum_rollforward_monthly
(CONSTRAINT bridge_balances EXPECT (abs(begin_aum_cad + net_flows_cad + market_movement_cad - end_aum_cad) < 1))
COMMENT 'Grain: month x advisor x segment x program x account type. AUM bridge + revenue (fees, commissions). The standard monthly reporting dataset.'
AS
WITH base AS (
  SELECT
    p.month_start, h.advisor_id, h.segment, a.program, a.account_type, a.revenue_model,
    sum(p.begin_mv_filled)                                             AS begin_aum_cad,
    sum(p.end_mv)                                                      AS end_aum_cad,
    sum(p.gross_inflows_cad)                                           AS gross_inflows_cad,
    sum(p.gross_outflows_cad)                                          AS gross_outflows_cad,
    sum(p.net_flows_cad)                                               AS net_flows_cad,
    sum(p.market_movement_cad)                                         AS market_movement_cad,
    sum(p.fee_revenue_cad)                                             AS fee_revenue_cad,
    sum(p.commission_cad)                                              AS commission_revenue_cad,
    sum(p.fee_revenue_cad + p.commission_cad)                          AS total_revenue_cad,
    count_if(p.end_mv > 0)                                             AS funded_accounts,
    count_if(p.begin_mv IS NULL AND p.end_mv > 0)                      AS new_accounts,
    count_if(p.begin_mv > 0 AND p.end_mv = 0)                          AS closed_accounts
  FROM ${wm.catalog}.wm_gold.fct_account_monthly_performance p
  JOIN ${wm.catalog}.wm_gold.dim_account a   ON p.account_id = a.account_id
  JOIN ${wm.catalog}.wm_gold.dim_household h ON a.household_id = h.household_id
  GROUP BY p.month_start, h.advisor_id, h.segment, a.program, a.account_type, a.revenue_model
)
SELECT
  base.*,
  ah.branch_name, ah.region
FROM base
LEFT JOIN ${wm.catalog}.wm_gold.dim_advisor_history ah
  ON base.advisor_id = ah.advisor_id
 AND last_day(base.month_start) >= ah.valid_from AND last_day(base.month_start) < ah.valid_to;

-- Asset mix at month end (from position-level holdings) + reconciliation to valuations
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_asset_mix_monthly
COMMENT 'Grain: month-end x advisor x segment x asset class. Position-level market value from custodian holdings'
AS
SELECT
  trunc(ho.as_of_date, 'MM')                                           AS month_start,
  ho.as_of_date,
  h.advisor_id, h.segment, s.asset_class, s.asset_class_group,
  sum(ho.market_value_cad)                                             AS market_value_cad,
  count(DISTINCT ho.account_id)                                        AS accounts_holding
FROM ${wm.catalog}.wm_silver.holdings ho
JOIN ${wm.catalog}.wm_gold.dim_security s  ON ho.security_id = s.security_id
JOIN ${wm.catalog}.wm_gold.dim_account a   ON ho.account_id = a.account_id
JOIN ${wm.catalog}.wm_gold.dim_household h ON a.household_id = h.household_id
GROUP BY trunc(ho.as_of_date, 'MM'), ho.as_of_date, h.advisor_id, h.segment, s.asset_class, s.asset_class_group;
