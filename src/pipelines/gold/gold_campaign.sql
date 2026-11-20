-- =====================================================================================
-- GOLD: measurement-ready panel for incremental impact (treatment vs control, pre vs post)
-- =====================================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_campaign_household_monthly
COMMENT 'Grain: campaign x household x month. Balanced pre/post panel for difference-in-differences; rel_month 0 = launch month'
AS
SELECT
  a.campaign_id, c.campaign_name, c.primary_metric,
  a.household_id, a.assignment_group, a.stratum,
  h.segment, h.month_start,
  CAST(months_between(h.month_start, trunc(c.start_date, 'MM')) AS INT)          AS rel_month,
  CASE WHEN h.month_start < trunc(c.start_date, 'MM') THEN 'pre' ELSE 'post' END AS period,
  h.net_flows_cad, h.gross_inflows_cad, h.deposit_count, h.interactions, h.meetings,
  h.digital_sessions, h.digital_active_flag, h.service_cases, h.complaints,
  h.avg_resolution_hours, h.attrition_flag, h.end_aum_cad
FROM ${wm.catalog}.wm_silver.campaign_assignments a
JOIN ${wm.catalog}.wm_silver.campaigns c           ON a.campaign_id = c.campaign_id
JOIN ${wm.catalog}.wm_gold.fct_household_monthly h ON a.household_id = h.household_id
WHERE h.month_start >= add_months(trunc(c.start_date, 'MM'), -3)
  AND h.month_start <= trunc(c.measurement_end_date, 'MM');

CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_campaign_trend
COMMENT 'Grain: campaign x group x relative month. Average per household, for the treatment-vs-control trend chart'
AS
SELECT
  campaign_id, campaign_name, assignment_group, rel_month, month_start, period,
  count(DISTINCT household_id)                               AS households,
  avg(net_flows_cad)                                         AS avg_net_flows_cad,
  avg(deposit_count)                                         AS avg_deposits,
  avg(interactions)                                          AS avg_interactions,
  avg(digital_sessions)                                      AS avg_digital_sessions,
  avg(digital_active_flag)                                   AS digital_active_rate,
  avg(complaints)                                            AS avg_complaints
FROM ${wm.catalog}.wm_gold.fct_campaign_household_monthly
GROUP BY campaign_id, campaign_name, assignment_group, rel_month, month_start, period;
