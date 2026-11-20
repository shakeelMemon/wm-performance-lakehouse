-- =====================================================================================
-- GOLD: client experience, advisor scorecard, service quality, digital engagement
-- Quantity (assets, flows, revenue, activity) and quality (NPS, complaints, SLA) side by side
-- =====================================================================================

-- Household x month: the widest reusable dataset (feeds scorecards, campaigns, Genie, the app)
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_household_monthly
(CONSTRAINT pk EXPECT (household_id IS NOT NULL AND month_start IS NOT NULL) ON VIOLATION FAIL UPDATE)
CLUSTER BY (month_start)
COMMENT 'Grain: household x month. Assets, flows, revenue, engagement, digital, service and NPS signals'
AS
WITH assets AS (
  SELECT a.household_id, p.month_start,
         sum(p.begin_mv_filled)                              AS begin_aum_cad,
         sum(p.end_mv)                                       AS end_aum_cad,
         sum(p.net_flows_cad)                                AS net_flows_cad,
         sum(p.gross_inflows_cad)                            AS gross_inflows_cad,
         sum(p.gross_outflows_cad)                           AS gross_outflows_cad,
         sum(p.deposit_count)                                AS deposit_count,
         sum(p.fee_revenue_cad + p.commission_cad)           AS revenue_cad,
         count_if(p.end_mv > 0)                              AS funded_accounts
  FROM ${wm.catalog}.wm_gold.fct_account_monthly_performance p
  JOIN ${wm.catalog}.wm_gold.dim_account a ON p.account_id = a.account_id
  GROUP BY a.household_id, p.month_start
),
crm AS (
  SELECT household_id, trunc(interaction_date, 'MM') AS month_start,
         count(*)                                                                    AS interactions,
         count_if(interaction_type IN ('Meeting', 'Portfolio Review', 'Financial Plan')) AS meetings,
         count_if(interaction_type = 'Financial Plan')                                AS financial_plans
  FROM ${wm.catalog}.wm_silver.crm_interactions
  GROUP BY household_id, trunc(interaction_date, 'MM')
),
dig AS (
  SELECT household_id, trunc(session_date, 'MM') AS month_start,
         count(*)                                            AS digital_sessions,
         count(DISTINCT client_id)                           AS digital_active_clients
  FROM ${wm.catalog}.wm_silver.digital_sessions
  GROUP BY household_id, trunc(session_date, 'MM')
),
svc AS (
  SELECT household_id, trunc(CAST(opened_ts AS DATE), 'MM') AS month_start,
         count(*)                                            AS service_cases,
         count_if(category = 'Complaint')                    AS complaints,
         count_if(resolution_hours > 72)                     AS sla_breaches,
         avg(resolution_hours)                               AS avg_resolution_hours
  FROM ${wm.catalog}.wm_silver.service_cases
  GROUP BY household_id, trunc(CAST(opened_ts AS DATE), 'MM')
),
nps AS (
  SELECT household_id, trunc(response_date, 'MM') AS month_start,
         count(*)                                            AS nps_responses,
         count_if(score >= 9)                                AS promoters,
         count_if(score <= 6)                                AS detractors,
         avg(score)                                          AS avg_nps_score
  FROM ${wm.catalog}.wm_silver.nps_responses
  GROUP BY household_id, trunc(response_date, 'MM')
)
SELECT
  x.household_id, x.month_start,
  h.advisor_id, h.segment, h.province, h.tenure_band,
  x.begin_aum_cad, x.end_aum_cad, x.net_flows_cad, x.gross_inflows_cad, x.gross_outflows_cad,
  x.deposit_count, x.revenue_cad, x.funded_accounts,
  coalesce(c.interactions, 0)                                AS interactions,
  coalesce(c.meetings, 0)                                    AS meetings,
  coalesce(c.financial_plans, 0)                             AS financial_plans,
  coalesce(d.digital_sessions, 0)                            AS digital_sessions,
  coalesce(d.digital_active_clients, 0)                      AS digital_active_clients,
  CASE WHEN coalesce(d.digital_sessions, 0) > 0 THEN 1 ELSE 0 END AS digital_active_flag,
  coalesce(s.service_cases, 0)                               AS service_cases,
  coalesce(s.complaints, 0)                                  AS complaints,
  coalesce(s.sla_breaches, 0)                                AS sla_breaches,
  s.avg_resolution_hours,
  coalesce(n.nps_responses, 0)                               AS nps_responses,
  coalesce(n.promoters, 0)                                   AS promoters,
  coalesce(n.detractors, 0)                                  AS detractors,
  CASE WHEN trunc(h.onboard_date, 'MM') = x.month_start THEN 1 ELSE 0 END AS new_household_flag,
  CASE WHEN h.closed_date IS NOT NULL AND trunc(h.closed_date, 'MM') = x.month_start THEN 1 ELSE 0 END
                                                             AS attrition_flag
FROM assets x
JOIN ${wm.catalog}.wm_gold.dim_household h ON x.household_id = h.household_id
LEFT JOIN crm c ON x.household_id = c.household_id AND x.month_start = c.month_start
LEFT JOIN dig d ON x.household_id = d.household_id AND x.month_start = d.month_start
LEFT JOIN svc s ON x.household_id = s.household_id AND x.month_start = s.month_start
LEFT JOIN nps n ON x.household_id = n.household_id AND x.month_start = n.month_start;

-- Advisor balanced scorecard: quantity (growth, revenue, activity) + quality (NPS, complaints, SLA)
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_advisor_scorecard_monthly
COMMENT 'Grain: advisor x month. Balanced scorecard with quantity and quality metrics and within-region percentile ranks'
AS
WITH a AS (
  SELECT
    advisor_id, month_start,
    count_if(end_aum_cad > 0)                                AS households,
    sum(begin_aum_cad)                                       AS begin_aum_cad,
    sum(end_aum_cad)                                         AS end_aum_cad,
    sum(net_flows_cad)                                       AS net_flows_cad,
    sum(gross_inflows_cad)                                   AS gross_inflows_cad,
    sum(revenue_cad)                                         AS revenue_cad,
    sum(interactions)                                        AS interactions,
    count_if(meetings > 0)                                   AS households_met,
    sum(financial_plans)                                     AS financial_plans,
    sum(digital_active_flag)                                 AS digital_active_households,
    sum(service_cases)                                       AS service_cases,
    sum(complaints)                                          AS complaints,
    sum(sla_breaches)                                        AS sla_breaches,
    sum(nps_responses)                                       AS nps_responses,
    sum(promoters)                                           AS promoters,
    sum(detractors)                                          AS detractors,
    sum(new_household_flag)                                  AS new_households,
    sum(attrition_flag)                                      AS attrited_households
  FROM ${wm.catalog}.wm_gold.fct_household_monthly
  GROUP BY advisor_id, month_start
),
k AS (
  SELECT
    a.*,
    ah.branch_name, ah.region,
    try_divide(net_flows_cad, begin_aum_cad) * 12                              AS organic_growth_annualized,
    try_divide(revenue_cad, (begin_aum_cad + end_aum_cad) / 2) * 12 * 10000    AS roa_bps,
    try_divide(interactions, households)                                       AS interactions_per_household,
    try_divide(households_met, households)                                     AS meeting_coverage_pct,
    try_divide(digital_active_households, households)                          AS digital_adoption_pct,
    try_divide(complaints, households) * 100                                   AS complaints_per_100_households,
    try_divide(sla_breaches, service_cases)                                    AS sla_breach_rate,
    try_divide(promoters - detractors, nps_responses) * 100                    AS nps
  FROM a
  LEFT JOIN ${wm.catalog}.wm_gold.dim_advisor_history ah
    ON a.advisor_id = ah.advisor_id
   AND last_day(a.month_start) >= ah.valid_from AND last_day(a.month_start) < ah.valid_to
)
SELECT
  k.*,
  percent_rank() OVER (PARTITION BY month_start, region ORDER BY organic_growth_annualized) AS pr_organic_growth,
  percent_rank() OVER (PARTITION BY month_start, region ORDER BY roa_bps)                   AS pr_roa,
  percent_rank() OVER (PARTITION BY month_start, region ORDER BY meeting_coverage_pct)      AS pr_meeting_coverage,
  percent_rank() OVER (PARTITION BY month_start, region ORDER BY complaints_per_100_households DESC) AS pr_low_complaints,
  percent_rank() OVER (PARTITION BY month_start, region ORDER BY coalesce(sla_breach_rate, 0) DESC) AS pr_low_sla_breach,
  round(100 * (percent_rank() OVER (PARTITION BY month_start, region ORDER BY organic_growth_annualized)
             + percent_rank() OVER (PARTITION BY month_start, region ORDER BY roa_bps)) / 2, 1)  AS quantity_score,
  round(100 * (percent_rank() OVER (PARTITION BY month_start, region ORDER BY meeting_coverage_pct)
             + percent_rank() OVER (PARTITION BY month_start, region ORDER BY complaints_per_100_households DESC)
             + percent_rank() OVER (PARTITION BY month_start, region ORDER BY coalesce(sla_breach_rate, 0) DESC)) / 3, 1)
                                                                               AS quality_score
FROM k;

-- Service quality: volumes, resolution time, SLA adherence
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_service_quality_monthly
COMMENT 'Grain: month x region x case category x channel. Case volumes, resolution time distribution and 72h SLA adherence'
AS
SELECT
  trunc(CAST(s.opened_ts AS DATE), 'MM')                     AS month_start,
  adv.region, s.category, s.channel,
  count(*)                                                   AS cases_opened,
  count_if(s.closed_ts IS NOT NULL)                          AS cases_closed,
  count_if(s.closed_ts IS NULL)                              AS cases_open,
  avg(s.resolution_hours)                                    AS avg_resolution_hours,
  percentile_approx(s.resolution_hours, 0.5)                 AS median_resolution_hours,
  percentile_approx(s.resolution_hours, 0.9)                 AS p90_resolution_hours,
  count_if(s.resolution_hours > 72)                          AS sla_breaches,
  try_divide(count_if(s.resolution_hours > 72), count_if(s.closed_ts IS NOT NULL)) AS sla_breach_rate
FROM ${wm.catalog}.wm_silver.service_cases s
LEFT JOIN ${wm.catalog}.wm_gold.dim_advisor adv ON s.advisor_id = adv.advisor_id
GROUP BY trunc(CAST(s.opened_ts AS DATE), 'MM'), adv.region, s.category, s.channel;

-- Digital engagement by segment / age band / channel / feature
CREATE OR REFRESH MATERIALIZED VIEW ${wm.catalog}.wm_gold.fct_digital_engagement_monthly
COMMENT 'Grain: month x segment x age band x channel x feature. Sessions, active clients, average session length'
AS
SELECT
  trunc(d.session_date, 'MM')                                AS month_start,
  h.segment, c.age_band, d.channel, d.primary_feature,
  count(*)                                                   AS sessions,
  count(DISTINCT d.client_id)                                AS active_clients,
  avg(d.duration_sec) / 60.0                                 AS avg_session_minutes
FROM ${wm.catalog}.wm_silver.digital_sessions d
JOIN ${wm.catalog}.wm_gold.dim_client c    ON d.client_id = c.client_id
JOIN ${wm.catalog}.wm_gold.dim_household h ON d.household_id = h.household_id
GROUP BY trunc(d.session_date, 'MM'), h.segment, c.age_band, d.channel, d.primary_feature;
