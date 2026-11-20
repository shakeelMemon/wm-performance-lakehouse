-- Trusted SQL examples for the "WM Performance" Genie space.
-- Each block: the question it answers, then the certified query.

-- Q: Net new assets and organic growth by region last quarter
SELECT `Region`,
       MEASURE(`Net New Assets`)                   AS net_new_assets,
       MEASURE(`Organic Growth Rate (Annualized)`) AS organic_growth
FROM wm_semantic.wm_aum_flows_revenue
WHERE `Month` >= add_months(trunc(current_date(), 'QUARTER'), -3)
  AND `Month` <  trunc(current_date(), 'QUARTER')
GROUP BY ALL
ORDER BY net_new_assets DESC;

-- Q: Month-end AUM by segment, last 12 complete months (semi-additive: one row per month)
SELECT `Month`, `Client Segment`, MEASURE(`Month-End AUM`) AS month_end_aum
FROM wm_semantic.wm_aum_flows_revenue
WHERE `Month` >= add_months(trunc(current_date(), 'MM'), -12)
  AND `Month` <  trunc(current_date(), 'MM')
GROUP BY ALL
ORDER BY `Month`, `Client Segment`;

-- Q: Top advisors by ROA last month, with NPS and response count
SELECT a.`Advisor`, a.roa_bps, e.nps, e.nps_responses
FROM (SELECT `Advisor`, MEASURE(`Return on Assets (bps)`) AS roa_bps
      FROM wm_semantic.wm_aum_flows_revenue
      WHERE `Month` = add_months(trunc(current_date(), 'MM'), -1) GROUP BY ALL) a
LEFT JOIN (SELECT `Advisor`, MEASURE(`NPS`) AS nps, MEASURE(`NPS Responses`) AS nps_responses
           FROM wm_semantic.wm_client_experience
           WHERE `Month` >= add_months(trunc(current_date(), 'MM'), -12) GROUP BY ALL) e
  ON a.`Advisor` = e.`Advisor`
ORDER BY a.roa_bps DESC
LIMIT 10;

-- Q: Growing advisors with low client contact (quantity vs quality tension)
SELECT advisor_name, region,
       round(organic_growth_annualized * 100, 1) AS organic_growth_pct,
       round(meeting_coverage_pct * 100, 1)      AS meeting_coverage_pct
FROM wm_semantic.v_advisor_scorecard
WHERE month_start = add_months(trunc(current_date(), 'MM'), -1)
  AND organic_growth_annualized > 0.10
  AND meeting_coverage_pct < 0.30
ORDER BY organic_growth_annualized DESC;

-- Q: Campaign incremental impact, quantity and quality
SELECT campaign_name, metric, dimension,
       round(did_estimate, 2) AS incremental_per_household_per_month,
       round(ci_low, 2) AS ci_low, round(ci_high, 2) AS ci_high,
       round(relative_lift * 100, 1) AS lift_pct, verdict
FROM wm_semantic.v_campaign_impact
ORDER BY campaign_name, dimension DESC, metric;

-- Q: SLA breach rate by case category, current fiscal year
SELECT `Case Category`, MEASURE(`Cases Opened`) AS cases, MEASURE(`SLA Breach Rate`) AS sla_breach_rate
FROM wm_semantic.wm_service_quality
WHERE `Month` >= make_date(CASE WHEN month(current_date()) >= 11 THEN year(current_date())
                                ELSE year(current_date()) - 1 END, 11, 1)
GROUP BY ALL
ORDER BY sla_breach_rate DESC;
