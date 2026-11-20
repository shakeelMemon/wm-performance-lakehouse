# Metric definitions

_Generated from `src/semantic/metric_catalog.yml` by `tools/gen_metric_docs.py` — edit the YAML, not this file._

Every certified metric has one definition, one owner and one governed implementation (a measure in a Unity Catalog metric view). Dashboards, Genie, the app and notebooks all read that implementation.

## Quantity metrics

| Metric | Definition | Formula | Grain | Source | Owner |
|---|---|---|---|---|---|
| **Month-End AUM** | Market value of all funded client accounts at the last business day of the month, in CAD. | `SUM(end_aum_cad)` | month (semi-additive — do not sum across months) | `wm_aum_flows_revenue` → Month-End AUM | WM Finance |
| **Average AUM** | Average of beginning and ending AUM, averaged over the months selected. Denominator for yield metrics. | `SUM((begin_aum_cad + end_aum_cad)/2) / COUNT(DISTINCT month_start)` | any period | `wm_aum_flows_revenue` → Average AUM | WM Finance |
| **Net New Assets** | External client money in (deposits, transfers-in) minus money out (withdrawals, transfers-out). Excludes market movement, income, fees and trades. | `SUM(net_flows_cad)` | any period (additive) | `wm_aum_flows_revenue` → Net New Assets | WM Finance |
| **Organic Growth Rate (Annualized)** | Net new assets as a share of beginning AUM, annualized. Isolates growth the business generated from market performance. | `SUM(net_flows_cad) * 12 / SUM(begin_aum_cad)` | any period | `wm_aum_flows_revenue` → Organic Growth Rate (Annualized) | WM Strategy |
| **Market Movement** | Change in AUM not explained by external flows (investment returns net of fees and commissions). Closes the AUM bridge Begin + NNA + Market = End. | `SUM(end_aum_cad - begin_aum_cad - net_flows_cad)` | any period (additive) | `wm_aum_flows_revenue` → Market Movement | WM Finance |
| **Total Revenue** | Fee-based and managed-program fees charged plus trading commissions. | `SUM(fee_revenue_cad + commission_revenue_cad)` | any period (additive) | `wm_aum_flows_revenue` → Total Revenue | WM Finance |
| **Return on Assets (bps)** | Annualized revenue yield on average AUM, in basis points. | `SUM(total_revenue_cad) * 12 * 10000 / SUM((begin_aum_cad + end_aum_cad)/2)` | any period | `wm_aum_flows_revenue` → Return on Assets (bps) | WM Finance |
| **Time-Weighted Return** | Net-of-fee account return that neutralizes the timing of client cash flows (daily returns geometrically linked). Standard for comparing investment performance. | `EXP(SUM(LN(1 + daily_return))) - 1, daily_return = (MV_t - MV_t-1 - Flow_t) / MV_t-1` | account x month (link months for longer periods) | `gold.fct_account_monthly_performance` → twr | WM Investment Performance |
| **Active Households** | Distinct households with at least one funded account at month end. | `COUNT(DISTINCT household_id WHERE end_aum_cad > 0)` | month | `wm_client_experience` → Active Households | WM Client Strategy |
| **Digital Adoption %** | Share of household-months with at least one web or mobile session. | `SUM(digital_active_flag) / COUNT(household-months)` | any period | `wm_client_experience` → Digital Adoption % | WM Digital |
| **Incremental NNA (Campaign)** | Difference-in-differences estimate of extra net new assets per treated household per month versus a randomized control group, with 95% bootstrap confidence interval. | `(post_T - pre_T) - (post_C - pre_C), household-level means` | campaign | `gold.campaign_impact_results` → did_estimate | WM Analytics |

## Quality metrics

| Metric | Definition | Formula | Grain | Source | Owner |
|---|---|---|---|---|---|
| **Meeting Coverage %** | Share of household-months with at least one substantive meeting (meeting, portfolio review or financial plan). | `SUM(meetings > 0) / COUNT(household-months)` | any period | `wm_client_experience` → Meeting Coverage % | WM Client Strategy |
| **Complaints per 100 Households (Monthly)** | Formal complaints opened per 100 households per month. | `SUM(complaints) * 100 / COUNT(household-months)` | any period | `wm_client_experience` → Complaints per 100 Households (Monthly) | WM Client Experience |
| **SLA Breach Rate** | Share of closed service cases resolved in more than 72 hours. | `SUM(sla_breaches) / SUM(cases_closed)` | any period | `wm_service_quality` → SLA Breach Rate | WM Operations |
| **Average Resolution Hours** | Mean hours from case opened to case closed (closed cases only). | `SUM(avg_resolution_hours * cases_closed) / SUM(cases_closed)` | any period | `wm_service_quality` → Average Resolution Hours | WM Operations |
| **NPS** | Relationship Net Promoter Score — % promoters (9-10) minus % detractors (0-6). Report with response count; below 30 responses treat as directional. | `(SUM(promoters) - SUM(detractors)) * 100 / SUM(nps_responses)` | any period (quarter or longer recommended) | `wm_client_experience` → NPS | WM Client Experience |
| **Attrition Rate (Annualized)** | Households that closed their relationship, per household-month, annualized. | `SUM(attrition_flag) * 12 / COUNT(household-months)` | any period | `wm_client_experience` → Attrition Rate (Annualized) | WM Client Strategy |

## Conventions

* **Currency** — CAD. USD positions are converted at the custodian's daily FX rate.
* **Flows** — only external movements count as flows (deposits, withdrawals, transfers in/out). Trades, income, fees and commissions are internal and show up in performance, not flows.
* **Returns** — net of fees and commissions, flows assumed end-of-day, daily returns geometrically linked (TWR).
* **Semi-additive** — AUM is a balance: never sum it across months. Use Average AUM for periods.
* **Rates** — ratios are computed as ratio-of-sums so they stay correct for any time window (never average monthly ratios).
* **Fiscal year** — November to October.
