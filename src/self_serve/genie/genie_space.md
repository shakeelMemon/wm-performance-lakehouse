# Genie space: **WM Performance**

Natural-language self-serve for Wealth Management leaders and analysts. Genie answers
from the certified semantic layer, so "net new assets" means the same thing in Genie,
the dashboard, the app and every notebook.

## Setup (≈5 minutes, after the bootstrap job)

1. **Genie → New space** · Title `WM Performance` · Warehouse: `WM Reporting Warehouse`.
2. **Data** — add only governed objects:
   - `wm_semantic.wm_aum_flows_revenue` (metric view)
   - `wm_semantic.wm_client_experience` (metric view)
   - `wm_semantic.wm_service_quality` (metric view)
   - `wm_semantic.v_advisor_scorecard`, `wm_semantic.v_household_book` (row-filtered views)
   - `wm_semantic.v_campaign_impact`, `wm_semantic.metric_catalog`
3. **Instructions** — paste the block below.
4. **SQL examples** — add each query from `trusted_queries.sql` as a trusted example.
5. **Sample questions** — add the list below.
6. Share with `wm_business_users` (CAN RUN). Put the space URL in the app's `WM_GENIE_URL` env var.

## General instructions (paste into Genie)

```
You answer questions about the Wealth Management business in Canada (CAD).
- Always use MEASURE() on the metric views for certified metrics; never re-derive a metric
  that exists as a measure (e.g. Net New Assets, Organic Growth Rate, ROA, NPS).
- AUM is semi-additive: for AUM "now" or "at the end of <period>" filter to the single
  last month of the period and use `Month-End AUM`. For AUM over a period use `Average AUM`.
- "Last month" / "last quarter" mean the last COMPLETE calendar month / quarter.
- Fiscal year runs November to October (FY2026 = Nov 2025 - Oct 2026). Use `Fiscal Year`
  and `Fiscal Quarter` when the user says fiscal/FY.
- Quantity metrics: AUM, flows, revenue, households, interactions, digital sessions.
  Quality metrics: NPS, meeting coverage, complaints, SLA breach rate, attrition.
  When asked "how is X performing", show one quantity and one quality metric.
- NPS: always show NPS Responses alongside NPS; flag results with fewer than 30 responses.
- Campaign impact: report did_estimate with ci_low/ci_high and the verdict. Do not claim an
  effect when the verdict is "No detectable effect".
- Segments: Mass Affluent, High Net Worth, Ultra High Net Worth.
  Programs: Discretionary Managed, Fee-Based, Commission.
- Format currency as $X.XM for values above one million.
```

## Sample questions

- What were net new assets and organic growth by region last quarter?
- Show month-end AUM by client segment for the last 12 months.
- Which 10 advisors had the highest ROA last month, and what was their NPS?
- Which advisors have organic growth above 10% but meeting coverage below 30%?
- How has digital adoption changed for clients under 40 since January?
- What is the SLA breach rate by case category this fiscal year?
- Did the Registered Plan Consolidation Outreach increase net flows? Did complaints change?
- What does "Organic Growth Rate" mean and how is it calculated?
