"""Dashboard-as-code: generates dashboards/wm_performance.lvdash.json (Databricks AI/BI).

Every dataset reads only from the governed semantic layer (wm_semantic metric views and
secure views), so business viewers need no access to gold and row-level security applies.

    python tools/build_dashboard.py
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "dashboards" / "wm_performance.lvdash.json"
LAST_COMPLETE_MONTH = "add_months(trunc(current_date(), 'MM'), -1)"

DATASETS = {
    "kpi_latest": ("Headline KPIs (last complete month)", f"""
SELECT
  MEASURE(`Month-End AUM`)                     AS month_end_aum,
  MEASURE(`Net New Assets`)                    AS net_new_assets,
  MEASURE(`Organic Growth Rate (Annualized)`)  AS organic_growth,
  MEASURE(`Return on Assets (bps)`)            AS roa_bps,
  MEASURE(`Total Revenue`)                     AS total_revenue
FROM wm_semantic.wm_aum_flows_revenue
WHERE `Month` = {LAST_COMPLETE_MONTH}"""),
    "flows_monthly": ("AUM bridge by month", """
SELECT `Month` AS month, `Region` AS region, `Client Segment` AS segment, `Program` AS program,
       MEASURE(`Net New Assets`)   AS net_new_assets,
       MEASURE(`Gross Inflows`)    AS gross_inflows,
       MEASURE(`Gross Outflows`)   AS gross_outflows,
       MEASURE(`Market Movement`)  AS market_movement,
       MEASURE(`Month-End AUM`)    AS month_end_aum,
       MEASURE(`Total Revenue`)    AS total_revenue
FROM wm_semantic.wm_aum_flows_revenue
GROUP BY ALL"""),
    "aum_daily": ("Daily AUM", """
SELECT as_of_date, region, segment, program, sum(aum_cad) AS aum_cad
FROM wm_semantic.v_aum_daily
GROUP BY ALL"""),
    "asset_mix": ("Asset mix (latest month end)", """
SELECT asset_class, segment, sum(market_value_cad) AS market_value_cad
FROM wm_semantic.v_asset_mix
WHERE as_of_date = (SELECT max(as_of_date) FROM wm_semantic.v_asset_mix)
GROUP BY ALL"""),
    "scorecard": ("Advisor scorecard (last complete month)", f"""
SELECT advisor_name, region, branch_name, households, end_aum_cad, net_flows_cad,
       round(organic_growth_annualized * 100, 1) AS organic_growth_pct,
       round(roa_bps, 1) AS roa_bps,
       round(meeting_coverage_pct * 100, 1) AS meeting_coverage_pct,
       round(digital_adoption_pct * 100, 1) AS digital_adoption_pct,
       round(complaints_per_100_households, 2) AS complaints_per_100_hh,
       quantity_score, quality_score
FROM wm_semantic.v_advisor_scorecard
WHERE month_start = {LAST_COMPLETE_MONTH}"""),
    "experience_monthly": ("Client experience by month", """
SELECT `Month` AS month, `Region` AS region, `Client Segment` AS segment,
       MEASURE(`NPS`)                                      AS nps,
       MEASURE(`NPS Responses`)                            AS nps_responses,
       MEASURE(`Digital Adoption %`)                       AS digital_adoption,
       MEASURE(`Meeting Coverage %`)                       AS meeting_coverage,
       MEASURE(`Complaints per 100 Households (Monthly)`)  AS complaints_per_100_hh,
       MEASURE(`Attrition Rate (Annualized)`)              AS attrition_rate
FROM wm_semantic.wm_client_experience
GROUP BY ALL"""),
    "service_monthly": ("Service quality by month", """
SELECT `Month` AS month, `Region` AS region, `Case Category` AS category,
       MEASURE(`Cases Opened`) AS cases_opened,
       MEASURE(`Average Resolution Hours`) AS avg_resolution_hours,
       MEASURE(`SLA Breach Rate`) AS sla_breach_rate
FROM wm_semantic.wm_service_quality
GROUP BY ALL"""),
    "campaign_impact": ("Campaign incremental impact", """
SELECT campaign_id, campaign_name, metric, dimension, verdict,
       round(did_estimate, 3) AS incremental_per_hh_month,
       round(ci_low, 3) AS ci_low, round(ci_high, 3) AS ci_high,
       round(relative_lift * 100, 1) AS relative_lift_pct,
       round(total_incremental, 0) AS total_incremental,
       n_treatment, n_control, pre_trend_ok, is_primary_metric
FROM wm_semantic.v_campaign_impact"""),
    "campaign_trend": ("Campaign treatment vs control trend", """
SELECT campaign_id, campaign_name, assignment_group, rel_month,
       avg_net_flows_cad, avg_digital_sessions, avg_interactions, avg_complaints
FROM wm_semantic.v_campaign_trend"""),
    "platform_health": ("Latest data-health checks", """
SELECT check_name, category, severity, status, observed_value, threshold, details, checked_at
FROM wm_semantic.v_platform_health
WHERE run_id = (SELECT max_by(run_id, checked_at) FROM wm_semantic.v_platform_health)"""),
    "health_history": ("Data-health history", """
SELECT date(checked_at) AS check_date, category,
       count_if(status = 'FAIL') AS failed_checks, count(*) AS checks
FROM wm_semantic.v_platform_health
GROUP BY ALL"""),
}


# ------------------------------------------------------------------------------ helpers
def f(name, expr=None):
    return {"name": name, "expression": expr or f"`{name}`"}


def q(ds, fields, disaggregated=False, qname="main_query"):
    return [{"name": qname, "query": {"datasetName": ds, "fields": fields, "disaggregated": disaggregated}}]


def frame(title, desc=None):
    fr = {"showTitle": True, "title": title}
    if desc:
        fr.update(showDescription=True, description=desc)
    return fr


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def text(name, lines, p):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}}, "position": p}


def counter(name, ds, field, title, fmt_expr, p, desc=None):
    return {"widget": {"name": name, "queries": q(ds, [f(field, fmt_expr)]),
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": field, "displayName": title}},
                                "frame": frame(title, desc)}}, "position": p}


def chart(name, kind, ds, x, y, title, p, color=None, x_temporal=True, desc=None, y_title=None):
    fields = [f(x[0], x[1]), f(y[0], y[1])] + ([f(color)] if color else [])
    enc = {"x": {"fieldName": x[0], "scale": {"type": "temporal" if x_temporal else "categorical"},
                 "displayName": x[2]},
           "y": {"fieldName": y[0], "scale": {"type": "quantitative"}, "displayName": y_title or y[2]}}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ").title()}
    return {"widget": {"name": name, "queries": q(ds, fields),
                       "spec": {"version": 3, "widgetType": kind, "encodings": enc, "frame": frame(title, desc)}},
            "position": p}


def scatter(name, ds, x, y, color, title, p, desc=None):
    return {"widget": {"name": name, "queries": q(ds, [f(x), f(y), f(color), f("advisor_name")], disaggregated=True),
                       "spec": {"version": 3, "widgetType": "scatter",
                                "encodings": {"x": {"fieldName": x, "scale": {"type": "quantitative"}, "displayName": "Quantity score"},
                                              "y": {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": "Quality score"},
                                              "color": {"fieldName": color, "scale": {"type": "categorical"}, "displayName": "Region"}},
                                "frame": frame(title, desc)}}, "position": p}


def table(name, ds, cols, title, p, desc=None):
    columns = []
    for i, (c, label, typ) in enumerate(cols):
        columns.append({"fieldName": c, "type": typ, "displayAs": typ, "visible": True, "order": 100000 + i,
                        "title": label, "displayName": label, "alignContent": "right" if typ in ("float", "integer") else "left",
                        "allowSearch": typ == "string", "booleanValues": ["false", "true"]})
    return {"widget": {"name": name, "queries": q(ds, [f(c) for c, _, _ in cols], disaggregated=True),
                       "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": columns},
                                "itemsPerPage": 25, "condensed": True, "withRowNumber": False,
                                "frame": frame(title, desc)}}, "position": p}


def multi_filter(name, field, title, targets, p):
    queries = [{"name": f"{name}_{ds}", "query": {"datasetName": ds, "fields": [f(field)], "disaggregated": False}}
               for ds in targets]
    return {"widget": {"name": name, "queries": queries,
                       "spec": {"version": 2, "widgetType": "filter-multi-select",
                                "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": qq["name"]}
                                                         for qq in queries]},
                                "frame": frame(title)}}, "position": p}


# ------------------------------------------------------------------------------ pages
def pages():
    month = ("monthly(month)", "DATE_TRUNC(\"MONTH\", `month`)", "Month")
    overview = [
        text("hdr_overview", ["## Wealth Management — Executive Overview",
                              "Certified metrics from the `wm_semantic` layer. KPIs show the **last complete month**; "
                              "AUM is semi-additive (month-end), flows and revenue are additive."], pos(0, 0, 6, 2)),
        counter("kpi_aum", "kpi_latest", "month_end_aum", "Month-End AUM", "SUM(`month_end_aum`)", pos(0, 2, 2, 3)),
        counter("kpi_nna", "kpi_latest", "net_new_assets", "Net New Assets", "SUM(`net_new_assets`)", pos(2, 2, 1, 3)),
        counter("kpi_ogr", "kpi_latest", "organic_growth", "Organic Growth (ann.)", "SUM(`organic_growth`)", pos(3, 2, 1, 3)),
        counter("kpi_roa", "kpi_latest", "roa_bps", "ROA (bps)", "SUM(`roa_bps`)", pos(4, 2, 1, 3)),
        counter("kpi_rev", "kpi_latest", "total_revenue", "Revenue", "SUM(`total_revenue`)", pos(5, 2, 1, 3)),
        multi_filter("flt_region", "region", "Region", ["flows_monthly", "aum_daily"], pos(0, 5, 3, 1)),
        multi_filter("flt_segment", "segment", "Client segment", ["flows_monthly", "aum_daily"], pos(3, 5, 3, 1)),
        chart("aum_trend", "area", "aum_daily", ("as_of_date", "`as_of_date`", "Date"),
              ("sum(aum_cad)", "SUM(`aum_cad`)", "AUM (CAD)"), "Daily AUM by segment", pos(0, 6, 6, 6), color="segment"),
        chart("nna_by_segment", "bar", "flows_monthly", month,
              ("sum(net_new_assets)", "SUM(`net_new_assets`)", "Net new assets (CAD)"),
              "Net new assets by month and segment", pos(0, 12, 3, 6), color="segment"),
        chart("market_vs_flows", "bar", "flows_monthly", month,
              ("sum(market_movement)", "SUM(`market_movement`)", "Market movement (CAD)"),
              "Market movement by month (AUM bridge component)", pos(3, 12, 3, 6), color="program"),
        chart("asset_mix", "bar", "asset_mix", ("asset_class", "`asset_class`", "Asset class"),
              ("sum(market_value_cad)", "SUM(`market_value_cad`)", "Market value (CAD)"),
              "Asset mix at latest month end", pos(0, 18, 6, 5), color="segment", x_temporal=False),
    ]
    advisors = [
        text("hdr_adv", ["## Advisor Balanced Scorecard",
                         "Quantity score = percentile of organic growth and ROA within region. "
                         "Quality score = percentile of meeting coverage, low complaints and low SLA breaches. "
                         "Row-level security: advisors see only their own book."], pos(0, 0, 6, 2)),
        scatter("adv_quadrant", "scorecard", "quantity_score", "quality_score", "region",
                "Quantity vs. quality (last complete month)", pos(0, 2, 3, 7),
                desc="Top-right = growing the book without sacrificing client experience"),
        chart("adv_growth", "bar", "scorecard", ("advisor_name", "`advisor_name`", "Advisor"),
              ("sum(net_flows_cad)", "SUM(`net_flows_cad`)", "Net flows (CAD)"),
              "Net flows by advisor", pos(3, 2, 3, 7), color="region", x_temporal=False),
        table("adv_table", "scorecard", [
            ("advisor_name", "Advisor", "string"), ("region", "Region", "string"), ("branch_name", "Branch", "string"),
            ("households", "Households", "integer"), ("end_aum_cad", "AUM", "float"),
            ("net_flows_cad", "Net flows", "float"), ("organic_growth_pct", "Organic growth %", "float"),
            ("roa_bps", "ROA bps", "float"), ("meeting_coverage_pct", "Meeting coverage %", "float"),
            ("digital_adoption_pct", "Digital adoption %", "float"),
            ("complaints_per_100_hh", "Complaints /100 HH", "float"),
            ("quantity_score", "Quantity score", "float"), ("quality_score", "Quality score", "float")],
            "Advisor scorecard", pos(0, 9, 6, 9)),
    ]
    experience = [
        text("hdr_exp", ["## Client Experience & Service Quality",
                         "Quality dimension: NPS (show with response counts), engagement, complaints, SLA adherence."],
             pos(0, 0, 6, 2)),
        multi_filter("flt_exp_region", "region", "Region", ["experience_monthly", "service_monthly"], pos(0, 2, 6, 1)),
        chart("nps_trend", "line", "experience_monthly", month, ("avg(nps)", "AVG(`nps`)", "NPS"),
              "NPS by segment", pos(0, 3, 3, 6), color="segment"),
        chart("digital_trend", "line", "experience_monthly", month,
              ("avg(digital_adoption)", "AVG(`digital_adoption`)", "Digital adoption"),
              "Digital adoption by segment", pos(3, 3, 3, 6), color="segment"),
        chart("complaints_trend", "bar", "experience_monthly", month,
              ("avg(complaints_per_100_hh)", "AVG(`complaints_per_100_hh`)", "Complaints per 100 HH"),
              "Complaints per 100 households", pos(0, 9, 3, 6), color="region"),
        chart("sla_trend", "line", "service_monthly", month,
              ("avg(sla_breach_rate)", "AVG(`sla_breach_rate`)", "SLA breach rate"),
              "72-hour SLA breach rate by case category", pos(3, 9, 3, 6), color="category"),
    ]
    impact = [
        text("hdr_imp", ["## Incremental Impact of Initiatives",
                         "Randomized treatment vs. control, household-level difference-in-differences with 95% "
                         "bootstrap CI. Quality metrics are guardrails: a significant adverse move is flagged."],
             pos(0, 0, 6, 2)),
        multi_filter("flt_campaign", "campaign_name", "Campaign", ["campaign_impact", "campaign_trend"], pos(0, 2, 6, 1)),
        chart("camp_flows", "line", "campaign_trend", ("rel_month", "`rel_month`", "Months from launch"),
              ("avg(avg_net_flows_cad)", "AVG(`avg_net_flows_cad`)", "Avg net flows / household"),
              "Net flows per household: treatment vs control", pos(0, 3, 3, 6), color="assignment_group",
              x_temporal=False),
        chart("camp_digital", "line", "campaign_trend", ("rel_month", "`rel_month`", "Months from launch"),
              ("avg(avg_digital_sessions)", "AVG(`avg_digital_sessions`)", "Avg sessions / household"),
              "Digital sessions per household: treatment vs control", pos(3, 3, 3, 6), color="assignment_group",
              x_temporal=False),
        table("camp_table", "campaign_impact", [
            ("campaign_name", "Campaign", "string"), ("metric", "Metric", "string"),
            ("dimension", "Dimension", "string"), ("incremental_per_hh_month", "Incremental / HH / month", "float"),
            ("ci_low", "95% CI low", "float"), ("ci_high", "95% CI high", "float"),
            ("relative_lift_pct", "Lift %", "float"), ("total_incremental", "Total incremental", "float"),
            ("verdict", "Verdict", "string"), ("pre_trend_ok", "Parallel pre-trend", "string"),
            ("n_treatment", "Treated HH", "integer"), ("n_control", "Control HH", "integer")],
            "Impact results", pos(0, 9, 6, 8)),
    ]
    health = [
        text("hdr_ops", ["## Platform Health",
                         "Freshness, volume, reconciliation, integrity and Lakeflow expectation checks from the daily run. "
                         "Critical failures stop the job before numbers reach this dashboard."], pos(0, 0, 6, 2)),
        chart("ops_hist", "bar", "health_history", ("check_date", "`check_date`", "Date"),
              ("sum(failed_checks)", "SUM(`failed_checks`)", "Failed checks"),
              "Failed checks per run by category", pos(0, 2, 6, 5), color="category"),
        table("ops_table", "platform_health", [
            ("check_name", "Check", "string"), ("category", "Category", "string"),
            ("severity", "Severity", "string"), ("status", "Status", "string"),
            ("observed_value", "Observed", "float"), ("threshold", "Threshold", "string"),
            ("details", "Details", "string")], "Latest run", pos(0, 7, 6, 10)),
    ]
    return [
        {"name": "overview", "displayName": "Executive Overview", "pageType": "PAGE_TYPE_CANVAS", "layout": overview},
        {"name": "advisors", "displayName": "Advisor Scorecard", "pageType": "PAGE_TYPE_CANVAS", "layout": advisors},
        {"name": "experience", "displayName": "Client Experience & Service", "pageType": "PAGE_TYPE_CANVAS", "layout": experience},
        {"name": "impact", "displayName": "Initiative Impact", "pageType": "PAGE_TYPE_CANVAS", "layout": impact},
        {"name": "health", "displayName": "Platform Health", "pageType": "PAGE_TYPE_CANVAS", "layout": health},
    ]


def build() -> dict:
    ds = [{"name": k, "displayName": v[0], "queryLines": [ln + "\n" for ln in v[1].strip().splitlines()]}
          for k, v in DATASETS.items()]
    return {"datasets": ds, "pages": pages(), "uiSettings": {"theme": {"widgetHeaderAlignment": "LEFT"}}}


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2))
    print(f"wrote {OUT}")
