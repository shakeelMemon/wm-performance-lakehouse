"""WM Insights — self-serve Databricks App for advisors, branch managers and analysts.

* Book explorer: an advisor's (or region's) households, AUM, flows, engagement and service
  signals, with "who needs attention" flags.
* Metric glossary: the certified metric catalog, searchable, so everyone uses one definition.
* Governed export: download exactly what you can see as CSV.

Runs with on-behalf-of-user auth, so Unity Catalog row filters in wm_semantic apply: an
advisor only ever sees their own book.
"""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

CATALOG = os.getenv("WM_CATALOG", "wm_reporting_dev")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
SEM = f"{CATALOG}.wm_semantic"
cfg = Config()

st.set_page_config(page_title="WM Insights", layout="wide")


def _token() -> str | None:
    # Databricks Apps forwards the signed-in user's token when user authorization is enabled
    return st.context.headers.get("X-Forwarded-Access-Token")


@st.cache_data(ttl=600, show_spinner=False)
def query(statement: str, params: tuple = (), user_token: str | None = None) -> pd.DataFrame:
    conn_args = dict(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}")
    if user_token:
        conn_args["access_token"] = user_token
    else:
        conn_args["credentials_provider"] = lambda: cfg.authenticate
    with sql.connect(**conn_args) as conn, conn.cursor() as cur:
        cur.execute(statement, list(params))
        return cur.fetchall_arrow().to_pandas()


def q(statement: str, *params) -> pd.DataFrame:
    return query(statement, tuple(params), _token())


def money(x: float) -> str:
    return f"${x / 1e6:,.1f}M" if abs(x) >= 1e6 else f"${x:,.0f}"


st.title("WM Insights")
st.caption(f"Certified data from `{SEM}` · refreshed daily after the 06:30 pipeline run")

tab_book, tab_glossary, tab_ask = st.tabs(["Book explorer", "Metric glossary", "Ask a question"])

# ------------------------------------------------------------------------------ book explorer
with tab_book:
    months = q(f"SELECT DISTINCT month_start FROM {SEM}.v_household_book ORDER BY month_start DESC")
    if months.empty:
        st.info("No households are visible to you. Ask WM Data to map your advisor ID.")
        st.stop()
    c1, c2, c3 = st.columns(3)
    month = c1.selectbox("Month", months.month_start.tolist(), format_func=lambda d: d.strftime("%b %Y"))
    advisors = q(f"SELECT DISTINCT advisor_name FROM {SEM}.v_household_book ORDER BY 1")
    advisor = c2.selectbox("Advisor", ["All visible advisors"] + advisors.advisor_name.tolist())
    segment = c3.multiselect("Segment", ["Mass Affluent", "High Net Worth", "Ultra High Net Worth"])

    where, params = ["month_start = ?"], [month]
    if advisor != "All visible advisors":
        where.append("advisor_name = ?")
        params.append(advisor)
    if segment:
        where.append("segment IN (" + ",".join("?" * len(segment)) + ")")
        params += segment
    book = q(f"""SELECT household_id, household_name, advisor_name, segment, tenure_band,
                        end_aum_cad, net_flows_cad, revenue_cad, interactions, meetings,
                        digital_sessions, service_cases, complaints, attrition_flag
                 FROM {SEM}.v_household_book WHERE {' AND '.join(where)}
                 ORDER BY end_aum_cad DESC""", *params)

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Households", f"{(book.end_aum_cad > 0).sum():,}")
    k2.metric("AUM", money(book.end_aum_cad.sum()))
    k3.metric("Net new assets", money(book.net_flows_cad.sum()))
    k4.metric("Meeting coverage", f"{(book.meetings > 0).mean():.0%}" if len(book) else "–")
    k5.metric("Complaints", int(book.complaints.sum()))

    # "Needs attention": large outflow, complaint, or no contact for a high-value household
    book["attention"] = ""
    big = book.end_aum_cad > book.end_aum_cad.quantile(0.8)
    book.loc[book.net_flows_cad < -0.1 * book.end_aum_cad.clip(lower=1), "attention"] += "Large outflow · "
    book.loc[book.complaints > 0, "attention"] += "Complaint · "
    book.loc[big & (book.interactions == 0), "attention"] += "Top-20% HH, no contact · "
    book["attention"] = book.attention.str.rstrip(" ·")

    only_flagged = st.toggle("Show only households needing attention")
    view = book[book.attention != ""] if only_flagged else book
    st.dataframe(view, use_container_width=True, hide_index=True,
                 column_config={"end_aum_cad": st.column_config.NumberColumn("AUM", format="$%,.0f"),
                                "net_flows_cad": st.column_config.NumberColumn("Net flows", format="$%,.0f"),
                                "revenue_cad": st.column_config.NumberColumn("Revenue", format="$%,.0f")})
    st.download_button("Download CSV", view.to_csv(index=False), f"wm_book_{month:%Y%m}.csv", "text/csv")

    trend = q(f"""SELECT month_start, sum(end_aum_cad) aum, sum(net_flows_cad) net_flows
                  FROM {SEM}.v_household_book
                  {"WHERE advisor_name = ?" if advisor != "All visible advisors" else ""}
                  GROUP BY 1 ORDER BY 1""", *([advisor] if advisor != "All visible advisors" else []))
    a, b = st.columns(2)
    a.subheader("AUM trend")
    a.line_chart(trend.set_index("month_start").aum)
    b.subheader("Net new assets by month")
    b.bar_chart(trend.set_index("month_start").net_flows)

# ------------------------------------------------------------------------------ glossary
with tab_glossary:
    cat = q(f"""SELECT name, dimension, category, definition, formula, unit, grain, owner, metric_view, measure
                FROM {SEM}.metric_catalog ORDER BY dimension, category, name""")
    term = st.text_input("Search metrics", placeholder="e.g. growth, NPS, revenue")
    if term:
        mask = cat.apply(lambda r: term.lower() in " ".join(map(str, r.values)).lower(), axis=1)
        cat = cat[mask]
    dim = st.radio("Dimension", ["All", "Quantity", "Quality"], horizontal=True)
    if dim != "All":
        cat = cat[cat.dimension == dim]
    for r in cat.itertuples():
        with st.expander(f"**{r.name}** · {r.dimension} · {r.category}"):
            st.write(r.definition)
            st.code(r.formula, language="sql")
            st.caption(f"Unit: {r.unit} · Grain: {r.grain} · Owner: {r.owner} · "
                       f"Source: `{r.metric_view}` → `{r.measure}`")

# ------------------------------------------------------------------------------ ask
with tab_ask:
    st.write("Ask questions in plain English in the **WM Performance Genie** space — it answers "
             "from the same certified metric views and respects your row-level access.")
    genie_url = os.getenv("WM_GENIE_URL")
    if genie_url:
        st.link_button("Open WM Performance Genie", genie_url)
    st.markdown("""Try:
* *What was net new assets by region last quarter?*
* *Which advisors have organic growth above 10% but meeting coverage below 30%?*
* *Did the Registered Plan Consolidation Outreach lift net flows, and did complaints change?*""")
