# Databricks notebook source
# MAGIC %md
# MAGIC # Governance: access model, row-level security, PII masking, tags
# MAGIC
# MAGIC | Persona | Group | Can see |
# MAGIC |---|---|---|
# MAGIC | Data engineers | `engineer_group` | everything |
# MAGIC | Analysts | `analyst_group` | gold, semantic, ops (read) |
# MAGIC | Business users / leadership | `business_group` | **semantic layer only** (metric views, secure views, catalog) |
# MAGIC | Advisors (via app / secure views) | mapped in `advisor_access_map` | only their own book |
# MAGIC
# MAGIC Row-level security and masking are implemented as UC **dynamic views** in `wm_semantic`,
# MAGIC because the gold tables are pipeline-managed materialized views.

# COMMAND ----------

for w, d in [("catalog", "wm_reporting_dev"), ("business_group", "wm_business_users"),
             ("analyst_group", "wm_analysts"), ("engineer_group", "wm_data_engineers"),
             ("leadership_group", "wm_leadership"), ("pii_group", "wm_pii_readers")]:
    dbutils.widgets.text(w, d)
cat = dbutils.widgets.get("catalog")
BUS, ANA, ENG, LEAD, PII = (dbutils.widgets.get(k) for k in
                            ("business_group", "analyst_group", "engineer_group", "leadership_group", "pii_group"))
SEM, GOLD = f"{cat}.wm_semantic", f"{cat}.wm_gold"


def run(sql: str, tolerate: bool = False):
    try:
        spark.sql(sql)
    except Exception as e:  # groups may not exist in a sandbox workspace
        if not tolerate:
            raise
        print(f"skipped: {sql.splitlines()[0][:90]} -> {str(e).splitlines()[0][:120]}")

# COMMAND ----------

# MAGIC %md ### Advisor-to-user entitlement map + row filter function

# COMMAND ----------

run(f"""CREATE TABLE IF NOT EXISTS {SEM}.advisor_access_map (
          advisor_id STRING NOT NULL, user_email STRING NOT NULL, granted_by STRING, granted_at TIMESTAMP)
        COMMENT 'Which workspace users may see which advisor books (drives row-level security)'""")
# demo entitlement: the deploying user can see the first advisor's book
run(f"""MERGE INTO {SEM}.advisor_access_map t
        USING (SELECT 'ADV0001' advisor_id, current_user() user_email) s
        ON t.advisor_id = s.advisor_id AND t.user_email = s.user_email
        WHEN NOT MATCHED THEN INSERT (advisor_id, user_email, granted_by, granted_at)
        VALUES (s.advisor_id, s.user_email, current_user(), current_timestamp())""")

run(f"""CREATE OR REPLACE FUNCTION {SEM}.can_see_advisor(p_advisor_id STRING)
        RETURNS BOOLEAN
        COMMENT 'Row-level security: leadership/engineers see all books; others only mapped advisors'
        RETURN is_account_group_member('{LEAD}') OR is_account_group_member('{ENG}')
            OR EXISTS (SELECT 1 FROM {SEM}.advisor_access_map m
                       WHERE m.advisor_id = p_advisor_id AND lower(m.user_email) = lower(current_user()))""")

run(f"""CREATE OR REPLACE FUNCTION {SEM}.mask_email(email STRING)
        RETURNS STRING
        RETURN CASE WHEN is_account_group_member('{PII}') THEN email
                    ELSE regexp_replace(email, '^[^@]+', '*****') END""")

# COMMAND ----------

# MAGIC %md ### Secure serving views (what advisors, the app and Genie query)

# COMMAND ----------

run(f"""CREATE OR REPLACE VIEW {SEM}.v_household_book
        COMMENT 'Household x month book of business, row-filtered to the viewer''s advisor entitlements'
        AS SELECT h.*, a.advisor_name, a.branch_name, a.region, d.household_name
           FROM {GOLD}.fct_household_monthly h
           JOIN {GOLD}.dim_advisor a ON h.advisor_id = a.advisor_id
           JOIN {GOLD}.dim_household d ON h.household_id = d.household_id
           WHERE {SEM}.can_see_advisor(h.advisor_id)""")

run(f"""CREATE OR REPLACE VIEW {SEM}.v_client_directory
        COMMENT 'Client directory with masked PII unless the viewer is in the PII group; row-filtered by advisor'
        AS SELECT c.client_id, c.household_id, d.advisor_id,
                  CASE WHEN is_account_group_member('{PII}') THEN c.first_name ELSE left(c.first_name, 1) || '.' END AS first_name,
                  CASE WHEN is_account_group_member('{PII}') THEN c.last_name ELSE left(c.last_name, 1) || '.' END AS last_name,
                  {SEM}.mask_email(c.email) AS email,
                  c.age_band, c.risk_profile, c.email_opt_in, c.status
           FROM {GOLD}.dim_client c
           JOIN {GOLD}.dim_household d ON c.household_id = d.household_id
           WHERE {SEM}.can_see_advisor(d.advisor_id)""")

run(f"""CREATE OR REPLACE VIEW {SEM}.v_advisor_scorecard
        COMMENT 'Advisor balanced scorecard (quantity + quality), row-filtered to the viewer''s entitlements'
        AS SELECT s.*, a.advisor_name FROM {GOLD}.fct_advisor_scorecard_monthly s
           JOIN {GOLD}.dim_advisor a ON s.advisor_id = a.advisor_id
           WHERE {SEM}.can_see_advisor(s.advisor_id)""")

# COMMAND ----------

# MAGIC %md ### Grants and tags

# COMMAND ----------

grants = [
    f"GRANT ALL PRIVILEGES ON CATALOG {cat} TO `{ENG}`",
    f"GRANT USE CATALOG ON CATALOG {cat} TO `{ANA}`",
    f"GRANT USE CATALOG ON CATALOG {cat} TO `{BUS}`",
    *[f"GRANT USE SCHEMA, SELECT ON SCHEMA {cat}.{s} TO `{ANA}`" for s in ("wm_gold", "wm_semantic", "wm_ops")],
    f"GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA {SEM} TO `{BUS}`",
]
for g in grants:
    run(g, tolerate=True)

tags = {
    "wm_bronze": ("bronze", "confidential"), "wm_silver": ("silver", "confidential"),
    "wm_gold": ("gold", "confidential"), "wm_semantic": ("semantic", "internal"), "wm_ops": ("ops", "internal"),
}
for schema, (layer, cls) in tags.items():
    run(f"ALTER SCHEMA {cat}.{schema} SET TAGS ('domain' = 'wealth_management', 'layer' = '{layer}', "
        f"'data_classification' = '{cls}')", tolerate=True)
for col in ("first_name", "last_name", "email"):
    run(f"ALTER MATERIALIZED VIEW {GOLD}.dim_client ALTER COLUMN {col} SET TAGS ('pii' = 'true')", tolerate=True)
print("governance applied")

# COMMAND ----------

# MAGIC %md ### Dashboard-facing views
# MAGIC Dashboards run with the viewer's identity, so every dataset reads from `wm_semantic`
# MAGIC (metric views or the views below) — business users never need gold access.

# COMMAND ----------

OPS = f"{cat}.wm_ops"
serving = {
    "v_aum_daily": f"""SELECT * FROM {GOLD}.fct_aum_daily WHERE {SEM}.can_see_advisor(advisor_id)""",
    "v_asset_mix": f"""SELECT * FROM {GOLD}.fct_asset_mix_monthly WHERE {SEM}.can_see_advisor(advisor_id)""",
    "v_campaign_impact": f"""SELECT * FROM {GOLD}.campaign_impact_results""",
    "v_campaign_trend": f"""SELECT * FROM {GOLD}.fct_campaign_trend""",
    "v_digital_engagement": f"""SELECT * FROM {GOLD}.fct_digital_engagement_monthly""",
    "v_platform_health": f"""SELECT * FROM {OPS}.dq_check_results""",
}
for name, body in serving.items():
    run(f"CREATE OR REPLACE VIEW {SEM}.{name} AS {body}", tolerate=True)
print("serving views published")
