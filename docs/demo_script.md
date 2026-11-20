# 10-minute walkthrough

A script for presenting this project to a hiring panel or a stakeholder group. Each step says what to show and the point it makes.

## 1. The problem (1 min)
> "Wealth Management leaders need one trusted answer to *how is the business doing* — assets, flows, revenue — and *is the client experience holding up* — engagement, complaints, NPS. They also need to know whether initiatives actually move the needle. The usual reality is five spreadsheets with five definitions of 'net new assets'."

Show `docs/requirements_traceability.md`: every responsibility maps to a component.

## 2. Architecture (2 min)
Show the diagram in `docs/architecture.md`.
* Seventeen upstream feeds land in a UC volume. One **metadata-driven** bronze layer ingests all of them with Auto Loader.
* Silver: **expectations** drop bad rows into **quarantine tables** that record which rules failed. **AUTO CDC** dedupes custodian re-sends (SCD1) and keeps advisor, household and client history (SCD2).
* Gold: a **star schema** of reporting datasets, all materialized views that refresh incrementally.
* Semantic: **UC metric views**, the one place a metric is defined.

Point to make: *"If an advisor moves from Markham to Yorkville in March, Q1 AUM stays with Markham. That's SCD2 plus a point-in-time join, and a test proves it."*

## 3. Correctness (2 min)
Open `tests/test_pipeline_local.py`.
* The synthetic data follows an exact accounting identity, so the **true** return for every account-day is known.
* The test runs the **same gold SQL files** the pipeline runs and asserts that daily and monthly TWR match the truth to 1e-9, that the AUM bridge closes, and that holdings reconcile to valuations.

> "I don't just test that the SQL runs. I test that it's *right*."

## 4. Semantic layer and dashboard (2 min)
* Open `wm_aum_flows_revenue.yml`. AUM is **semi-additive**, so there is Month-End AUM for a single month and Average AUM for periods. ROA is a **ratio of sums**, so it is correct for any window (there's a test for this).
* Open the dashboard. Executive overview KPIs, the AUM trend, NNA by segment, the market movement bridge and asset mix.
* Open the Advisor Scorecard. The **quantity versus quality** quadrant: who is growing the book without hurting client experience?

## 5. Incremental impact (2 min)
Open the *Initiative Impact* page.
* Campaign 1 (registered-plan consolidation outreach) shows **significant lift in net flows, deposits and interactions**, while complaints, service cases and attrition show **no detectable change**. The quality guardrails held.
* Campaign 2 (digital nudge) shows a **~40% lift in digital sessions** and **no effect on flows**. The estimator correctly reports a null result where there isn't one.

> "Randomized holdouts plus household-level difference-in-differences. The verdict column is written for a business audience; the CI is there for the analysts."

## 6. Self-serve and operations (1 min)
* **WM Insights app:** an advisor sees only their book (row-level security through on-behalf-of-user auth), with "needs attention" flags, the metric glossary and CSV export.
* **Genie:** "Which advisors have organic growth above 10% but meeting coverage below 30%?" This is answered from certified measures.
* **Platform Health** page: if a critical check fails, the job stops before the dashboard refreshes.

## Likely questions

| Question | Short answer |
|---|---|
| Why TWR and not money-weighted? | TWR removes the effect of client flow timing, which is what you need to compare investment performance. Modified Dietz is included as a cross-check |
| How would this connect to real sources? | Replace the generator with the landing process (ADF, SFTP, or partner shares into the volume). Bronze and everything downstream stays the same because feeds are config |
| How do you stop metric drift? | Metric views are the only implementation. The catalog has an owner per metric, contract tests fail on broken references, and the dashboard is generated from code |
| What if marketing can't randomize? | Fall back to matched controls (propensity or synthetic control) and lean harder on the pre-trend check. The panel and estimator are already separated to allow that |
| Scale? | The `large` preset has 25k households and 250 advisors. Serverless, incremental MVs and liquid clustering on date and key cover it |
