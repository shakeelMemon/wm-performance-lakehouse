"""Render docs/metric_definitions.md from src/semantic/metric_catalog.yml (single source of truth)."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
metrics = yaml.safe_load((ROOT / "src" / "semantic" / "metric_catalog.yml").read_text())["metrics"]

lines = ["# Metric definitions", "",
         "_Generated from `src/semantic/metric_catalog.yml` by `tools/gen_metric_docs.py` — edit the YAML, not this file._",
         "", "Every certified metric has one definition, one owner and one governed implementation "
         "(a measure in a Unity Catalog metric view). Dashboards, Genie, the app and notebooks all read "
         "that implementation.", ""]
for dim in ("Quantity", "Quality"):
    lines += [f"## {dim} metrics", "", "| Metric | Definition | Formula | Grain | Source | Owner |",
              "|---|---|---|---|---|---|"]
    for m in (x for x in metrics if x["dimension"] == dim):
        lines.append(f"| **{m['name']}** | {m['definition']} | `{m['formula']}` | {m['grain']} | "
                     f"`{m['metric_view']}` → {m['measure']} | {m['owner']} |")
    lines.append("")
lines += ["## Conventions", "",
          "* **Currency** — CAD. USD positions are converted at the custodian's daily FX rate.",
          "* **Flows** — only external movements count as flows (deposits, withdrawals, transfers in/out). "
          "Trades, income, fees and commissions are internal and show up in performance, not flows.",
          "* **Returns** — net of fees and commissions, flows assumed end-of-day, daily returns geometrically linked (TWR).",
          "* **Semi-additive** — AUM is a balance: never sum it across months. Use Average AUM for periods.",
          "* **Rates** — ratios are computed as ratio-of-sums so they stay correct for any time window "
          "(never average monthly ratios).",
          "* **Fiscal year** — November to October.", ""]
out = ROOT / "docs" / "metric_definitions.md"
out.write_text("\n".join(lines))
print(f"wrote {out}")
