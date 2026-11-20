"""Synthetic Wealth Management data generator.

Produces a realistic, internally consistent book of business for a Canadian wealth
management division: branches, advisors, households, clients, accounts, securities,
prices, FX, transactions, daily valuations, month-end holdings, CRM interactions,
digital sessions, service cases, NPS responses and two randomized campaigns.

Design properties that the rest of the project (and the test-suite) rely on:

* **Accounting identity.** Every account follows
  ``MV_t = MV_{t-1} * (1 + r_t) - fees_t - commissions_t + external_flows_t``
  so a time-weighted return computed from the valuation + transaction feeds reproduces
  the simulated (true) net-of-fee return exactly (to rounding).
* **Append-only determinism.** Randomness is keyed by (seed, stream, day/month) rather
  than drawn from one global sequence, so extending ``end_date`` appends new days without
  rewriting history. That is what makes the daily incremental landing mode possible.
* **Known ground truth.** Campaigns are randomized (stratified by advisor) and carry an
  injected treatment effect, so the incremental-impact analysis can be validated.
* **Injected data-quality defects** (duplicates, orphans, bad types, future dates,
  negative values) at ``dq_error_rate`` so silver expectations have work to do.

Everything is pure numpy/pandas: runs on serverless, a laptop, or in CI.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict

import numpy as np
import pandas as pd

from .config import ACCOUNT_TYPES, ASSET_CLASSES, SCALE_PRESETS, SEGMENTS, GenConfig

HORIZON_DAYS = 1100  # fixed calendar horizon so draws never depend on end_date

_BRANCHES = [
    ("Bay & King", "ON", "Ontario - GTA"), ("Markham Centre", "ON", "Ontario - GTA"),
    ("Yorkville", "ON", "Ontario - GTA"), ("Oakville Harbour", "ON", "Ontario - GTA"),
    ("Ottawa Glebe", "ON", "Ontario - East & North"),
    ("Kingston Waterfront", "ON", "Ontario - East & North"),
    ("Montreal Westmount", "QC", "Quebec"), ("Quebec Sainte-Foy", "QC", "Quebec"),
    ("Vancouver Kerrisdale", "BC", "British Columbia"), ("Victoria Oak Bay", "BC", "British Columbia"),
    ("Calgary Beltline", "AB", "Prairies"), ("Edmonton Oliver", "AB", "Prairies"),
    ("Winnipeg Exchange", "MB", "Prairies"), ("Saskatoon Broadway", "SK", "Prairies"),
    ("Halifax Spring Garden", "NS", "Atlantic"), ("Moncton Main", "NB", "Atlantic"),
    ("London Masonville", "ON", "Ontario - Southwest"), ("Waterloo Uptown", "ON", "Ontario - Southwest"),
    ("Burlington Brant", "ON", "Ontario - Southwest"), ("Mississauga City Centre", "ON", "Ontario - GTA"),
    ("Kelowna Lakeshore", "BC", "British Columbia"), ("Regina Cathedral", "SK", "Prairies"),
    ("St. John's Water St", "NL", "Atlantic"), ("Laval Centropolis", "QC", "Quebec"),
    ("Richmond Hill Yonge", "ON", "Ontario - GTA"),
]
_FIRST = ["Aisha", "Ben", "Chloe", "Daniel", "Elena", "Farid", "Grace", "Hassan", "Isla", "Jun",
          "Kavya", "Liam", "Maya", "Noah", "Olivia", "Priya", "Quinn", "Raj", "Sofia", "Tariq",
          "Uma", "Victor", "Wei", "Ximena", "Yusuf", "Zoe", "Amir", "Beatrice", "Carlos", "Dana",
          "Emeka", "Fatima", "Gabriel", "Hana", "Ivan", "Jasmine", "Kofi", "Leila", "Mateo", "Nadia"]
_LAST = ["Tremblay", "Singh", "Chen", "Nguyen", "MacDonald", "Patel", "Roy", "Khan", "Wilson",
         "Gagnon", "Li", "Martin", "Kaur", "Brown", "Lee", "Côté", "Ahmed", "Taylor", "Wong",
         "Campbell", "Anderson", "Bouchard", "Kim", "Ali", "Fraser", "Morin", "Shah", "Park",
         "Stewart", "Lavoie", "Hussain", "Scott", "Pelletier", "Das", "Young", "Leblanc"]

_RISK = ["Conservative", "Moderate", "Balanced", "Growth", "Aggressive"]
_RISK_WEIGHTS = np.array([  # CA eq, US eq, Intl eq, FI, Cash, Alts
    [.10, .07, .05, .60, .15, .03],
    [.15, .12, .08, .50, .10, .05],
    [.22, .18, .12, .35, .06, .07],
    [.28, .26, .16, .20, .03, .07],
    [.33, .32, .20, .08, .02, .05],
])
_MU = np.array([.075, .095, .065, .035, .030, .055])      # annual drift by asset class
_VOL = np.array([.14, .16, .15, .055, .003, .08])         # annual vol by asset class
_CORR = np.array([
    [1.0, .75, .70, .10, 0, .40], [.75, 1.0, .75, .05, 0, .40], [.70, .75, 1.0, .10, 0, .40],
    [.10, .05, .10, 1.0, 0, .10], [0, 0, 0, 0, 1.0, 0], [.40, .40, .40, .10, 0, 1.0]])

CAMPAIGN_DEFS = [
    dict(campaign_id="CMP-2026-01", campaign_name="Registered Plan Consolidation Outreach",
         objective="Grow net new assets via RRSP/TFSA consolidation conversations",
         channel="Advisor outreach + email", start_date="2026-01-12", end_date="2026-03-31",
         measurement_end_date="2026-06-30", eligible_segments="Mass Affluent|High Net Worth",
         primary_metric="net_flows_cad", pre_months=3),
    dict(campaign_id="CMP-2026-02", campaign_name="Digital Adoption Nudge",
         objective="Increase active digital usage of the client portal and mobile app",
         channel="In-app + email", start_date="2026-05-04", end_date="2026-06-30",
         measurement_end_date="2026-08-31", eligible_segments="Mass Affluent|High Net Worth|Ultra High Net Worth",
         primary_metric="digital_sessions", pre_months=3),
]
# Injected ground-truth effects (multipliers on event rates for treated households)
TRUE_EFFECTS = {
    "CMP-2026-01": {"deposit_rate_mult": 1.6, "transfer_in_rate_mult": 2.5, "interaction_rate_add": 0.6},
    "CMP-2026-02": {"session_rate_mult": 1.35},
}


def _rng(cfg: GenConfig, *key: int) -> np.random.Generator:
    return np.random.default_rng([cfg.seed, *key])


def _d(x) -> pd.Timestamp:
    return pd.Timestamp(x)


def _hash_pick(keys: pd.DataFrame) -> np.ndarray:
    """Stable pseudo-uniform [0,1) per row, independent of window length (for DQ injection)."""
    h = pd.util.hash_pandas_object(keys.astype(str), index=False).values
    return (h % np.uint64(1_000_003)).astype(float) / 1_000_003


def generate(cfg: GenConfig | str = "medium") -> dict[str, pd.DataFrame]:
    """Generate every landing feed for ``[cfg.start_date, cfg.end_date]``.

    Returns a dict of DataFrames keyed by feed name. Each frame has a ``_file_date``
    column (the business date the upstream system would have delivered it on), plus a
    ``_truth`` frame with the simulated per-account daily net return for testing.
    """
    if isinstance(cfg, str):
        cfg = SCALE_PRESETS[cfg]
    start, end = _d(cfg.start_date), _d(cfg.end_date)
    horizon = pd.bdate_range(start, start + pd.Timedelta(days=HORIZON_DAYS))
    days = horizon[horizon <= end]
    T, H = len(days), len(horizon)
    out: dict[str, pd.DataFrame] = {}

    # ---------------------------------------------------------------- branches
    r = _rng(cfg, 1)
    nb = min(cfg.n_branches, len(_BRANCHES))
    br = pd.DataFrame(_BRANCHES[:nb], columns=["branch_name", "province", "region"])
    br.insert(0, "branch_id", [f"BR{i:03d}" for i in range(1, nb + 1)])
    br["opened_date"] = [(start - pd.Timedelta(days=int(d))).date() for d in r.integers(900, 9000, nb)]
    br["_file_date"] = start.date()
    out["branches"] = br

    # ---------------------------------------------------------------- advisors (SCD2 source)
    r = _rng(cfg, 2)
    na = cfg.n_advisors
    adv = pd.DataFrame({
        "advisor_id": [f"ADV{i:04d}" for i in range(1, na + 1)],
        "advisor_name": [f"{_FIRST[i % len(_FIRST)]} {_LAST[(i * 7) % len(_LAST)]}" for i in range(na)],
        "branch_id": r.choice(br.branch_id, na),
        "designation": r.choice(["CFP", "CIM", "PFP", "CFA", "CFP, CIM"], na, p=[.35, .25, .2, .1, .1]),
        "hire_date": [(start - pd.Timedelta(days=int(d))).date() for d in r.integers(200, 8000, na)],
        "team_type": r.choice(["Solo", "Team"], na, p=[.6, .4]),
        "status": "Active",
    })
    diligence = np.clip(r.normal(1.0, 0.25, na), 0.45, 1.6)      # hidden quality driver
    book_w = r.lognormal(0, 0.55, na)
    ev = adv.copy()
    ev["event_ts"] = pd.to_datetime(ev.hire_date)
    ev["operation"] = "INSERT"
    transfers = []
    for i in np.where(r.random(na) < 0.15)[0]:
        t = int(r.integers(20, H - 1))
        new_b = r.choice(br.branch_id[br.branch_id != adv.branch_id[i]])
        row = ev.iloc[i].copy()
        row["branch_id"], row["event_ts"], row["operation"] = new_b, horizon[t] + pd.Timedelta(hours=7), "UPDATE"
        transfers.append(row)
    if transfers:
        ev = pd.concat([ev, pd.DataFrame(transfers)], ignore_index=True)
    ev["_file_date"] = [max(ts.normalize(), start).date() for ts in ev.event_ts]
    out["advisor_events"] = ev[ev._file_date <= end.date()].reset_index(drop=True)

    # ---------------------------------------------------------------- households
    r = _rng(cfg, 3)
    nh = cfg.n_households
    hh_adv = r.choice(na, nh, p=book_w / book_w.sum())
    seg = r.choice(len(SEGMENTS), nh, p=[.62, .30, .08])
    is_new = r.random(nh) < cfg.new_hh_share
    t_open = np.where(is_new, r.integers(0, min(H, 520), nh), -1)
    onboard = np.array([horizon[t].date() if t >= 0 else (start - pd.Timedelta(days=int(d))).date()
                        for t, d in zip(t_open, r.integers(60, 6000, nh))])
    hazard = cfg.annual_hh_churn * (1.7 - diligence[hh_adv]) / 252.0
    t_close = np.maximum(t_open, 0) + r.geometric(np.clip(hazard, 1e-6, 1)) + 5
    adv_branch0 = adv.set_index("advisor_id").branch_id
    hh = pd.DataFrame({
        "household_id": [f"HH{i:06d}" for i in range(1, nh + 1)],
        "household_name": [f"{_LAST[(i * 13) % len(_LAST)]} Household" for i in range(nh)],
        "advisor_id": adv.advisor_id.values[hh_adv],
        "segment": np.array(SEGMENTS)[seg],
        "onboard_date": onboard,
    })
    hh["province"] = br.set_index("branch_id").province.reindex(adv_branch0.reindex(hh.advisor_id).values).values
    alive_hh = (t_open < T)
    hev = hh[alive_hh].copy()
    hev["status"], hev["operation"] = "Active", "INSERT"
    hev["event_ts"] = pd.to_datetime(hev.onboard_date) + pd.Timedelta(hours=9)
    closed_idx = np.where(alive_hh & (t_close < T))[0]
    cl = hh.iloc[closed_idx].copy()
    cl["status"], cl["operation"] = "Closed", "UPDATE"
    cl["event_ts"] = [horizon[t] + pd.Timedelta(hours=17) for t in t_close[closed_idx]]
    hev = pd.concat([hev, cl], ignore_index=True)
    hev["_file_date"] = [max(ts.normalize(), start).date() for ts in hev.event_ts]
    out["household_events"] = hev

    # ---------------------------------------------------------------- clients
    r = _rng(cfg, 4)
    n_cl = r.choice([1, 2, 3], nh, p=[.45, .40, .15])
    c_hh = np.repeat(np.arange(nh), n_cl)
    nc = len(c_hh)
    birth = np.clip(r.normal(1966, 14, nc).round().astype(int), 1935, 2004)
    risk_idx = np.clip(np.round(r.normal(2.0 - (seg[c_hh] == 0) * 0.3 + (birth - 1966) / 25, 0.9)), 0, 4).astype(int)
    cli = pd.DataFrame({
        "client_id": [f"CL{i:07d}" for i in range(1, nc + 1)],
        "household_id": hh.household_id.values[c_hh],
        "first_name": [_FIRST[(i * 11) % len(_FIRST)] for i in range(nc)],
        "last_name": [_LAST[(i * 13 // 2) % len(_LAST)] for i in range(nc)],
        "email": [f"client{i:07d}@example.com" for i in range(1, nc + 1)],
        "birth_year": birth,
        "risk_profile": np.array(_RISK)[risk_idx],
        "email_opt_in": r.random(nc) < 0.7,
        "is_primary": np.r_[True, c_hh[1:] != c_hh[:-1]],
    })
    cev = cli[alive_hh[c_hh]].copy()
    cev["status"], cev["operation"] = "Active", "INSERT"
    cev["event_ts"] = pd.to_datetime(hh.onboard_date.values[c_hh][alive_hh[c_hh]]) + pd.Timedelta(hours=10)
    ups = []
    for i in np.where(alive_hh[c_hh] & (r.random(nc) < 0.18))[0]:   # KYC refresh: risk profile change
        t = int(r.integers(max(t_open[c_hh[i]], 0) + 10, H))
        if t >= min(T, t_close[c_hh[i]]):
            continue
        row = cev.loc[i].copy()
        row["risk_profile"] = _RISK[int(np.clip(risk_idx[i] + r.choice([-1, 1]), 0, 4))]
        row["operation"], row["event_ts"] = "UPDATE", horizon[t] + pd.Timedelta(hours=11)
        ups.append(row)
    for i in np.where(alive_hh[c_hh] & (t_close[c_hh] < T))[0]:
        row = cev.loc[i].copy()
        row["status"], row["operation"] = "Closed", "UPDATE"
        row["event_ts"] = horizon[t_close[c_hh[i]]] + pd.Timedelta(hours=18)
        ups.append(row)
    if ups:
        cev = pd.concat([cev, pd.DataFrame(ups)], ignore_index=True)
    bad = r.random(len(cev)) < cfg.dq_error_rate                     # DQ: malformed emails
    cev.loc[bad, "email"] = "not-an-email"
    cev["_file_date"] = [max(ts.normalize(), start).date() for ts in cev.event_ts]
    out["client_events"] = cev.reset_index(drop=True)

    # ---------------------------------------------------------------- accounts
    r = _rng(cfg, 5)
    age = start.year - birth
    n_acc = np.clip(1 + r.poisson(0.4 + 0.5 * seg[c_hh]), 1, 5)
    a_cl = np.repeat(np.arange(nc), n_acc)
    A = len(a_cl)
    a_hh = c_hh[a_cl]
    a_seg = seg[a_hh]
    types = []
    for i in range(A):
        opts = ["Non-Registered", "TFSA", "RRSP"]
        if age[a_cl[i]] >= 71:
            opts = ["Non-Registered", "TFSA", "RRIF"]
        elif age[a_cl[i]] < 40:
            opts += ["FHSA"]
        if 28 <= age[a_cl[i]] <= 55:
            opts += ["RESP"]
        types.append(opts[int(r.integers(0, len(opts)))])
    types = np.array(types)
    prog_p = np.array([[.10, .50, .40], [.35, .50, .15], [.70, .30, .0]])
    prog = np.array([r.choice(3, p=prog_p[s]) for s in a_seg])
    fee_bps = np.select([prog == 0, prog == 1], [r.uniform(110, 150, A) - 35 * (a_seg == 2),
                                                 r.uniform(80, 125, A) - 20 * (a_seg == 2)], 0.0).round(1)
    hh_total = r.lognormal(np.log([180_000, 1_600_000, 9_000_000])[seg], 0.55)
    counts = np.bincount(a_hh, minlength=nh)          # a_hh is sorted by construction
    share = np.concatenate([r.dirichlet(np.ones(c) * 2) for c in counts if c > 0]) if A else np.array([])
    init_mv = hh_total[a_hh] * share
    init_mv = np.round(np.maximum(init_mv, 2_000), 2)
    acc_ids = np.array([f"AC{i:08d}" for i in range(1, A + 1)])
    a_open = t_open[a_hh]
    a_close = t_close[a_hh]
    accounts = pd.DataFrame({
        "account_id": acc_ids, "client_id": cli.client_id.values[a_cl],
        "household_id": hh.household_id.values[a_hh], "account_type": types,
        "program": np.array(["Discretionary Managed", "Fee-Based", "Commission"])[prog],
        "fee_bps": fee_bps, "currency": "CAD",
        "open_date": [hh.onboard_date.values[h] for h in a_hh],
    })
    alive_a = a_open < T
    aev = accounts[alive_a].copy()
    aev["close_date"], aev["status"], aev["operation"] = None, "Open", "INSERT"
    aev["event_ts"] = pd.to_datetime(aev.open_date) + pd.Timedelta(hours=12)
    ci = np.where(alive_a & (a_close < T))[0]
    ac = accounts.iloc[ci].copy()
    ac["close_date"] = [horizon[t].date() for t in a_close[ci]]
    ac["status"], ac["operation"] = "Closed", "UPDATE"
    ac["event_ts"] = [horizon[t] + pd.Timedelta(hours=19) for t in a_close[ci]]
    aev = pd.concat([aev, ac], ignore_index=True)
    aev["_file_date"] = [max(ts.normalize(), start).date() for ts in aev.event_ts]
    out["account_events"] = aev

    # ---------------------------------------------------------------- securities / prices / fx
    r = _rng(cfg, 6)
    sec_rows = []
    per_class = {"Canadian Equity": 8, "US Equity": 8, "International Equity": 5,
                 "Fixed Income": 7, "Cash & Equivalents": 2, "Alternatives": 4}
    k = 1
    for ci_, (ac_, n) in enumerate(per_class.items()):
        for j in range(n):
            sec_rows.append(dict(security_id=f"SEC{k:03d}", ticker=f"{ac_[:2].upper()}{j + 1:02d}",
                                 security_name=f"{ac_} Fund {chr(65 + j)}", asset_class=ac_, class_idx=ci_,
                                 currency="USD" if ac_ == "US Equity" else "CAD",
                                 sector=r.choice(["Financials", "Energy", "Technology", "Industrials",
                                                  "Health Care", "Broad Market"]) if "Equity" in ac_ else None))
            k += 1
    sec = pd.DataFrame(sec_rows)
    sec_by_class = [sec.index[sec.class_idx == c].to_numpy() for c in range(6)]

    L = np.linalg.cholesky(_CORR)
    F = np.zeros((H, 6))
    fx = np.zeros(H)
    for t in range(H):
        rr = _rng(cfg, 7, t)
        z = L @ rr.standard_normal(6)
        F[t] = _MU / 252 + _VOL / np.sqrt(252) * z
        fx[t] = rr.normal(0, 0.06 / np.sqrt(252))
    shock = (horizon >= "2025-04-02") & (horizon <= "2025-04-10")     # a tariff-style drawdown
    F[shock, :3] -= 0.012
    F[shock, 5] -= 0.005
    usdcad = 1.36 * np.cumprod(1 + fx)
    rr = _rng(cfg, 8)
    idio = rr.normal(0, 0.004, (H, len(sec))) * (sec.class_idx.values < 4)
    local_ret = F[:, sec.class_idx.values] + idio
    local_ret[:, (sec.currency == "USD").values] -= fx[:, None]
    px = rr.uniform(20, 120, len(sec)) * np.cumprod(1 + local_ret, axis=0)
    px[:, (sec.class_idx == 4).values] = 1.0
    sec = sec.drop(columns="class_idx")
    sec["_file_date"] = start.date()
    out["securities"] = sec
    pr = pd.DataFrame({"security_id": np.tile(sec.security_id.values, T),
                       "price_date": np.repeat(days.date, len(sec)),
                       "close_price": px[:T].ravel().round(4),
                       "currency": np.tile(sec.currency.values, T)})
    pr["_file_date"] = pr.price_date
    out["prices"] = pr
    fxdf = pd.DataFrame({"rate_date": days.date, "currency": "USD", "rate_to_cad": usdcad[:T].round(5)})
    fxdf["_file_date"] = fxdf.rate_date
    out["fx_rates"] = fxdf

    # ---------------------------------------------------------------- account asset mix
    r = _rng(cfg, 9)
    a_risk = risk_idx[a_cl]
    W = np.array([r.dirichlet(_RISK_WEIGHTS[k_] * 60) for k_ in a_risk]) if A else np.zeros((0, 6))
    slots = []  # (account_idx, security_idx, weight)
    for i in range(A):
        for c in range(6):
            if W[i, c] < 0.01:
                continue
            n = 2 if c in (0, 1) and r.random() < 0.5 else 1
            picks = r.choice(sec_by_class[c], n, replace=False)
            for p in picks:
                slots.append((i, p, W[i, c] / n))
    slots = np.array(slots) if slots else np.zeros((0, 3))
    if len(slots):   # re-normalise so positions sum exactly to the account's market value
        tot = np.bincount(slots[:, 0].astype(int), weights=slots[:, 2], minlength=A)
        slots[:, 2] = slots[:, 2] / tot[slots[:, 0].astype(int)]

    # ---------------------------------------------------------------- campaigns & assignment
    camps = [c for c in CAMPAIGN_DEFS if cfg.campaigns and _d(c["start_date"]) <= end
             and _d(c["start_date"]) - pd.DateOffset(months=c["pre_months"]) >= start]
    hh_treat = {c["campaign_id"]: np.zeros(nh, dtype=bool) for c in camps}
    assign_rows = []
    for ci_, c in enumerate(camps):
        rc = _rng(cfg, 10, ci_)
        cs = _d(c["start_date"])
        t_cs = int(horizon.searchsorted(cs))
        elig = alive_hh & (t_open < t_cs - 40) & (t_close > t_cs) & \
            np.isin(np.array(SEGMENTS)[seg], c["eligible_segments"].split("|"))
        for a_ in range(na):   # stratified randomization within advisor book
            idx = np.where(elig & (hh_adv == a_))[0]
            rc.shuffle(idx)
            treat = idx[: len(idx) // 2 + (len(idx) % 2) * int(rc.integers(0, 2))]
            hh_treat[c["campaign_id"]][treat] = True
            for h in idx:
                assign_rows.append(dict(campaign_id=c["campaign_id"], household_id=hh.household_id[h],
                                        assignment_group="Treatment" if hh_treat[c["campaign_id"]][h] else "Control",
                                        assigned_date=(cs - pd.Timedelta(days=7)).date(),
                                        stratum=adv.advisor_id[a_]))
    cdf = pd.DataFrame(camps).drop(columns=["pre_months"]) if camps else pd.DataFrame(
        columns=["campaign_id", "campaign_name", "objective", "channel", "start_date", "end_date",
                 "measurement_end_date", "eligible_segments", "primary_metric"])
    cdf["_file_date"] = [(_d(s) - pd.Timedelta(days=14)).date() for s in cdf.start_date]
    out["campaigns"] = cdf
    adf = pd.DataFrame(assign_rows, columns=["campaign_id", "household_id", "assignment_group", "assigned_date", "stratum"])
    adf["_file_date"] = adf.assigned_date
    out["campaign_assignments"] = adf

    def effect_window(cid: str) -> np.ndarray:
        c = next(c for c in camps if c["campaign_id"] == cid)
        return (days >= _d(c["start_date"])) & (days <= _d(c["measurement_end_date"]))

    c1 = "CMP-2026-01" if "CMP-2026-01" in hh_treat else None
    c2 = "CMP-2026-02" if "CMP-2026-02" in hh_treat else None
    win1 = effect_window(c1) if c1 else np.zeros(T, bool)

    # ---------------------------------------------------------------- daily simulation
    R_acct = W @ F[:T].T if A else np.zeros((0, T))           # A x T gross asset-class returns
    month_first = np.r_[True, days.month[1:] != days.month[:-1]]
    month_last = np.r_[days.month[1:] != days.month[:-1], (horizon[T].month != days[-1].month) if T < H else True]
    quarter_last = month_last & np.isin(days.month, [3, 6, 9, 12])
    base_dep = np.array([.25, .20, .15])[a_seg] / 21
    base_wd = np.array([.12, .10, .10])[a_seg] / 21
    is_rrsp, is_tfsa, is_rrif = types == "RRSP", types == "TFSA", types == "RRIF"
    treat1_a = hh_treat[c1][a_hh] if c1 else np.zeros(A, bool)
    is_comm = prog == 2
    mv = np.zeros(A)
    MV = np.full((A, T), np.nan)
    NETF = np.zeros((A, T))
    TRUE_R = np.full((A, T), np.nan)
    tx = {k_: [] for k_ in ("DEPOSIT", "WITHDRAWAL", "TRANSFER_IN", "TRANSFER_OUT", "FEE", "TRADE")}

    for t in range(T):
        rr = _rng(cfg, 11, t)
        prev = mv.copy()
        active = (a_open <= t) & (a_close >= t)
        opening = a_open == t
        existing_start = (t == 0) & (a_open < 0)
        grown = np.round(prev * (1 + R_acct[:, t] + rr.normal(0, 0.0015, A)), 2)
        fee = np.where(month_first[t] & active & (prev > 0), np.round(prev * fee_bps / 1e4 / 12, 2), 0.0)
        trade = active & (prev > 0) & (rr.random(A) < 0.6 / 21)
        trade_amt = np.round(prev * rr.uniform(0.01, 0.05, A), 2)
        comm = np.where(trade & is_comm, np.round(np.maximum(25, 0.0075 * trade_amt), 2), 0.0)
        mon = days[t].month
        season = np.where(is_rrsp & np.isin(mon, [1, 2]), 4.0, 1.0) * np.where(is_tfsa & (mon == 1), 3.0, 1.0)
        lift = np.where(treat1_a & win1[t], TRUE_EFFECTS["CMP-2026-01"]["deposit_rate_mult"], 1.0)
        lift_ti = np.where(treat1_a & win1[t], TRUE_EFFECTS["CMP-2026-01"]["transfer_in_rate_mult"], 1.0)
        ok = active & ~opening & (prev > 0) & (a_close > t)
        dep = ok & (rr.random(A) < base_dep * season * lift)
        wd = ok & ((rr.random(A) < base_wd) | (is_rrif & month_first[t]))
        tin = ok & (rr.random(A) < 0.02 / 21 * lift_ti)
        tout = ok & (rr.random(A) < 0.008 / 21)
        avail = grown - fee - comm
        dep_amt = np.where(dep, np.round(np.maximum(500, prev * rr.lognormal(np.log(.035), .7, A)), 2), 0)
        wd_amt = np.where(wd, np.round(np.minimum(avail * .5, np.where(is_rrif & month_first[t], prev * .006, 0)
                                                  + prev * rr.lognormal(np.log(.025), .7, A) * (~is_rrif | ~month_first[t])), 2), 0)
        tin_amt = np.where(tin, np.round(prev * rr.lognormal(np.log(.2), .6, A), 2), 0)
        tout_amt = np.where(tout, np.round(avail * rr.uniform(.05, .3, A), 2), 0)
        closing = active & (a_close == t) & (prev > 0)
        close_amt = np.where(closing, np.round(avail, 2), 0)
        open_amt = np.where(opening & active, init_mv, 0)
        flow = dep_amt - wd_amt + tin_amt - tout_amt - close_amt + open_amt
        new_mv = np.where(existing_start, init_mv, avail + flow)
        new_mv = np.where(active, np.round(np.maximum(new_mv, 0), 2), 0)
        mv = new_mv
        MV[active, t] = mv[active]
        NETF[:, t] = np.where(existing_start, 0, flow)
        with np.errstate(divide="ignore", invalid="ignore"):
            TRUE_R[:, t] = np.where(active & (prev > 0), (mv - prev - NETF[:, t]) / prev, np.nan)
        for name, m, amt in (("DEPOSIT", dep, dep_amt), ("WITHDRAWAL", wd & (wd_amt > 0), -wd_amt),
                             ("TRANSFER_IN", tin | (opening & active), tin_amt + open_amt),
                             ("TRANSFER_OUT", tout | closing, -(tout_amt + close_amt)),
                             ("FEE", fee > 0, -fee)):
            ix = np.nonzero(m)[0]
            tx[name].append(np.c_[ix, np.full(len(ix), t), amt[ix]])
        ix = np.nonzero(trade)[0]
        tx["TRADE"].append(np.c_[ix, np.full(len(ix), t), trade_amt[ix], comm[ix]])

    # ---------------------------------------------------------------- valuations
    ai, ti = np.nonzero(~np.isnan(MV))
    val = pd.DataFrame({"account_id": acc_ids[ai], "as_of_date": days.date[ti],
                        "market_value_cad": MV[ai, ti]})
    cash_w = W[:, 4] if A else np.zeros(0)
    val["cash_cad"] = np.round(val.market_value_cad.values * cash_w[ai], 2)
    val["currency"], val["source_system"] = "CAD", "CUSTODIAN_A"
    val["file_ts"] = pd.to_datetime(val.as_of_date) + pd.Timedelta(hours=21)
    hv = _hash_pick(val[["account_id", "as_of_date"]])
    dup = val[hv < cfg.dq_error_rate].copy()
    dup["file_ts"] = dup.file_ts + pd.Timedelta(hours=10)                  # restatement re-sent
    junk = val[(hv >= 0.5) & (hv < 0.5 + cfg.dq_error_rate / 5)].copy()
    junk["account_id"] = "AC_UNKNOWN"
    junk["market_value_cad"] = -junk.market_value_cad
    val = pd.concat([val, dup, junk], ignore_index=True)
    val["_file_date"] = val.as_of_date
    out["valuations"] = val
    out["_truth"] = pd.DataFrame({"account_id": acc_ids[ai], "as_of_date": days.date[ti],
                                  "market_value_cad": MV[ai, ti], "net_flow_cad": NETF[ai, ti],
                                  "true_return": TRUE_R[ai, ti]})

    # ---------------------------------------------------------------- transactions
    rt = _rng(cfg, 13)
    rs, rch = _rng(cfg, 15), _rng(cfg, 16)
    frames = []
    for name in ("DEPOSIT", "WITHDRAWAL", "TRANSFER_IN", "TRANSFER_OUT", "FEE"):
        arr = np.concatenate(tx[name]) if tx[name] else np.zeros((0, 3))
        if len(arr) == 0:
            continue
        frames.append(pd.DataFrame({"acc": arr[:, 0].astype(int), "t": arr[:, 1].astype(int),
                                    "txn_type": name, "gross_amount_cad": arr[:, 2], "commission_cad": 0.0}))
    arr = np.concatenate(tx["TRADE"]) if tx["TRADE"] else np.zeros((0, 4))
    if len(arr):
        side = np.where(rt.random(len(arr)) < 0.55, "BUY", "SELL")
        frames.append(pd.DataFrame({"acc": arr[:, 0].astype(int), "t": arr[:, 1].astype(int), "txn_type": side,
                                    "gross_amount_cad": np.where(side == "BUY", -arr[:, 2], arr[:, 2]),
                                    "commission_cad": arr[:, 3]}))
    for c_, name, rate in ((0, "DIVIDEND", .006), (3, "INTEREST", .008)):   # informational income
        qi = np.where(quarter_last)[0]
        for t in qi:
            ok = ~np.isnan(MV[:, t]) & (W[:, c_] > .02)
            ix = np.nonzero(ok)[0]
            frames.append(pd.DataFrame({"acc": ix, "t": t, "txn_type": name,
                                        "gross_amount_cad": np.round(MV[ix, t] * W[ix, c_] * rate, 2),
                                        "commission_cad": 0.0}))
    txn = pd.concat(frames, ignore_index=True).sort_values(["t", "acc", "txn_type"], kind="stable")
    txn = txn.reset_index(drop=True)
    txn["txn_id"] = [f"T{cfg.seed:02d}{t:04d}{i:07d}" for i, t in zip(range(len(txn)), txn.t)]
    txn["account_id"] = acc_ids[txn.acc]
    txn["trade_date"] = days.date[txn.t]
    txn["settle_date"] = (pd.to_datetime(txn.trade_date) + pd.offsets.BDay(1)).dt.date
    # security / quantity / price for trades and income
    slot_df = pd.DataFrame(slots, columns=["acc", "sec", "w"]).astype({"acc": int, "sec": int})
    first_slot = slot_df[slot_df.w > 0].groupby("acc").sec.apply(np.array)
    is_sec = txn.txn_type.isin(["BUY", "SELL", "DIVIDEND", "INTEREST"]).values
    sec_idx = np.full(len(txn), -1)
    for i in np.where(is_sec)[0]:
        cands = first_slot.get(txn.acc.iat[i], np.array([0]))
        sec_idx[i] = cands[int(rs.integers(0, len(cands)))]
    txn["security_id"] = np.where(sec_idx >= 0, sec.security_id.values[np.maximum(sec_idx, 0)], None)
    price_local = np.where(sec_idx >= 0, px[txn.t.values, np.maximum(sec_idx, 0)], np.nan)
    fx_t = np.where((sec_idx >= 0) & (sec.currency.values[np.maximum(sec_idx, 0)] == "USD"), usdcad[txn.t.values], 1.0)
    q = np.where(txn.txn_type.isin(["BUY", "SELL"]), np.abs(txn.gross_amount_cad) / (price_local * fx_t), np.nan)
    txn["quantity"] = np.round(q, 4)
    txn["price"] = np.where(txn.txn_type.isin(["BUY", "SELL"]), np.round(price_local, 4), np.nan)
    txn["channel"] = rch.choice(["Advisor", "Online", "Mobile", "Branch"], len(txn), p=[.45, .3, .2, .05])
    txn.loc[txn.txn_type == "FEE", "channel"] = "System"
    txn["file_ts"] = pd.to_datetime(txn.trade_date) + pd.Timedelta(hours=18) + \
        pd.to_timedelta(rch.integers(0, 180, len(txn)), unit="m")
    txn = txn.drop(columns=["acc", "t"])
    # DQ defects
    hv = _hash_pick(txn[["txn_id"]])
    q_ = cfg.dq_error_rate
    d1 = txn[hv < q_].copy()
    d1["file_ts"] = d1.file_ts + pd.Timedelta(days=1)                      # duplicate re-send
    d2 = txn[(hv >= .2) & (hv < .2 + q_ / 3)].copy()
    d2["account_id"], d2["txn_id"] = None, "X" + d2.txn_id.str[1:]         # orphan / null key
    d3 = txn[(hv >= .4) & (hv < .4 + q_ / 4)].copy()
    d3["txn_type"], d3["txn_id"] = "ADJ??", "Y" + d3.txn_id.str[1:]        # unknown type
    d4 = txn[(hv >= .6) & (hv < .6 + q_ / 5)].copy()
    d4["trade_date"] = (pd.to_datetime(d4.trade_date) + pd.Timedelta(days=400)).dt.date
    d4["txn_id"] = "Z" + d4.txn_id.str[1:]                                 # future-dated
    txn["_file_date"] = txn.trade_date
    for d_ in (d1, d2, d3, d4):
        d_["_file_date"] = (d_.file_ts.dt.normalize()).dt.date
    txn = pd.concat([txn, d1, d2, d3, d4], ignore_index=True)
    txn["_file_date"] = [min(fd, end.date()) for fd in txn._file_date]
    out["transactions"] = txn

    # ---------------------------------------------------------------- month-end holdings
    rows = []
    me_idx = np.where(month_last)[0]
    if len(slot_df):
        for t in me_idx:
            m = MV[slot_df.acc.values, t]
            ok = ~np.isnan(m) & (m > 0)
            s = slot_df[ok]
            mv_pos = np.round(m[ok] * s.w.values, 2)
            fx_s = np.where(sec.currency.values[s.sec.values] == "USD", usdcad[t], 1.0)
            p = px[t, s.sec.values]
            rows.append(pd.DataFrame({"account_id": acc_ids[s.acc.values], "as_of_date": days[t].date(),
                                      "security_id": sec.security_id.values[s.sec.values],
                                      "quantity": np.round(mv_pos / (p * fx_s), 4), "price_local": np.round(p, 4),
                                      "fx_rate_to_cad": np.round(fx_s, 5), "market_value_cad": mv_pos}))
    hold = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["account_id", "as_of_date", "security_id", "quantity", "price_local", "fx_rate_to_cad", "market_value_cad"])
    hold["_file_date"] = hold.as_of_date
    out["holdings"] = hold

    # ---------------------------------------------------------------- monthly behavioural feeds
    months = pd.period_range(days[0], days[-1], freq="M")
    hh_open_d = pd.to_datetime(hh.onboard_date).values
    hh_close_d = np.where(t_close < T, horizon[np.minimum(t_close, H - 1)].values,
                          np.datetime64("2262-01-01", "ns"))
    seg_rate = np.array([.35, .9, 1.8])
    crm, sess, cases, nps = [], [], [], []
    c_age = start.year - birth
    c_rate = np.where(c_age < 45, 4.0, np.where(c_age < 65, 2.2, 0.8))
    c2_treat_c = hh_treat[c2][c_hh] if c2 else np.zeros(nc, bool)
    c2_camp = next((c for c in camps if c["campaign_id"] == c2), None)
    c1_camp = next((c for c in camps if c["campaign_id"] == c1), None)
    for mi, per in enumerate(months):
        rm = _rng(cfg, 14, mi)
        m0, m1 = per.start_time, min(per.end_time.normalize(), end)
        mdays = pd.date_range(m0, m1, freq="D")
        act = (hh_open_d <= np.datetime64(m1)) & (hh_close_d >= np.datetime64(m0)) & alive_hh
        # CRM interactions
        lam = seg_rate[seg] * diligence[hh_adv] * len(mdays) / 30.5
        if c1_camp is not None and _d(c1_camp["start_date"]) <= m1 and m0 <= _d(c1_camp["end_date"]):
            lam = lam + hh_treat[c1] * TRUE_EFFECTS["CMP-2026-01"]["interaction_rate_add"]
        n_i = np.where(act, rm.poisson(lam), 0)
        hix = np.repeat(np.arange(nh), n_i)
        if len(hix):
            itype = rm.choice(["Call", "Email", "Meeting", "Portfolio Review", "Financial Plan"], len(hix),
                              p=[.38, .27, .2, .12, .03])
            crm.append(pd.DataFrame({
                "household_id": hh.household_id.values[hix], "advisor_id": hh.advisor_id.values[hix],
                "interaction_date": rm.choice(mdays, len(hix)).astype("datetime64[D]").astype(object),
                "interaction_type": itype,
                "duration_min": np.where(np.isin(itype, ["Meeting", "Portfolio Review", "Financial Plan"]),
                                         rm.integers(30, 120, len(hix)), rm.integers(5, 25, len(hix))),
                "outcome": rm.choice(["Completed", "Follow-up Required", "No Answer"], len(hix), p=[.75, .17, .08])}))
        # Digital sessions (client level)
        c_act = act[c_hh]
        lam_c = c_rate * len(mdays) / 30.5
        if c2_camp is not None and _d(c2_camp["start_date"]) <= m1 and m0 <= _d(c2_camp["measurement_end_date"]):
            lam_c = lam_c * np.where(c2_treat_c, TRUE_EFFECTS["CMP-2026-02"]["session_rate_mult"], 1.0)
        n_s = np.where(c_act, rm.poisson(lam_c), 0)
        cix = np.repeat(np.arange(nc), n_s)
        if len(cix):
            ts = rm.choice(mdays, len(cix)) + pd.to_timedelta(rm.integers(6 * 3600, 23 * 3600, len(cix)), unit="s")
            sess.append(pd.DataFrame({
                "session_id": [f"S{mi:03d}{i:08d}" for i in range(len(cix))],
                "client_id": cli.client_id.values[cix], "household_id": cli.household_id.values[cix],
                "session_ts": ts, "channel": rm.choice(["Web", "Mobile App"], len(cix), p=[.45, .55]),
                "primary_feature": rm.choice(["Portfolio View", "Statements", "Transfers", "Trading",
                                              "Documents", "Goals Planner"], len(cix), p=[.4, .15, .15, .1, .1, .1]),
                "duration_sec": rm.integers(30, 1500, len(cix))}))
        # Service cases
        n_c = np.where(act, rm.poisson(0.045 * (1.8 - diligence[hh_adv]) * len(mdays) / 30.5), 0)
        kix = np.repeat(np.arange(nh), n_c)
        if len(kix):
            cat = rm.choice(["Complaint", "Service Request", "Money Movement Issue", "Tax Slip Request"], len(kix),
                            p=[.18, .55, .17, .10])
            opened = rm.choice(mdays, len(kix)) + pd.to_timedelta(rm.integers(8 * 3600, 18 * 3600, len(kix)), unit="s")
            hrs = rm.lognormal(np.log(30), .8, len(kix)) * np.where(cat == "Complaint", 1.8, 1.0)
            closed = pd.to_datetime(opened) + pd.to_timedelta(hrs, unit="h")
            closed = closed.where(closed <= end + pd.Timedelta(hours=20))
            cases.append(pd.DataFrame({
                "case_id": [f"CS{mi:03d}{i:06d}" for i in range(len(kix))],
                "household_id": hh.household_id.values[kix], "advisor_id": hh.advisor_id.values[kix],
                "category": cat, "opened_ts": opened, "closed_ts": closed,
                "status": np.where(closed.isna(), "Open", "Closed"),
                "channel": rm.choice(["Phone", "Email", "Branch", "Secure Message"], len(kix))}))
        # NPS (relationship survey, ~4%/month sample)
        surv = act & (rm.random(nh) < 0.04)
        six = np.where(surv)[0]
        if len(six):
            score = np.clip(np.round(rm.normal(7.9 + 2.0 * (diligence[hh_adv[six]] - 1) + .25 * seg[six], 1.7)), 0, 10)
            nps.append(pd.DataFrame({
                "response_id": [f"NPS{mi:03d}{i:06d}" for i in range(len(six))],
                "household_id": hh.household_id.values[six], "advisor_id": hh.advisor_id.values[six],
                "response_date": rm.choice(mdays, len(six)).astype("datetime64[D]").astype(object),
                "score": score.astype(int),
                "survey_type": "Relationship"}))

    def _cat(lst, cols):
        return pd.concat(lst, ignore_index=True) if lst else pd.DataFrame(columns=cols)

    out["crm_interactions"] = _cat(crm, ["household_id"])
    if len(out["crm_interactions"]):
        ci_ = out["crm_interactions"]
        ci_.insert(0, "interaction_id", [f"I{i:09d}" for i in range(1, len(ci_) + 1)])
        ci_["interaction_date"] = pd.to_datetime(ci_.interaction_date).dt.date
        ci_["_file_date"] = ci_.interaction_date
    out["digital_sessions"] = _cat(sess, ["session_id"])
    if len(out["digital_sessions"]):
        out["digital_sessions"]["_file_date"] = pd.to_datetime(out["digital_sessions"].session_ts).dt.date
    out["service_cases"] = _cat(cases, ["case_id"])
    if len(out["service_cases"]):
        out["service_cases"]["_file_date"] = pd.to_datetime(out["service_cases"].opened_ts).dt.date
    out["nps_responses"] = _cat(nps, ["response_id"])
    if len(out["nps_responses"]):
        out["nps_responses"]["response_date"] = pd.to_datetime(out["nps_responses"].response_date).dt.date
        out["nps_responses"]["_file_date"] = out["nps_responses"].response_date
    for k_, v in out.items():
        if "_file_date" in v.columns:
            out[k_] = v[pd.to_datetime(v._file_date) <= end].reset_index(drop=True)
    return out


# --------------------------------------------------------------------------------------
# Writing to the landing zone
# --------------------------------------------------------------------------------------
_FMT = {}


def _fmt(feed: str) -> str:
    if not _FMT:
        from .config import FEEDS
        _FMT.update({f.name: f.fmt for f in FEEDS})
    return _FMT[feed]


def write_landing(frames: dict[str, pd.DataFrame], base_path: str, since: str | None = None,
                  granularity: str = "month") -> dict[str, int]:
    """Write feeds as partitioned files: ``{base}/{feed}/{period}/part-00000.{ext}``.

    ``since``: only write rows whose ``_file_date`` is strictly after this date (incremental
    mode). ``granularity``: 'month' for history backfills (fewer, larger files) or 'day'
    to mimic a real daily file drop.
    """
    written = {}
    for feed, df in frames.items():
        if feed.startswith("_") or df is None or len(df) == 0:
            continue
        d = df.copy()
        fd = pd.to_datetime(d.pop("_file_date"))
        if since:
            keep = fd > pd.Timestamp(since)
            d, fd = d[keep.values], fd[keep.values]
        if len(d) == 0:
            continue
        key = fd.dt.strftime("%Y-%m") if granularity == "month" else fd.dt.strftime("%Y-%m-%d")
        ext = _fmt(feed)
        for period, part in d.groupby(key.values):
            folder = os.path.join(base_path, feed, f"period={period}")
            os.makedirs(folder, exist_ok=True)
            suffix = f"{since or 'hist'}".replace("-", "")
            path = os.path.join(folder, f"part-{suffix}.{ext}")
            part = part.reset_index(drop=True)
            if ext == "parquet":
                part.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
            elif ext == "csv":
                part.to_csv(path, index=False)
            else:
                p2 = part.copy()
                for c in p2.columns:
                    if pd.api.types.is_datetime64_any_dtype(p2[c]):
                        p2[c] = p2[c].dt.strftime("%Y-%m-%dT%H:%M:%S")
                p2.to_json(path, orient="records", lines=True, date_format="iso", default_handler=str)
            written[feed] = written.get(feed, 0) + len(part)
    return written


def read_state(base_path: str) -> dict:
    p = os.path.join(base_path, "_generator_state.json")
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return {}


def write_state(base_path: str, cfg: GenConfig, last_date: str) -> None:
    os.makedirs(base_path, exist_ok=True)
    with open(os.path.join(base_path, "_generator_state.json"), "w") as f:
        json.dump({"last_written_date": last_date, "config": asdict(cfg)}, f, indent=2, default=str)


_ = (ACCOUNT_TYPES, ASSET_CLASSES)  # re-exported for notebooks
