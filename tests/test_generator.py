"""The synthetic data must be realistic *and* internally consistent."""
import dataclasses

import numpy as np
import pandas as pd

from wm_common.config import FEEDS, SCALE_PRESETS
from wm_common.generator import generate


def _clean_vals(v):
    v = v[v.market_value_cad >= 0]
    return v.sort_values("file_ts").drop_duplicates(["account_id", "as_of_date"], keep="last")


def test_every_feed_is_produced(frames):
    for f in FEEDS:
        assert f.name in frames and len(frames[f.name]) > 0, f.name


def test_accounting_identity_reproduces_true_returns(frames):
    """TWR inputs (valuations + external flows) must reproduce the simulated returns exactly."""
    v = _clean_vals(frames["valuations"])
    t = frames["transactions"]
    t = t[t.account_id.notna() & t.txn_type.isin(["DEPOSIT", "WITHDRAWAL", "TRANSFER_IN", "TRANSFER_OUT"])]
    t = t.sort_values("file_ts").drop_duplicates("txn_id", keep="last")
    f = (t.groupby(["account_id", "trade_date"]).gross_amount_cad.sum()
         .rename("flow").reset_index().rename(columns={"trade_date": "as_of_date"}))
    d = v.merge(f, on=["account_id", "as_of_date"], how="left").fillna({"flow": 0})
    d = d.sort_values(["account_id", "as_of_date"])
    d["prev"] = d.groupby("account_id").market_value_cad.shift()
    d["r"] = (d.market_value_cad - d.prev - d.flow) / d.prev
    m = d.merge(frames["_truth"], on=["account_id", "as_of_date"]).dropna(subset=["true_return"])
    assert len(m) > 10_000
    assert np.abs(m.r - m.true_return).max() < 1e-9


def test_generation_is_append_only(frames):
    """Extending the window must not rewrite history (enables daily incremental landing)."""
    cfg = dataclasses.replace(SCALE_PRESETS["tiny"], end_date="2026-05-29")
    shorter = generate(cfg)
    a = shorter["valuations"].query("account_id != 'AC_UNKNOWN'").set_index(["account_id", "as_of_date", "file_ts"])
    b = frames["valuations"].query("account_id != 'AC_UNKNOWN'").set_index(["account_id", "as_of_date", "file_ts"])
    common = a.index.intersection(b.index)
    assert len(common) == len(a)
    assert np.allclose(a.loc[common, "market_value_cad"], b.loc[common, "market_value_cad"])


def test_campaigns_are_balanced_randomizations(frames):
    a = frames["campaign_assignments"]
    for _cid, g in a.groupby("campaign_id"):
        counts = g.assignment_group.value_counts()
        assert abs(counts["Treatment"] - counts["Control"]) <= g.stratum.nunique()
        assert not g.household_id.duplicated().any()


def test_injected_dq_defects_exist(frames):
    t = frames["transactions"]
    assert t.txn_id.duplicated().any(), "expected re-sent duplicates"
    assert t.account_id.isna().any(), "expected null keys"
    assert (~t.txn_type.isin(["DEPOSIT", "WITHDRAWAL", "TRANSFER_IN", "TRANSFER_OUT", "BUY", "SELL",
                              "DIVIDEND", "INTEREST", "FEE"])).any()
    assert (frames["valuations"].market_value_cad < 0).any()


def test_realistic_shape(frames):
    hh = frames["household_events"]
    seg_share = hh[hh.operation == "INSERT"].segment.value_counts(normalize=True)
    assert seg_share["Mass Affluent"] > seg_share["High Net Worth"] > seg_share["Ultra High Net Worth"]
    truth = frames["_truth"].dropna(subset=["true_return"])
    ann = (1 + truth.true_return.mean()) ** 252 - 1
    assert -0.15 < ann < 0.30
    fees = frames["transactions"].query("txn_type == 'FEE'").gross_amount_cad
    assert (fees < 0).all()
    assert pd.to_datetime(frames["valuations"].as_of_date).dt.dayofweek.max() <= 4
