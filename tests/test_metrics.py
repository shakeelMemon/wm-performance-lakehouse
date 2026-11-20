import math

import pytest

from wm_common import metrics as m


def test_daily_return_handles_flows_and_new_accounts():
    assert m.daily_return(110, 100, 5) == pytest.approx(0.05)
    assert m.daily_return(50_000, None, 50_000) is None
    assert m.daily_return(0, 0, 0) is None


def test_chain_link_is_geometric():
    assert m.chain_link([0.1, -0.1]) == pytest.approx(-0.01)
    assert m.chain_link([None, 0.05, float("nan")]) == pytest.approx(0.05)


def test_modified_dietz_matches_textbook_example():
    # Begin 100, +50 contributed half-way, end 160 -> (160-100-50)/(100+25) = 8%
    assert m.modified_dietz(100, 160, [(50, 0.5)]) == pytest.approx(0.08)
    assert m.modified_dietz(0, 0, []) is None


def test_growth_roa_nps_sla():
    assert m.organic_growth_rate(1_000_000, 100_000_000) == pytest.approx(0.12)
    assert m.return_on_assets_bps(80_000, 100_000_000) == pytest.approx(96)
    assert m.nps([10, 9, 8, 7, 6, 0]) == pytest.approx(0)
    assert m.nps([]) is None
    assert m.sla_breach_rate([10, 80, 100, 20], 72) == pytest.approx(0.5)
    assert math.isclose(m.annualize(0.1, 365), 0.1)
