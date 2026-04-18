import base64
from types import SimpleNamespace
from urllib.parse import quote

from bet_intent import BetIntent
from blogabet_publisher import (
    _detect_match_sport_key,
    _contains_handicap_marker,
    _coupon_matches_intent,
    _line_diff_for_market,
    _parse_odd_button_onclick_payload,
    _clean_team_label,
    build_league_selection_plan,
    is_recoverable_submit_error,
    resolve_forced_league_alias,
    resolve_period_tab_request,
    tab_text_matches_synonyms,
)


def _intent(*, market: str, period: str) -> BetIntent:
    return BetIntent(
        metric="goals",
        period=period,
        scope="match",
        market=market,
        side="over",
        line=2.5,
        is_live=False,
        current_score=None,
        raw_text="",
    )


def _moneyline_intent(side: str = "home") -> BetIntent:
    return BetIntent(
        metric="goals",
        period="ft",
        scope="match",
        market="moneyline",
        side=side,
        line=None,
        is_live=False,
        current_score=None,
        raw_text="П1",
    )


def _onclick_with_payload(body: str) -> str:
    serialized = f's:{len(body)}:"{body}";'
    encoded = base64.b64encode(serialized.encode("utf-8")).decode("ascii")
    return f"return updateCoupon('{quote(encoded, safe='')}',this)"


def test_clean_team_label_strips_corners_bookings_suffixes() -> None:
    assert _clean_team_label("H Arsenal (Corners)") == "arsenal"
    assert _clean_team_label("Liverpool Bookings") == "liverpool"
    assert _clean_team_label("Chelsea Cards") == "chelsea"
    assert _clean_team_label("Cornerstone United") == "cornerstone united"


def test_alias_lookup_forces_exact_league_target() -> None:
    aliases = {
        "Austria - Bundesliga Corners": "Austrian Cor",
    }
    available_titles = [
        "austrian cor",
        "austria bundesliga",
        "england premier league",
    ]
    alias_info = resolve_forced_league_alias(
        "Austria - Bundesliga Corners",
        aliases,
        available_titles,
    )
    assert alias_info["has_alias"] is True
    assert alias_info["found"] is True
    assert alias_info["matched_title"] == "austrian cor"

    plan = build_league_selection_plan(
        [
            {"index": 0, "title": "austrian cor"},
            {"index": 1, "title": "austria bundesliga corners"},
            {"index": 2, "title": "england premier league"},
        ],
        "Austria - Bundesliga Corners",
        "corners",
        league_aliases=aliases,
        fallback_top_n=7,
    )
    assert plan["ordered_candidates"][0]["title"] == "austrian cor"
    assert plan["ordered_candidates"][0]["method"] == "forced_alias"


def test_odds_changed_is_recoverable_submit_error() -> None:
    assert is_recoverable_submit_error("\u00d7 odds changed to 1 806") is True
    assert is_recoverable_submit_error("odds dropped to 2 130") is True
    assert is_recoverable_submit_error("price has changed") is True
    assert is_recoverable_submit_error("insufficient balance") is False


def test_period_tab_synonyms_resolution() -> None:
    first_half_request = resolve_period_tab_request(_intent(market="total", period="1h"))
    assert first_half_request["primary"] == "First Half"
    assert "1st Half" in first_half_request["synonyms"]
    assert tab_text_matches_synonyms("1H", first_half_request["synonyms"]) is True

    third_quarter_request = resolve_period_tab_request(_intent(market="total", period="q3"))
    assert third_quarter_request["primary"] == "third quarter"
    assert "3rd quarter" in third_quarter_request["synonyms"]
    assert tab_text_matches_synonyms("Q3", third_quarter_request["synonyms"]) is True

    fourth_quarter_request = resolve_period_tab_request(_intent(market="total", period="q4"))
    assert fourth_quarter_request["primary"] == "fourth quarter"
    assert "4th quarter" in fourth_quarter_request["synonyms"]
    assert tab_text_matches_synonyms("Q4", fourth_quarter_request["synonyms"]) is True

    team_total_request = resolve_period_tab_request(_intent(market="team_total", period="ft"))
    assert team_total_request["primary"] == "Team Total"
    assert "Team Totals" in team_total_request["synonyms"]
    assert tab_text_matches_synonyms("Team Totals", team_total_request["synonyms"]) is True


def test_detect_match_sport_key_from_q4_period() -> None:
    intent = _intent(market="total", period="q4")
    match = SimpleNamespace(
        tournament="Friendly League",
        home_team="Home",
        away_team="Away",
        rate_description="",
        href="",
    )
    assert _detect_match_sport_key(match, intent) == "basketball"


def test_detect_match_sport_key_from_q3_period() -> None:
    intent = _intent(market="total", period="q3")
    match = SimpleNamespace(
        tournament="Friendly League",
        home_team="Home",
        away_team="Away",
        rate_description="",
        href="",
    )
    assert _detect_match_sport_key(match, intent) == "basketball"


def test_detect_match_sport_key_from_markers() -> None:
    intent = _intent(market="total", period="ft")
    match = SimpleNamespace(
        tournament="NBA",
        home_team="Home",
        away_team="Away",
        rate_description="",
        href="",
    )
    assert _detect_match_sport_key(match, intent) == "basketball"


def test_coupon_moneyline_rejects_handicap_coupon_text() -> None:
    ok, diag = _coupon_matches_intent(
        "Full Event Home +0.75 (AH) (1 - 0) @ 1.568",
        _moneyline_intent("home"),
    )
    assert ok is False
    assert diag["intent_has_handicap_marker"] is True


def test_coupon_moneyline_accepts_plain_home_text() -> None:
    ok, diag = _coupon_matches_intent(
        "Full Event Home @ 1.568",
        _moneyline_intent("home"),
    )
    assert ok is True
    assert diag["intent_has_handicap_marker"] is False


def test_coupon_moneyline_with_live_score_is_not_handicap() -> None:
    ok, diag = _coupon_matches_intent(
        "live Japan - North Korea FT Home (1 - 0) Odd: 1.219 Bookmaker: Pinnacle",
        _moneyline_intent("home"),
    )
    assert ok is True
    assert diag["intent_has_handicap_marker"] is False


def test_contains_handicap_marker_detects_signed_line() -> None:
    assert _contains_handicap_marker("H +0.75 1.568") is True
    assert _contains_handicap_marker("A -0.25 2.620") is True
    assert _contains_handicap_marker("live score 1 - 0") is False


def test_parse_odd_button_onclick_payload_moneyline() -> None:
    onclick = _onclick_with_payload(
        "isLive=true^|^marketName=moneyline^|^pick=Home^|^odd=1.149^|^extra=moneyline:home"
    )
    parsed = _parse_odd_button_onclick_payload(onclick)
    assert parsed["market_name"] == "moneyline"
    assert parsed["pick"] == "home"


def test_parse_odd_button_onclick_payload_spreads_line() -> None:
    onclick = _onclick_with_payload(
        "isLive=true^|^marketName=spreads^|^pick=Away^|^line=-0.75^|^extra=spreads:away:hdp"
    )
    parsed = _parse_odd_button_onclick_payload(onclick)
    assert parsed["market_name"] == "spreads"
    assert parsed["pick"] == "away"
    assert parsed["line"] == -0.75


def test_line_diff_for_market_handicap_uses_absolute_line_value() -> None:
    assert _line_diff_for_market("handicap", +1.0, +1.0) == 0.0
    assert _line_diff_for_market("handicap", -1.0, +1.0) == 0.0
    assert _line_diff_for_market("handicap", -0.75, +1.0) == 0.25


def test_line_diff_for_market_total_keeps_sign() -> None:
    assert _line_diff_for_market("total", -1.0, +1.0) == 2.0
