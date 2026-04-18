from types import SimpleNamespace

from bet_intent import BetIntent
from blogabet_publisher import (
    _detect_match_sport_key,
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
