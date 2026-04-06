from fuzzy import (
    detect_metric_hint_from_tournament,
    is_bookings_league,
    is_corners_league,
    league_passes_metric,
    pick_best,
    score_league_candidate,
    similarity,
    strip_league_metric_suffix,
)
from text_normalize import normalize


def test_normalize_ru_text() -> None:
    assert normalize("  Премьер-Лига,  Ёж  ") == "премьер лига еж"


def test_similarity_with_short_aliases() -> None:
    score = similarity("England Premier", "England - Premier League")
    assert score > 0.65


def test_pick_best_returns_ordered_candidates() -> None:
    candidates = ["Spain La Liga", "England Premier League", "Italy Serie A"]
    best = pick_best(candidates, "England Premier", threshold=0.2, top_n=2)
    assert best
    assert best[0].value == "England Premier League"


def test_league_suffix_filters() -> None:
    assert is_corners_league("Brazil Serie B - Corners") is True
    assert is_bookings_league("Italy - Serie A Bookings") is True
    assert league_passes_metric("Brazil Serie B - Corners", "corners") is True
    assert league_passes_metric("Italy - Serie A Bookings", "bookings") is True
    assert league_passes_metric("England Premier League", "goals") is True
    assert league_passes_metric("England Premier League Corners", "goals") is False


def test_strip_metric_suffix() -> None:
    assert strip_league_metric_suffix("England - Championship Corners") == "england championship"
    assert strip_league_metric_suffix("France - Ligue 1 Bookings") == "france ligue 1"


def test_score_league_candidate_penalizes_wrong_metric() -> None:
    goals_score = score_league_candidate("England Premier League", "England Premier League", "goals")
    corners_score = score_league_candidate("England Premier League Corners", "England Premier League", "goals")
    assert goals_score > corners_score


def test_detect_metric_hint_from_tournament() -> None:
    assert detect_metric_hint_from_tournament("USA - Major League Soccer Corners") == "corners"
    assert detect_metric_hint_from_tournament("USA(MLS) Cor") == "corners"
    assert detect_metric_hint_from_tournament("Spain - La Liga Bookings") == "bookings"


def test_score_league_candidate_with_alias_mls() -> None:
    corners = score_league_candidate(
        "USA(MLS) Cor",
        "USA - Major League Soccer Corners",
        "corners",
    )
    bookings = score_league_candidate(
        "USA Major League Soccer Bookings",
        "USA - Major League Soccer Corners",
        "corners",
    )
    assert corners > bookings
