from bet_intent import parse_bet_intent


def test_parse_corners_total_over_ft() -> None:
    intent = parse_bet_intent("Угловые ТБ 9.5 Основная игра")
    assert intent.metric == "corners"
    assert intent.period == "ft"
    assert intent.market == "total"
    assert intent.side == "over"
    assert intent.line == 9.5


def test_parse_bookings_total_under_first_half() -> None:
    intent = parse_bet_intent("ЖК ТМ 4,5 1-я половина")
    assert intent.metric == "bookings"
    assert intent.period == "1h"
    assert intent.market == "total"
    assert intent.side == "under"
    assert intent.line == 4.5


def test_parse_team_total_home_over() -> None:
    intent = parse_bet_intent("ИТБ1(1.5) Вся игра")
    assert intent.market == "team_total"
    assert intent.scope == "home"
    assert intent.side == "over"
    assert intent.line == 1.5


def test_parse_team_total_away_under_second_half() -> None:
    intent = parse_bet_intent("ИТМ2 0.5 2-я половина")
    assert intent.market == "team_total"
    assert intent.scope == "away"
    assert intent.side == "under"
    assert intent.period == "2h"
    assert intent.line == 0.5


def test_parse_handicap_away_plus() -> None:
    intent = parse_bet_intent("ФОРА2 (+1.25) Основная игра")
    assert intent.market == "handicap"
    assert intent.scope == "away"
    assert intent.side == "plus"
    assert intent.line == 1.25


def test_parse_moneyline_live_score() -> None:
    intent = parse_bet_intent("П1 При текущем счете 1:0")
    assert intent.market == "moneyline"
    assert intent.side == "home"
    assert intent.is_live is True
    assert intent.current_score == "1:0"


def test_parse_moneyline_draw() -> None:
    intent = parse_bet_intent("Х")
    assert intent.market == "moneyline"
    assert intent.side == "draw"


def test_parse_total_under_fourth_quarter() -> None:
    intent = parse_bet_intent("4-я четверть ТМ (38)")
    assert intent.period == "q4"
    assert intent.market == "total"
    assert intent.side == "under"
    assert intent.line == 38.0


def test_parse_total_under_third_time() -> None:
    intent = parse_bet_intent("3 тайм ТМ (42.5)")
    assert intent.period == "q3"
    assert intent.market == "total"
    assert intent.side == "under"
    assert intent.line == 42.5
