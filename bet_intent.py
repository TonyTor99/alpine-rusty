from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


_NUMBER_RE = r"[-+]?\d+(?:[\.,]\d+)?"


@dataclass(frozen=True)
class BetIntent:
    metric: str  # goals | corners | bookings
    period: str  # ft | 1h | 2h
    scope: str  # match | home | away
    market: str  # total | team_total | handicap | moneyline
    side: str  # over | under | home | away | draw | plus | minus
    line: Optional[float]
    is_live: bool
    current_score: Optional[str]
    raw_text: str


def _normalize(raw_text: str) -> str:
    text = (raw_text or "").lower().replace("ё", "е")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _compact(text: str) -> str:
    return re.sub(r"[^a-zа-я0-9+\-\.,]", "", text.lower())


def _parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    normalized = value.replace(",", ".").strip()
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _find_first_line(text: str) -> Optional[float]:
    match = re.search(_NUMBER_RE, text)
    if match is None:
        return None
    return _parse_float(match.group(0))


def _detect_metric(text: str) -> str:
    if re.search(r"угл|corner|corn\b|корнер", text):
        return "corners"
    if re.search(r"карточ|\bжк\b|book", text):
        return "bookings"
    return "goals"


def _detect_period(text: str) -> str:
    if re.search(
        r"2\s*(?:-?\s*я)?\s*полов|втор[ао]я\s+полов|2nd\s+half|second\s+half|2\s*half",
        text,
    ):
        return "2h"
    if re.search(
        r"1\s*(?:-?\s*я)?\s*полов|перв[ао]я\s+полов|1st\s+half|first\s+half|1\s*half",
        text,
    ):
        return "1h"
    return "ft"


def _detect_live_and_score(text: str) -> tuple[bool, Optional[str]]:
    score_match = re.search(r"(\d{1,2}\s*[:\-]\s*\d{1,2})", text)
    current_score = None
    if score_match is not None:
        current_score = re.sub(r"\s+", "", score_match.group(1)).replace("-", ":")

    is_live = bool(
        re.search(r"при\s+текущ|текущ[а-я]*\s+счет|\blive\b", text)
        or current_score
    )
    return is_live, current_score


def parse_bet_intent(ocr_text: str) -> BetIntent:
    raw_text = (ocr_text or "").strip()
    text = _normalize(raw_text)
    compact = _compact(text)

    metric = _detect_metric(text)
    period = _detect_period(text)
    is_live, current_score = _detect_live_and_score(text)

    # Team total: ИТБ1 / ИТМ2 / itb1 / itm2
    team_total_match = re.search(r"ит([бм])([12])", compact)
    if team_total_match:
        side_token = team_total_match.group(1)
        team_token = team_total_match.group(2)
        scope = "home" if team_token == "1" else "away"
        side = "over" if side_token == "б" else "under"

        line_match = re.search(r"ит[бм][12][^\d+-]*({})".format(_NUMBER_RE), text)
        line = _parse_float(line_match.group(1)) if line_match else _find_first_line(text)
        return BetIntent(
            metric=metric,
            period=period,
            scope=scope,
            market="team_total",
            side=side,
            line=line,
            is_live=is_live,
            current_score=current_score,
            raw_text=raw_text,
        )

    # Handicap: ФОРА1(-0.5) / Ф1(+1)
    handicap_match = re.search(r"ф(?:ора)?\s*([12])", text)
    if handicap_match:
        scope = "home" if handicap_match.group(1) == "1" else "away"
        line_match = re.search(r"ф(?:ора)?\s*[12][^\d+-]*({})".format(_NUMBER_RE), text)
        line = _parse_float(line_match.group(1)) if line_match else _find_first_line(text)
        normalized_line = line if line is not None else 0.0
        side = "plus" if normalized_line >= 0 else "minus"
        return BetIntent(
            metric=metric,
            period=period,
            scope=scope,
            market="handicap",
            side=side,
            line=line,
            is_live=is_live,
            current_score=current_score,
            raw_text=raw_text,
        )

    # Moneyline: П1 / П2 / Х
    if re.search(r"\bп\s*1\b|\bp\s*1\b", text):
        return BetIntent(
            metric=metric,
            period=period,
            scope="match",
            market="moneyline",
            side="home",
            line=None,
            is_live=is_live,
            current_score=current_score,
            raw_text=raw_text,
        )
    if re.search(r"\bп\s*2\b|\bp\s*2\b", text):
        return BetIntent(
            metric=metric,
            period=period,
            scope="match",
            market="moneyline",
            side="away",
            line=None,
            is_live=is_live,
            current_score=current_score,
            raw_text=raw_text,
        )
    if re.search(r"\b[хx]\b|draw", text):
        return BetIntent(
            metric=metric,
            period=period,
            scope="match",
            market="moneyline",
            side="draw",
            line=None,
            is_live=is_live,
            current_score=current_score,
            raw_text=raw_text,
        )

    # Total: ТБ / ТМ / over / under
    total_side: Optional[str] = None
    if re.search(r"\bт\s*б\b|\bt\s*b\b|\bover\b", text):
        total_side = "over"
    elif re.search(r"\bт\s*м\b|\bt\s*m\b|\bunder\b", text):
        total_side = "under"

    if total_side is None:
        total_side = "over"

    total_line_match = re.search(r"(?:\bт\s*[бм]\b|\bover\b|\bunder\b)[^\d+-]*({})".format(_NUMBER_RE), text)
    line = _parse_float(total_line_match.group(1)) if total_line_match else _find_first_line(text)

    return BetIntent(
        metric=metric,
        period=period,
        scope="match",
        market="total",
        side=total_side,
        line=line,
        is_live=is_live,
        current_score=current_score,
        raw_text=raw_text,
    )
