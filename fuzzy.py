from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import re
from typing import Optional

from text_normalize import normalize


@dataclass(frozen=True)
class ScoredCandidate:
    value: str
    score: float


def similarity(a: str, b: str) -> float:
    na = _normalize_for_league_scoring(a)
    nb = _normalize_for_league_scoring(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def _normalize_for_league_scoring(value: str) -> str:
    text = normalize(value)
    replacements = (
        (r"\bmls\b", "major league soccer"),
        (r"\bepl\b", "english premier league"),
        (r"\bser\b", "serie"),
    )
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text)
    return normalize(text)


def is_corners_league(title: str) -> bool:
    text = f" {_normalize_for_league_scoring(title)} "
    return "corners" in text or " corn " in text or text.endswith(" cor") or " cor " in text


def is_bookings_league(title: str) -> bool:
    text = _normalize_for_league_scoring(title)
    return "bookings" in text or " booking" in text


def detect_metric_hint_from_tournament(title: str) -> Optional[str]:
    text = _normalize_for_league_scoring(title)
    if not text:
        return None
    if is_corners_league(text) or re.search(r"\bугл", text):
        return "corners"
    if is_bookings_league(text) or re.search(r"\bжк\b|карточ", text):
        return "bookings"
    return None


def league_passes_metric(title: str, metric: str) -> bool:
    metric_lower = _normalize_for_league_scoring(metric)
    corners = is_corners_league(title)
    bookings = is_bookings_league(title)
    if metric_lower == "corners":
        return corners
    if metric_lower == "bookings":
        return bookings
    return not corners and not bookings


def strip_league_metric_suffix(title: str) -> str:
    text = _normalize_for_league_scoring(title)
    replacements = (
        "corners",
        "corner",
        "bookings",
        "booking",
        " cor",
        " corn",
    )
    for token in replacements:
        text = text.replace(token, " ")
    return normalize(text)


def score_league_candidate(candidate_title: str, target_title: str, metric: str) -> float:
    base_candidate = strip_league_metric_suffix(candidate_title)
    base_target = strip_league_metric_suffix(target_title)
    score = similarity(base_candidate, base_target)

    metric_lower = normalize(metric)
    if metric_lower == "corners":
        score += 0.25 if is_corners_league(candidate_title) else -0.35
    elif metric_lower == "bookings":
        score += 0.25 if is_bookings_league(candidate_title) else -0.35
    else:
        score += -0.25 if (is_corners_league(candidate_title) or is_bookings_league(candidate_title)) else 0.05

    return score


def pick_best(
    candidates: list[str],
    target: str,
    threshold: float = 0.0,
    top_n: int = 5,
) -> list[ScoredCandidate]:
    scored = [
        ScoredCandidate(value=candidate, score=similarity(candidate, target))
        for candidate in candidates
    ]
    scored.sort(key=lambda item: item.score, reverse=True)
    return [item for item in scored if item.score >= threshold][: max(top_n, 1)]
