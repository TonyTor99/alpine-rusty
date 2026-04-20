from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone
import inspect
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import unquote, urlsplit

from playwright.async_api import (
    Browser as AsyncBrowser,
    BrowserContext as AsyncBrowserContext,
    Page as AsyncPage,
    Playwright as AsyncPlaywright,
    async_playwright,
)

from bet_intent import BetIntent
from fuzzy import (
    detect_metric_hint_from_tournament,
    league_passes_metric,
    score_league_candidate,
    similarity,
)
from text_normalize import normalize


@dataclass(frozen=True)
class BlogabetConfig:
    enabled: bool
    storage_state_path: str
    headless: bool
    default_stake: int
    admin_tg_chat_id: str
    league_aliases_path: str = "./blogabet_league_aliases.json"
    upcoming_url: str = "https://blogabet.com/pinnacle/live"
    login_url: str = "https://blogabet.com"
    interactive_login_timeout_seconds: int = 600
    login_email: str = ""
    login_password: str = ""


@dataclass(frozen=True)
class PublishResult:
    success: bool
    pick_id: Optional[int] = None
    pick_url: Optional[str] = None
    diagnostics: Optional[dict[str, Any]] = None


class BlogabetAuthRequired(RuntimeError):
    pass


class BlogabetPublishError(RuntimeError):
    def __init__(
        self,
        step_name: str,
        reason: str,
        diagnostics: Optional[dict[str, Any]] = None,
        screenshot_path: str = "",
        html_dump_path: str = "",
    ) -> None:
        super().__init__(reason)
        self.step_name = step_name
        self.reason = reason
        self.diagnostics = diagnostics or {}
        self.screenshot_path = screenshot_path
        self.html_dump_path = html_dump_path


class SELECTORS:
    SPORTS_CONTAINER = "#sports"
    SPORT_TRIGGER_TEMPLATE = "#sports [data-target='{sport_target}']"
    SPORT_LIST_TEMPLATE = "#{sport_target}"
    LEAGUE_LINKS_TEMPLATE = "#{sport_target} .list-group-item a.odds"
    LEAGUE_TITLE = ".leagueTitle"
    EVENT_CONTAINER = "#_event"
    EVENT_TAB_LINKS = "#_event ul.nav-tabs a[data-toggle='tab']"
    ACTIVE_TAB = "#_event .tab-content .tab-pane.active, #_event .tab-content .tab-pane.in.active"
    BLOCK_CONTENT = ".block-content"
    HOME_TEAM = ".title .home"
    AWAY_TEAM = ".title .away"
    ODD_BUTTON = ".odd-btn"
    ODD_BADGE = ".badge"
    STAKE_SELECT = "#stake"
    ANALYZE_TEXTAREA = "#_analyze"
    CREATE_PICK_BUTTON = "button._couponSave"
    CREATE_PICK_BUTTON_FALLBACK = "button[onclick*='submitCoupon'], #_coupon button.btn-warning"
    COUPON_BADGE = "#_coupon .badge"
    COUPON_ERROR = "#_coupon .alert-danger, #_coupon .text-danger, #_couponBox .alert-danger"
    COUPON_PICK_CARD = "#_couponUpdate div[id^='couponBox_']"
    COUPON_REMOVE_BUTTON = (
        "#_couponUpdate div[id^='couponBox_'] a[onclick*='updateCoupon'], "
        "#_couponUpdate div[id^='couponBox_'] a.btn.btn-xs.btn-darken.pull-right"
    )
    COUPON_LIVE_SCORE_INPUT = "#_couponUpdate div[id^='couponBox_'] input[name^='score_']"
    COUPON_PICK_TITLE = "h6"
    LOGIN_TRIGGER = (
        "a[data-target='#systemModal'][data-toggle='modal'], "
        "a[href='#systemModal'], "
        "a.btn.btn-outline:has-text('LOG IN'), "
        "a:has-text('Log in'), "
        "a:has-text('Login'), "
        "a:has-text('Sign in'), "
        "button:has-text('Log in'), "
        "button:has-text('Sign in')"
    )
    LOGIN_MODAL = "#systemModal"
    LOGIN_FORM = (
        "form#form-login, "
        "#systemModal form#form-login, "
        "form[action*='login'], "
        "form[action*='signin'], "
        "#login-form, "
        "#login-form form"
    )
    LOGIN_EMAIL_INPUT = (
        "input[name='email'], input#email, input[type='email'], "
        "input[name='username'], input[name='login']"
    )
    LOGIN_PASSWORD_INPUT = "input[name='password'], input#password, input[type='password']"
    LOGIN_SUBMIT_BUTTON = "button[type='submit'], input[type='submit']"


_TEAM_LABEL_MARKET_SUFFIXES = ("corners", "corner", "bookings", "booking", "cards", "card")
_RECOVERABLE_SUBMIT_MARKERS = (
    "odds dropped",
    "odds changed",
    "odds has changed",
    "odds were changed",
    "price changed",
    "price has changed",
)
_DEFAULT_LEAGUE_FALLBACK_CANDIDATES = 7
_PERIOD_TAB_SYNONYMS: dict[str, tuple[str, ...]] = {
    "full_event": ("Full Event", "Full Time", "Match"),
    "first_half": ("First Half", "1st Half", "1H"),
    "second_half": ("Second Half", "2nd Half", "2H"),
    "third_quarter": ("third quarter", "3rd quarter", "3 quarter", "q3"),
    "fourth_quarter": ("fourth quarter", "4th quarter", "4 quarter", "q4"),
    "team_total": ("Team Total", "Team Totals"),
}
_SPORT_FOOTBALL = "football"
_SPORT_BASKETBALL = "basketball"
_SPORT_TENNIS = "tennis"
_SPORT_TARGETS: dict[str, str] = {
    _SPORT_FOOTBALL: "_SOC",
    _SPORT_TENNIS: "_TNS",
    _SPORT_BASKETBALL: "_BSK",
}
_SPORT_TITLES: dict[str, str] = {
    _SPORT_FOOTBALL: "Football",
    _SPORT_TENNIS: "Tennis",
    _SPORT_BASKETBALL: "Basketball",
}
_BASKETBALL_MARKERS = (
    "баскет",
    "basket",
    "nba",
    "wnba",
    "euroleague",
    "euroliga",
    "евролига",
    "nbl",
    "overtime",
    "овертайм",
)
_TENNIS_MARKERS = (
    "теннис",
    "tennis",
    "wta",
    "atp",
    "itf",
    "challenger",
    "челленджер",
    "davis cup",
    "billie jean king",
    "wimbledon",
    "roland garros",
    "australian open",
    "us open",
    "гейм",
    "геймы",
    "геймов",
    "games",
)


def _sport_target(sport_key: str) -> str:
    return _SPORT_TARGETS.get(sport_key, _SPORT_TARGETS[_SPORT_FOOTBALL])


def _sport_title(sport_key: str) -> str:
    return _SPORT_TITLES.get(sport_key, _SPORT_TITLES[_SPORT_FOOTBALL])


def _league_links_selector_for_sport(sport_key: str) -> str:
    return SELECTORS.LEAGUE_LINKS_TEMPLATE.format(sport_target=_sport_target(sport_key))


def _detect_match_sport_key(match: Any, intent: BetIntent) -> str:
    if intent.period in {"q3", "q4"}:
        return _SPORT_BASKETBALL

    text_parts = [
        getattr(match, "tournament", ""),
        getattr(match, "home_team", ""),
        getattr(match, "away_team", ""),
        getattr(match, "rate_description", ""),
        getattr(match, "href", ""),
        intent.raw_text,
    ]
    search_blob = normalize(" ".join(str(value or "") for value in text_parts))
    if any(marker in search_blob for marker in _BASKETBALL_MARKERS):
        return _SPORT_BASKETBALL

    if any(marker in search_blob for marker in _TENNIS_MARKERS):
        return _SPORT_TENNIS

    if (
        intent.metric == "goals"
        and intent.market in {"total", "team_total"}
        and intent.line is not None
        and float(intent.line) >= 80.0
    ):
        return _SPORT_BASKETBALL

    return _SPORT_FOOTBALL


async def _first_visible(locator: Any, *, limit: int = 8) -> Any:
    total = await locator.count()
    for index in range(min(total, limit)):
        node = locator.nth(index)
        try:
            if await node.is_visible():
                return node
        except Exception:  # noqa: BLE001
            continue
    return None


async def _first_present(locator: Any, *, limit: int = 8) -> Any:
    total = await locator.count()
    for index in range(min(total, limit)):
        node = locator.nth(index)
        try:
            if await node.count() > 0:
                return node
        except Exception:  # noqa: BLE001
            continue
    return None


def _extract_float(value: str) -> Optional[float]:
    text = (value or "").strip()
    # Формат линий в Pinnacle часто приходит как "2 75" вместо "2.75".
    quarter_match = re.search(r"([-+]?\d+)\s+(00|25|50|75)\b", text)
    if quarter_match is not None:
        try:
            major = int(quarter_match.group(1))
            frac = int(quarter_match.group(2))
            sign = -1.0 if major < 0 else 1.0
            return major + sign * (frac / 100.0)
        except ValueError:
            pass

    match = re.search(r"[-+]?\d+(?:[\.,]\d+)?", value or "")
    if match is None:
        return None
    try:
        return float(match.group(0).replace(",", "."))
    except ValueError:
        return None


def _clean_team_label(value: str) -> str:
    text = normalize(value)
    text = re.sub(r"^(h|a|d)\s+", "", text, flags=re.IGNORECASE)
    text = _strip_team_market_suffix(text)
    return text.strip()


def _strip_team_market_suffix(value: str) -> str:
    text = normalize(value)
    if not text:
        return ""

    # Удаляем только хвостовые market-суффиксы, чтобы не ломать реальные имена клубов.
    suffix_pattern = "|".join(re.escape(token) for token in _TEAM_LABEL_MARKET_SUFFIXES)
    pattern = re.compile(rf"(?:^|\s)(?:{suffix_pattern})$")
    while True:
        updated = pattern.sub("", text).strip()
        if updated == text:
            break
        text = normalize(updated)
    return normalize(text)


def _contains_team_token(haystack: str, team: str) -> bool:
    target = _clean_team_label(team)
    if not target:
        return False
    return target in _clean_team_label(haystack)


def _extract_badge(value: str) -> str:
    return normalize(value).lower()


def _extract_numeric_tokens(value: str) -> list[float]:
    text = (value or "").strip()
    if not text:
        return []
    matches = re.findall(r"[-+]?\d+\s+(?:00|25|50|75)|[-+]?\d+(?:[.,]\d+)?", text)
    parsed: list[float] = []
    for item in matches:
        number = _extract_float(item)
        if number is None:
            continue
        parsed.append(number)
    return parsed


def _safe_base64_decode(value: str) -> str:
    token = (value or "").strip()
    if not token:
        return ""
    token = token.replace(" ", "+")
    token += "=" * ((4 - len(token) % 4) % 4)
    try:
        return base64.b64decode(token).decode("utf-8", "ignore")
    except Exception:  # noqa: BLE001
        return ""


def _canonical_market_name(value: str) -> str:
    compact = normalize(value).replace(" ", "")
    if not compact:
        return ""
    if "moneyline" in compact:
        return "moneyline"
    if "spread" in compact or "handicap" in compact:
        return "spreads"
    if "total" in compact:
        return "totals"
    return compact


def _canonical_pick_name(value: str) -> str:
    compact = normalize(value).replace(" ", "")
    if not compact:
        return ""
    if "home" in compact:
        return "home"
    if "away" in compact:
        return "away"
    if "draw" in compact:
        return "draw"
    if "over" in compact:
        return "over"
    if "under" in compact:
        return "under"
    return compact


def _parse_odd_button_onclick_payload(onclick: str) -> dict[str, Any]:
    if not onclick:
        return {}
    payload_match = re.search(r"updateCoupon\('([^']+)'", onclick)
    if payload_match is None:
        return {}

    encoded_payload = unquote(payload_match.group(1))
    decoded_payload = _safe_base64_decode(encoded_payload) or encoded_payload
    serialized_match = re.match(r'^s:\d+:"(.*)";\s*$', decoded_payload, flags=re.DOTALL)
    payload_body = serialized_match.group(1) if serialized_match is not None else decoded_payload
    if not payload_body:
        return {}

    parsed: dict[str, Any] = {}
    for part in payload_body.split("^|^"):
        if "=" not in part:
            continue
        key, raw_value = part.split("=", 1)
        key_name = normalize(key).replace(" ", "")
        value = (raw_value or "").strip()

        if key_name == "marketname":
            parsed["market_name"] = _canonical_market_name(value)
        elif key_name == "pick":
            parsed["pick"] = _canonical_pick_name(value)
        elif key_name == "line":
            parsed["line_raw"] = value
            line_value = _extract_float(value)
            if line_value is not None:
                parsed["line"] = line_value
        elif key_name == "extra":
            parsed["extra"] = normalize(value)

    if "market_name" not in parsed and parsed.get("extra"):
        parsed["market_name"] = _canonical_market_name(str(parsed["extra"]))
    return parsed


def _line_diff_for_market(
    market: str,
    candidate_line: Optional[float],
    intent_line: Optional[float],
) -> Optional[float]:
    if candidate_line is None or intent_line is None:
        return None
    if market == "handicap":
        # Для форы сравниваем по модулю линии (Ф2(+1) и A -1 / H +1 в таблице).
        return abs(abs(candidate_line) - abs(intent_line))
    return abs(candidate_line - intent_line)


def _contains_total_marker(value: str) -> bool:
    lowered = normalize(value or "").lower()
    if not lowered:
        return False
    return "over" in lowered or "under" in lowered


def _contains_handicap_marker(value: str) -> bool:
    raw = (value or "").lower()
    if not raw:
        return False
    # Приводим возможные unicode-минусы к обычному '-', чтобы корректно ловить signed line.
    raw = raw.replace("−", "-").replace("–", "-").replace("—", "-")
    lowered = normalize(raw)
    if not lowered:
        return False
    if re.search(r"\b(?:ah|hcp|hdp|handicap|spread|spreads)\b", lowered):
        return True
    # Для Asian handicap важен знак линии (+0.75 / -0.5). Берем только случаи без пробела после знака,
    # чтобы не принимать live score "1 - 0" за handicap.
    return bool(re.search(r"(?<!\d)[+\-]\d+(?:[.,]\d+)?", raw))


def _normalize_score(value: str) -> str:
    match = re.search(r"(\d{1,2})\s*[:\-]\s*(\d{1,2})", value or "")
    if match is None:
        return ""
    return f"{match.group(1)}:{match.group(2)}"


def _coupon_reset_alert_present(error_text: str) -> bool:
    lowered = normalize(error_text or "").lower()
    return (
        "combination with live bets is not allowed" in lowered
        or "only pinnacle combo odds can be used for pinnacle parlay bet" in lowered
    )


def is_recoverable_submit_error(reason: str) -> bool:
    lowered = normalize(reason or "").lower()
    if not lowered:
        return False
    return any(marker in lowered for marker in _RECOVERABLE_SUBMIT_MARKERS)


def resolve_forced_league_alias(
    tournament: str,
    aliases: Optional[dict[str, str]],
    available_titles: list[str],
) -> dict[str, Any]:
    normalized_tournament = normalize(tournament)
    alias_source = ""
    alias_target = ""

    for source, target in (aliases or {}).items():
        source_norm = normalize(source)
        target_norm = normalize(target)
        if not source_norm or not target_norm:
            continue
        if source_norm == normalized_tournament:
            alias_source = source
            alias_target = target
            break

    if not alias_target:
        return {
            "has_alias": False,
            "found": False,
            "alias_source": "",
            "alias_target": "",
            "matched_title": "",
            "forced_alias_not_found": False,
        }

    normalized_target = normalize(alias_target)
    matched_title = ""
    for title in available_titles:
        if normalize(title) == normalized_target:
            matched_title = title
            break

    found = bool(matched_title)
    return {
        "has_alias": True,
        "found": found,
        "alias_source": alias_source,
        "alias_target": alias_target,
        "matched_title": matched_title,
        "forced_alias_not_found": not found,
    }


def resolve_period_tab_request(intent: BetIntent) -> dict[str, Any]:
    key = "full_event"
    if intent.market == "team_total":
        key = "team_total"
    elif intent.period == "1h":
        key = "first_half"
    elif intent.period == "2h":
        key = "second_half"
    elif intent.period == "q3":
        key = "third_quarter"
    elif intent.period == "q4":
        key = "fourth_quarter"

    synonyms = _PERIOD_TAB_SYNONYMS[key]
    return {
        "key": key,
        "primary": synonyms[0],
        "synonyms": list(synonyms),
    }


def tab_text_matches_synonyms(tab_text: str, synonyms: list[str]) -> bool:
    normalized_tab = normalize(tab_text)
    if not normalized_tab:
        return False
    compact_tab = normalized_tab.replace(" ", "")

    for item in synonyms:
        normalized_item = normalize(item)
        if not normalized_item:
            continue
        compact_item = normalized_item.replace(" ", "")
        if normalized_tab == normalized_item:
            return True
        if compact_tab == compact_item:
            return True
        if normalized_item in normalized_tab:
            return True
    return False


def build_league_selection_plan(
    league_entries: list[dict[str, Any]],
    tournament: str,
    metric: str,
    *,
    league_aliases: Optional[dict[str, str]] = None,
    fallback_top_n: int = _DEFAULT_LEAGUE_FALLBACK_CANDIDATES,
) -> dict[str, Any]:
    metric_hint = detect_metric_hint_from_tournament(tournament)
    effective_metric = metric_hint or metric

    available_titles = [str(item.get("title", "")) for item in league_entries if item.get("title")]
    alias_info = resolve_forced_league_alias(tournament, league_aliases, available_titles)

    scored: list[dict[str, Any]] = []
    for entry in league_entries:
        index = int(entry.get("index", -1))
        title_text = normalize(str(entry.get("title", "")))
        if index < 0 or not title_text:
            continue

        passes_metric = league_passes_metric(title_text, effective_metric)
        if normalize(effective_metric) in {"corners", "bookings"} and not passes_metric:
            continue

        score = score_league_candidate(title_text, tournament, effective_metric)
        scored.append(
            {
                "index": index,
                "title": title_text,
                "score": round(score, 4),
                "metric_ok": passes_metric,
                "method": "fuzzy",
            }
        )

    scored.sort(key=lambda item: item["score"], reverse=True)
    ordered: list[dict[str, Any]] = []

    if bool(alias_info.get("found")):
        matched_title = normalize(str(alias_info.get("matched_title", "")))
        forced_entry = next(
            (
                entry
                for entry in league_entries
                if normalize(str(entry.get("title", ""))) == matched_title
            ),
            None,
        )
        if forced_entry is not None:
            ordered.append(
                {
                    "index": int(forced_entry.get("index", -1)),
                    "title": normalize(str(forced_entry.get("title", ""))),
                    "score": 1.0,
                    "metric_ok": True,
                    "method": "forced_alias",
                    "alias_source": alias_info.get("alias_source", ""),
                    "alias_target": alias_info.get("alias_target", ""),
                }
            )

    limit = max(1, int(fallback_top_n))
    for candidate in scored:
        if any(existing.get("title") == candidate.get("title") for existing in ordered):
            continue
        ordered.append(candidate)
        if len(ordered) >= limit:
            break

    if not ordered:
        diagnostics = {
            "metric": metric,
            "metric_hint": metric_hint,
            "effective_metric": effective_metric,
            "tournament": tournament,
            "forced_alias": alias_info,
        }
        if not scored:
            raise BlogabetPublishError(
                "find_league",
                "Не найдено лиг, соответствующих типу ставки",
                diagnostics=diagnostics,
            )
        raise BlogabetPublishError(
            "find_league",
            "Не удалось сформировать список кандидатов лиги",
            diagnostics=diagnostics,
        )

    return {
        "best": ordered[0],
        "ordered_candidates": ordered,
        "top_candidates": scored[:10],
        "metric_hint": metric_hint,
        "effective_metric": effective_metric,
        "forced_alias": alias_info,
    }


async def _read_league_title(link: Any) -> str:
    title_node = link.locator(SELECTORS.LEAGUE_TITLE)
    if await title_node.count():
        return normalize(await title_node.inner_text())
    return normalize(await link.inner_text())


async def _collect_league_entries(
    page: AsyncPage,
    *,
    league_links_selector: Optional[str] = None,
    sport_title: str = "Football",
) -> list[dict[str, Any]]:
    selector = league_links_selector or _league_links_selector_for_sport(_SPORT_FOOTBALL)
    league_links = page.locator(selector)
    total = await league_links.count()
    if total == 0:
        raise BlogabetPublishError("find_league", f"Список лиг {sport_title} пуст")

    entries: list[dict[str, Any]] = []
    for index in range(total):
        link = league_links.nth(index)
        try:
            title_text = await _read_league_title(link)
        except Exception:  # noqa: BLE001
            title_text = ""
        if not title_text:
            continue
        entries.append({"index": index, "title": title_text})
    return entries


def _coupon_matches_intent(card_raw_text: str, intent: BetIntent) -> tuple[bool, dict[str, Any]]:
    raw = re.sub(r"\s+", " ", (card_raw_text or "").strip().lower())
    normalized = normalize(card_raw_text or "")
    diag: dict[str, Any] = {
        "coupon_raw_text": raw,
        "coupon_norm_text": normalized,
    }

    numbers = _extract_numeric_tokens(raw)
    diag["coupon_numbers"] = numbers[:20]

    def has_exact_line(line: Optional[float]) -> bool:
        if line is None:
            return True
        return any(abs(value - line) <= 0.01 for value in numbers)

    if intent.market == "total":
        side_ok = intent.side in {"over", "under"} and intent.side in raw
        line_ok = has_exact_line(intent.line)
        diag["intent_side_ok"] = side_ok
        diag["intent_line_ok"] = line_ok
        return side_ok and line_ok, diag

    if intent.market == "team_total":
        side_ok = intent.side in {"over", "under"} and intent.side in raw
        scope_word = "home" if intent.scope == "home" else "away"
        scope_ok = scope_word in raw or (scope_word == "home" and " h " in f" {normalized} ") or (
            scope_word == "away" and " a " in f" {normalized} "
        )
        line_ok = has_exact_line(intent.line)
        diag["intent_side_ok"] = side_ok
        diag["intent_scope_ok"] = scope_ok
        diag["intent_line_ok"] = line_ok
        return side_ok and scope_ok and line_ok, diag

    if intent.market == "handicap":
        scope_word = "home" if intent.scope == "home" else "away"
        scope_ok = scope_word in raw or (scope_word == "home" and " h " in f" {normalized} ") or (
            scope_word == "away" and " a " in f" {normalized} "
        )
        line_ok = has_exact_line(intent.line)
        diag["intent_scope_ok"] = scope_ok
        diag["intent_line_ok"] = line_ok
        return scope_ok and line_ok, diag

    if intent.market == "moneyline":
        has_total_marker = _contains_total_marker(raw)
        has_handicap_marker = _contains_handicap_marker(raw)
        diag["intent_has_total_marker"] = has_total_marker
        diag["intent_has_handicap_marker"] = has_handicap_marker

        if intent.side == "home":
            side_ok = "home" in raw or " h " in f" {normalized} "
            diag["intent_side_ok"] = side_ok
            return side_ok and not has_total_marker and not has_handicap_marker, diag
        if intent.side == "away":
            side_ok = "away" in raw or " a " in f" {normalized} "
            diag["intent_side_ok"] = side_ok
            return side_ok and not has_total_marker and not has_handicap_marker, diag
        if intent.side == "draw":
            side_ok = "draw" in raw or " d " in f" {normalized} "
            diag["intent_side_ok"] = side_ok
            return side_ok and not has_total_marker and not has_handicap_marker, diag
        return False, diag

    return False, diag


async def select_league_by_tournament(
    page: AsyncPage,
    tournament: str,
    metric: str,
    *,
    league_links_selector: Optional[str] = None,
    sport_title: str = "Football",
    league_aliases: Optional[dict[str, str]] = None,
    fallback_top_n: int = _DEFAULT_LEAGUE_FALLBACK_CANDIDATES,
) -> dict[str, Any]:
    selector = league_links_selector or _league_links_selector_for_sport(_SPORT_FOOTBALL)
    entries = await _collect_league_entries(page, league_links_selector=selector, sport_title=sport_title)
    plan = build_league_selection_plan(
        entries,
        tournament,
        metric,
        league_aliases=league_aliases,
        fallback_top_n=fallback_top_n,
    )

    league_links = page.locator(selector)
    best = plan["best"]
    best_link = league_links.nth(int(best["index"]))
    await best_link.scroll_into_view_if_needed()
    await best_link.click()

    return {
        "best": best,
        "ordered_candidates": plan.get("ordered_candidates", []),
        "top_candidates": plan.get("top_candidates", []),
        "metric_hint": plan.get("metric_hint"),
        "effective_metric": plan.get("effective_metric"),
        "forced_alias": plan.get("forced_alias", {}),
    }


class BlogabetPublisher:
    def __init__(
        self,
        blogabet_cfg: BlogabetConfig,
        playwright_browser_factory: Optional[
            Callable[[AsyncPlaywright, bool], Awaitable[AsyncBrowser] | AsyncBrowser]
        ] = None,
        logger: Any = None,
    ) -> None:
        self.cfg = blogabet_cfg
        self.logger = logger
        self._browser_factory = playwright_browser_factory
        self._lock = asyncio.Lock()
        self._playwright: Optional[AsyncPlaywright] = None
        self._browser: Optional[AsyncBrowser] = None
        self._context: Optional[AsyncBrowserContext] = None
        self._debug_dir = Path(__file__).resolve().parent / "debug" / "blogabet"
        self._mismatch_log_path = Path(__file__).resolve().parent / "blogabet_mismatches.jsonl"
        self._league_aliases = self._load_league_aliases()

    def _log(self, level: str, message: str, *args: Any) -> None:
        logger = self.logger
        if logger is None:
            return
        handler = getattr(logger, level, None)
        if not callable(handler):
            handler = getattr(logger, "info", None)
        if not callable(handler):
            return
        try:
            handler(message, *args)
        except Exception:  # noqa: BLE001
            return

    def _resolve_cfg_path(self, raw_path: str) -> Path:
        path = Path(raw_path)
        if not path.is_absolute():
            path = Path(__file__).resolve().parent / path
        return path

    def _storage_state_path(self) -> Path:
        return self._resolve_cfg_path(self.cfg.storage_state_path)

    def _league_aliases_path(self) -> Path:
        return self._resolve_cfg_path(self.cfg.league_aliases_path)

    def _load_league_aliases(self) -> dict[str, str]:
        path = self._league_aliases_path()
        if not path.exists():
            self._log("info", "Blogabet aliases: файл не найден, продолжаем без алиасов (%s)", path)
            return {}

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            self._log("warning", "Blogabet aliases: ошибка парсинга %s: %s", path, exc)
            return {}

        aliases_payload = payload.get("aliases") if isinstance(payload, dict) else None
        if not isinstance(aliases_payload, dict):
            self._log("warning", "Blogabet aliases: ключ aliases отсутствует или имеет неверный формат (%s)", path)
            return {}

        aliases: dict[str, str] = {}
        for source, target in aliases_payload.items():
            source_text = normalize(str(source))
            target_text = normalize(str(target))
            if not source_text or not target_text:
                continue
            aliases[source_text] = target_text

        self._log("info", "Blogabet aliases: загружено %s записей из %s", len(aliases), path)
        return aliases

    def _append_mismatch_diagnostics(
        self,
        *,
        match: Any,
        bet_intent: BetIntent,
        diagnostics: dict[str, Any],
        failure_step: str,
        failure_reason: str,
    ) -> None:
        selected_league_best = diagnostics.get("selected_league")
        if not selected_league_best:
            league_candidates = diagnostics.get("league_candidates", [])
            if isinstance(league_candidates, list) and league_candidates:
                selected_league_best = league_candidates[0]

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tournament": normalize(str(getattr(match, "tournament", ""))),
            "metric": normalize(bet_intent.metric),
            "selected_league_best": selected_league_best,
            "top_league_candidates": diagnostics.get("league_candidates", [])[:10],
            "home_team": normalize(str(getattr(match, "home_team", ""))),
            "away_team": normalize(str(getattr(match, "away_team", ""))),
            "failure_step": failure_step,
            "failure_reason": normalize(failure_reason),
        }

        try:
            self._mismatch_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._mismatch_log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as exc:  # noqa: BLE001
            self._log("warning", "Не удалось записать blogabet mismatch diagnostics: %s", exc)

    @staticmethod
    def _base_origin(url: str) -> str:
        parsed = urlsplit((url or "").strip())
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return "https://blogabet.com"

    def _effective_headless(self, requested_headless: bool) -> bool:
        if requested_headless:
            return True
        if os.getenv("DISPLAY") or os.getenv("WAYLAND_DISPLAY"):
            return False
        self._log(
            "warning",
            "Графическая сессия не обнаружена (DISPLAY/WAYLAND_DISPLAY), запускаю браузер в headless-режиме.",
        )
        return True

    async def _launch_browser(self, playwright: AsyncPlaywright, *, headless: bool) -> AsyncBrowser:
        effective_headless = self._effective_headless(headless)
        if self._browser_factory is None:
            return await playwright.chromium.launch(headless=effective_headless)

        maybe_browser = self._browser_factory(playwright, effective_headless)
        if inspect.isawaitable(maybe_browser):
            return await maybe_browser
        return maybe_browser

    async def _open_login_popup(self, page: AsyncPage) -> None:
        async def _login_form_exists(*, wait_timeout: int = 0) -> bool:
            form = page.locator(SELECTORS.LOGIN_FORM).first
            try:
                if wait_timeout > 0:
                    await form.wait_for(state="attached", timeout=wait_timeout)
                if await form.count() == 0:
                    return False
                return True
            except Exception:  # noqa: BLE001
                return False

        async def _login_controls_exist(*, wait_timeout: int = 0) -> bool:
            if await _login_form_exists(wait_timeout=wait_timeout):
                return True
            email_node = await _first_visible(page.locator(SELECTORS.LOGIN_EMAIL_INPUT))
            password_node = await _first_visible(page.locator(SELECTORS.LOGIN_PASSWORD_INPUT))
            submit_node = await _first_visible(page.locator(SELECTORS.LOGIN_SUBMIT_BUTTON))
            if email_node and password_node and submit_node:
                return True
            email_node_present = await _first_present(page.locator(SELECTORS.LOGIN_EMAIL_INPUT))
            password_node_present = await _first_present(page.locator(SELECTORS.LOGIN_PASSWORD_INPUT))
            submit_node_present = await _first_present(page.locator(SELECTORS.LOGIN_SUBMIT_BUTTON))
            return bool(email_node_present and password_node_present and submit_node_present)

        # На части страниц форма логина уже отрисована без popup.
        if await _login_controls_exist(wait_timeout=1200):
            return

        login_trigger = page.locator(SELECTORS.LOGIN_TRIGGER)
        if await login_trigger.count() > 0:
            clicked = False
            for index in range(min(await login_trigger.count(), 5)):
                node = login_trigger.nth(index)
                try:
                    if await node.is_visible():
                        await node.click(timeout=5000)
                        clicked = True
                        break
                except Exception:  # noqa: BLE001
                    continue
            if clicked:
                await page.wait_for_timeout(500)

        # Доп. попытка принудительно открыть login-модалку через hash/Bootstrap.
        try:
            await page.evaluate(
                """
() => {
  try { window.location.hash = "login"; } catch (_err) {}
  const modal = document.querySelector("#systemModal");
  if (!modal) return;
  try {
    if (window.jQuery && typeof window.jQuery(modal).modal === "function") {
      window.jQuery(modal).modal("show");
      return;
    }
  } catch (_err) {}
  try {
    modal.classList.add("in", "show");
    modal.style.display = "block";
    modal.removeAttribute("aria-hidden");
  } catch (_err) {}
}
""",
            )
            await page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            pass

        login_modal = page.locator(SELECTORS.LOGIN_MODAL)
        if await login_modal.count() > 0:
            try:
                await login_modal.first.wait_for(state="visible", timeout=8000)
            except Exception:  # noqa: BLE001
                pass

        if await _login_controls_exist(wait_timeout=3000):
            return

        # Fallback для headless/серверных сценариев: открываем прямую страницу логина.
        base_origin = self._base_origin(self.cfg.login_url) or self._base_origin(self.cfg.upcoming_url)
        fallback_urls = [
            (self.cfg.login_url or "").strip(),
            f"{base_origin}/login",
            f"{base_origin}/signin",
            f"{base_origin}/#login",
            base_origin,
        ]
        seen: set[str] = set()
        for raw_url in fallback_urls:
            target_url = (raw_url or "").strip()
            if not target_url or target_url in seen:
                continue
            seen.add(target_url)
            try:
                await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(700)
                await self._dismiss_age_confirmation(page)
                try:
                    await page.evaluate("() => { try { window.location.hash = 'login'; } catch (_err) {} }")
                except Exception:  # noqa: BLE001
                    pass
                if await _login_controls_exist(wait_timeout=3000):
                    return
            except Exception:  # noqa: BLE001
                continue

        current_url = (page.url or "").strip()
        raise BlogabetAuthRequired(
            f"Login popup не открылся (форма логина не найдена). url={current_url or '-'}"
        )

    async def _is_login_form_visible(self, page: AsyncPage) -> bool:
        try:
            form = page.locator(SELECTORS.LOGIN_FORM).first
            if await form.count() == 0:
                return False
            return await form.is_visible()
        except Exception:  # noqa: BLE001
            return False

    async def _try_auto_login(self, page: AsyncPage) -> bool:
        email = (self.cfg.login_email or "").strip()
        password = self.cfg.login_password or ""
        if not email or not password:
            return False

        forms = page.locator(SELECTORS.LOGIN_FORM)
        total_forms = await forms.count()

        for index in range(min(total_forms, 5)):
            login_form = forms.nth(index)
            try:
                if not await login_form.is_visible():
                    continue
            except Exception:  # noqa: BLE001
                continue

            email_input = login_form.locator(SELECTORS.LOGIN_EMAIL_INPUT).first
            password_input = login_form.locator(SELECTORS.LOGIN_PASSWORD_INPUT).first
            submit_button = login_form.locator(SELECTORS.LOGIN_SUBMIT_BUTTON).first
            if (
                await email_input.count() == 0
                or await password_input.count() == 0
                or await submit_button.count() == 0
            ):
                continue

            await email_input.fill(email)
            await password_input.fill(password)
            await submit_button.click()
            return True

        # Fallback: поля могут быть вне form.
        email_input = await _first_visible(page.locator(SELECTORS.LOGIN_EMAIL_INPUT))
        password_input = await _first_visible(page.locator(SELECTORS.LOGIN_PASSWORD_INPUT))
        submit_button = await _first_visible(page.locator(SELECTORS.LOGIN_SUBMIT_BUTTON))
        if not email_input or not password_input or not submit_button:
            email_input = await _first_present(page.locator(SELECTORS.LOGIN_EMAIL_INPUT))
            password_input = await _first_present(page.locator(SELECTORS.LOGIN_PASSWORD_INPUT))
            submit_button = await _first_present(page.locator(SELECTORS.LOGIN_SUBMIT_BUTTON))
            if not email_input or not password_input or not submit_button:
                return False
        await email_input.fill(email)
        await password_input.fill(password)
        await submit_button.click()
        return True

    async def _is_authenticated_from_current_page(self, page: AsyncPage) -> bool:
        try:
            await page.goto(self.cfg.upcoming_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(700)
        except Exception:  # noqa: BLE001
            return False

        current_url = normalize(page.url).lower()
        if "login" in current_url:
            return False
        return await page.locator("#_pinnacle-data, #sports, #_event").count() > 0

    async def _ensure_browser_context(self) -> AsyncBrowserContext:
        storage_state_path = self._storage_state_path()
        if not storage_state_path.exists():
            raise BlogabetAuthRequired(
                "Storage state Blogabet не найден. Выполните ручной логин в панели управления."
            )

        if self._context is not None:
            return self._context

        if self._playwright is None:
            self._playwright = await async_playwright().start()
        if self._browser is None:
            self._browser = await self._launch_browser(self._playwright, headless=self.cfg.headless)

        self._context = await self._browser.new_context(storage_state=str(storage_state_path))
        return self._context

    async def ensure_session(self) -> None:
        context = await self._ensure_browser_context()
        page = await context.new_page()
        try:
            await page.goto(self.cfg.upcoming_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1000)
            current_url = normalize(page.url).lower()
            if "login" in current_url:
                raise BlogabetAuthRequired(
                    "Сессия Blogabet недействительна. Выполните ручной логин и сохраните storage state."
                )
        finally:
            await page.close()

    async def close(self) -> None:
        if self._context is not None:
            await self._context.close()
            self._context = None
        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

    async def interactive_login_and_save_state(self) -> str:
        storage_state_path = self._storage_state_path()
        storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        headless_login = self._effective_headless(False)
        if headless_login and not ((self.cfg.login_email or "").strip() and (self.cfg.login_password or "")):
            raise BlogabetAuthRequired(
                "На сервере без GUI интерактивный логин недоступен. "
                "Укажите BLOGABET_LOGIN_EMAIL/BLOGABET_LOGIN_PASSWORD "
                "или загрузите готовый storage state."
            )

        manual_playwright = await async_playwright().start()
        browser: Optional[AsyncBrowser] = None
        try:
            browser = await self._launch_browser(manual_playwright, headless=headless_login)
            context = await browser.new_context()
            page = await context.new_page()
            home_url = self._base_origin(self.cfg.login_url) or self._base_origin(self.cfg.upcoming_url)
            await page.goto(home_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(900)
            await self._dismiss_age_confirmation(page)
            await self._open_login_popup(page)
            auto_login_done = await self._try_auto_login(page)

            deadline = time.monotonic() + max(self.cfg.interactive_login_timeout_seconds, 60)
            last_probe_at = 0.0
            while time.monotonic() < deadline:
                await page.wait_for_timeout(1000)
                now = time.monotonic()
                if now - last_probe_at < 4.0:
                    continue
                last_probe_at = now

                if await self._is_login_form_visible(page):
                    continue

                if await self._is_authenticated_from_current_page(page):
                    await context.storage_state(path=str(storage_state_path))
                    await context.close()

                    # Сбрасываем текущий контекст, чтобы при следующей публикации он открылся с новым state.
                    await self.close()
                    return str(storage_state_path)

                # Если логин все еще не выполнен, возвращаемся на главную и снова открываем popup.
                await page.goto(home_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(700)
                await self._dismiss_age_confirmation(page)
                await self._open_login_popup(page)
                if not auto_login_done:
                    auto_login_done = await self._try_auto_login(page)

            raise BlogabetAuthRequired(
                "Ручной логин не подтвержден в отведенное время. Повторите попытку и завершите вход в открытом браузере."
            )
        finally:
            if browser is not None:
                try:
                    await browser.close()
                except Exception:  # noqa: BLE001
                    pass
            await manual_playwright.stop()

    async def _capture_debug_artifacts(self, page: AsyncPage, step_name: str) -> tuple[str, str]:
        timestamp = int(time.time() * 1000)
        safe_step = re.sub(r"[^a-zA-Z0-9_-]", "_", step_name)
        self._debug_dir.mkdir(parents=True, exist_ok=True)

        screenshot_path = self._debug_dir / f"{timestamp}_{safe_step}.png"
        html_dump_path = self._debug_dir / f"{timestamp}_{safe_step}.html"

        try:
            await page.screenshot(path=str(screenshot_path), full_page=True)
        except Exception:  # noqa: BLE001
            screenshot_path = Path("")

        try:
            content = await page.content()
            html_dump_path.write_text(content, encoding="utf-8")
        except Exception:  # noqa: BLE001
            html_dump_path = Path("")

        return str(screenshot_path), str(html_dump_path)

    async def _dismiss_age_confirmation(self, page: AsyncPage) -> None:
        candidate_texts = (
            "I am over 18",
            "I am 18",
            "I agree",
            "Accept",
            "Continue",
            "Enter",
        )
        for text in candidate_texts:
            locator = page.locator(f"button:has-text('{text}'), a:has-text('{text}')")
            if await locator.count() == 0:
                continue
            try:
                await locator.first.click(timeout=1000)
                await page.wait_for_timeout(300)
                return
            except Exception:  # noqa: BLE001
                continue

    async def _select_sport(self, page: AsyncPage, *, sport_target: str, sport_title: str) -> None:
        sport_trigger_selector = SELECTORS.SPORT_TRIGGER_TEMPLATE.format(sport_target=sport_target)
        sport_trigger = page.locator(sport_trigger_selector)
        if await sport_trigger.count() == 0:
            raise BlogabetPublishError("select_sport", f"Не найден триггер {sport_title}")
        await sport_trigger.first.click()
        await page.wait_for_timeout(400)
        sport_list_selector = SELECTORS.SPORT_LIST_TEMPLATE.format(sport_target=sport_target)
        await page.wait_for_selector(sport_list_selector, state="attached", timeout=10000)

    async def _resolve_league_link_for_candidate(
        self,
        page: AsyncPage,
        candidate: dict[str, Any],
        *,
        league_links_selector: str,
    ) -> Any:
        league_links = page.locator(league_links_selector)
        total = await league_links.count()
        if total == 0:
            return None

        candidate_title = normalize(str(candidate.get("title", "")))
        candidate_index = int(candidate.get("index", -1))
        if 0 <= candidate_index < total:
            by_index = league_links.nth(candidate_index)
            if not candidate_title:
                return by_index
            try:
                by_index_title = await _read_league_title(by_index)
                if by_index_title == candidate_title:
                    return by_index
            except Exception:  # noqa: BLE001
                pass

        if candidate_title:
            for index in range(total):
                item = league_links.nth(index)
                try:
                    item_title = await _read_league_title(item)
                except Exception:  # noqa: BLE001
                    continue
                if item_title == candidate_title:
                    candidate["index"] = index
                    return item

        return None

    async def _click_league_candidate(
        self,
        page: AsyncPage,
        candidate: dict[str, Any],
        *,
        league_links_selector: str,
        sport_title: str,
    ) -> None:
        league_link = await self._resolve_league_link_for_candidate(
            page,
            candidate,
            league_links_selector=league_links_selector,
        )
        if league_link is None:
            available: list[str] = []
            try:
                entries = await _collect_league_entries(
                    page,
                    league_links_selector=league_links_selector,
                    sport_title=sport_title,
                )
                available = [str(entry.get("title", "")) for entry in entries[:20]]
            except Exception:  # noqa: BLE001
                available = []
            raise BlogabetPublishError(
                "find_league",
                f"Не удалось выбрать лигу: {candidate.get('title') or '-'}",
                diagnostics={"candidate": candidate, "available_leagues_sample": available},
            )

        try:
            await league_link.scroll_into_view_if_needed()
            await league_link.click()
            await page.wait_for_timeout(450)
        except Exception as exc:  # noqa: BLE001
            raise BlogabetPublishError(
                "find_league",
                f"Не удалось кликнуть по лиге: {candidate.get('title') or '-'}",
                diagnostics={"candidate": candidate, "click_error": normalize(str(exc))[:220]},
            ) from exc

    async def _wait_for_events_after_league_selection(self, page: AsyncPage) -> None:
        try:
            await page.wait_for_selector(SELECTORS.EVENT_CONTAINER, state="attached", timeout=30000)
        except Exception as exc:  # noqa: BLE001
            raise BlogabetPublishError(
                "find_league",
                "Не удалось дождаться загрузки событий после выбора лиги",
                diagnostics={"wait_error": normalize(str(exc))[:220]},
            ) from exc
        await page.wait_for_timeout(700)

    async def _reclick_league_candidate(
        self,
        page: AsyncPage,
        candidate: dict[str, Any],
        *,
        league_links_selector: str,
        sport_title: str,
    ) -> bool:
        try:
            await self._click_league_candidate(
                page,
                candidate,
                league_links_selector=league_links_selector,
                sport_title=sport_title,
            )
            await self._wait_for_events_after_league_selection(page)
            return True
        except BlogabetPublishError:
            return False

    async def _switch_period_tab(
        self,
        page: AsyncPage,
        intent: BetIntent,
        *,
        selected_league_candidate: Optional[dict[str, Any]] = None,
        league_links_selector: str,
        sport_title: str,
    ) -> dict[str, Any]:
        tab_request = resolve_period_tab_request(intent)
        tab_target_text = str(tab_request["primary"])
        tab_synonyms = [str(item) for item in tab_request["synonyms"]]

        diagnostics: dict[str, Any] = {
            "tab_target": tab_target_text,
            "tab_synonyms": tab_synonyms,
            "attempts": [],
        }
        tabs = page.locator(SELECTORS.EVENT_TAB_LINKS)
        reclick_done = False

        for attempt_no in range(1, 4):
            attempt_diag: dict[str, Any] = {"attempt": attempt_no}
            wait_timeout = 2500 + (attempt_no - 1) * 2500
            try:
                await page.wait_for_selector(SELECTORS.EVENT_TAB_LINKS, state="attached", timeout=wait_timeout)
                attempt_diag["tabs_wait"] = "ok"
            except Exception as exc:  # noqa: BLE001
                attempt_diag["tabs_wait"] = "timeout"
                attempt_diag["wait_error"] = normalize(str(exc))[:220]
                if not reclick_done and selected_league_candidate is not None:
                    reclick_done = await self._reclick_league_candidate(
                        page,
                        selected_league_candidate,
                        league_links_selector=league_links_selector,
                        sport_title=sport_title,
                    )
                    attempt_diag["league_reclick"] = reclick_done
                diagnostics["attempts"].append(attempt_diag)
                await page.wait_for_timeout(600 + attempt_no * 250)
                continue

            total_tabs = await tabs.count()
            attempt_diag["tab_count"] = total_tabs
            tab_titles: list[str] = []
            for index in range(total_tabs):
                tab = tabs.nth(index)
                tab_text = normalize(await tab.inner_text())
                if not tab_text:
                    continue
                tab_titles.append(tab_text)
                if not tab_text_matches_synonyms(tab_text, tab_synonyms):
                    continue
                await tab.scroll_into_view_if_needed()
                await tab.click()
                await page.wait_for_timeout(450)
                attempt_diag["result"] = "selected"
                attempt_diag["selected_tab"] = tab_text
                attempt_diag["available_tabs"] = tab_titles[:15]
                diagnostics["attempts"].append(attempt_diag)
                diagnostics["selected_tab"] = tab_text
                diagnostics["reclicked_league"] = reclick_done
                return diagnostics

            attempt_diag["result"] = "tab_not_found"
            attempt_diag["available_tabs"] = tab_titles[:15]
            if not reclick_done and selected_league_candidate is not None:
                reclick_done = await self._reclick_league_candidate(
                    page,
                    selected_league_candidate,
                    league_links_selector=league_links_selector,
                    sport_title=sport_title,
                )
                attempt_diag["league_reclick"] = reclick_done
            diagnostics["attempts"].append(attempt_diag)
            await page.wait_for_timeout(600 + attempt_no * 250)

        diagnostics["reclicked_league"] = reclick_done
        total_tabs = await tabs.count()
        if total_tabs == 0:
            raise BlogabetPublishError(
                "switch_period_tab",
                "Вкладки рынков не найдены",
                diagnostics=diagnostics,
            )

        raise BlogabetPublishError(
            "switch_period_tab",
            f"Не найдена вкладка периода: {tab_target_text}",
            diagnostics=diagnostics,
        )

    async def _find_match_with_league_fallback(
        self,
        page: AsyncPage,
        match: Any,
        bet_intent: BetIntent,
        diagnostics: dict[str, Any],
        *,
        league_links_selector: str,
        sport_title: str,
    ) -> tuple[int, dict[str, Any], dict[str, Any]]:
        entries = await _collect_league_entries(
            page,
            league_links_selector=league_links_selector,
            sport_title=sport_title,
        )
        league_plan = build_league_selection_plan(
            entries,
            match.tournament,
            bet_intent.metric,
            league_aliases=self._league_aliases,
            fallback_top_n=_DEFAULT_LEAGUE_FALLBACK_CANDIDATES,
        )
        diagnostics["league_candidates"] = league_plan.get("top_candidates", [])

        forced_alias = league_plan.get("forced_alias", {})
        if bool(forced_alias.get("has_alias")):
            diagnostics["forced_alias"] = forced_alias
        if bool(forced_alias.get("forced_alias_not_found")):
            diagnostics["forced_alias_not_found"] = True

        league_attempts: list[dict[str, Any]] = []
        ordered_candidates = [
            dict(item) for item in league_plan.get("ordered_candidates", []) if isinstance(item, dict)
        ]
        last_error: Optional[BlogabetPublishError] = None

        for candidate in ordered_candidates:
            attempt_diag = {
                "title": candidate.get("title"),
                "score": candidate.get("score"),
                "method": candidate.get("method", "fuzzy"),
                "result": "started",
                "fail_reason": "",
            }
            try:
                await self._click_league_candidate(
                    page,
                    candidate,
                    league_links_selector=league_links_selector,
                    sport_title=sport_title,
                )
                await self._wait_for_events_after_league_selection(page)
                tab_diag = await self._switch_period_tab(
                    page,
                    bet_intent,
                    selected_league_candidate=candidate,
                    league_links_selector=league_links_selector,
                    sport_title=sport_title,
                )
                attempt_diag["period_tab"] = tab_diag
                best_block_index, match_result = await self._find_event_block(
                    page,
                    match.home_team,
                    match.away_team,
                )
                attempt_diag["result"] = "match_found"
                attempt_diag["selected_match"] = match_result.get("best")
                league_attempts.append(attempt_diag)

                diagnostics["league_attempts"] = league_attempts
                diagnostics["selected_league"] = candidate
                diagnostics["match_candidates"] = match_result.get("top_candidates", [])
                diagnostics["selected_match"] = match_result.get("best")
                return best_block_index, match_result, candidate
            except BlogabetPublishError as exc:
                last_error = exc
                attempt_diag["result"] = "failed"
                attempt_diag["fail_step"] = exc.step_name
                attempt_diag["fail_reason"] = exc.reason
                if exc.diagnostics:
                    attempt_diag["fail_diagnostics"] = exc.diagnostics
                league_attempts.append(attempt_diag)
                await page.wait_for_timeout(350)
                continue

        diagnostics["league_attempts"] = league_attempts
        if last_error is not None:
            merged_diag = dict(diagnostics)
            merged_diag.update(last_error.diagnostics)
            raise BlogabetPublishError(
                "find_match",
                "Не удалось найти матч после перебора кандидатов лиги",
                diagnostics=merged_diag,
            ) from last_error

        raise BlogabetPublishError(
            "find_match",
            "Не удалось найти матч: список кандидатов лиги пуст",
            diagnostics=diagnostics,
        )

    async def _find_event_block(self, page: AsyncPage, home_team: str, away_team: str) -> tuple[int, dict[str, Any]]:
        active_tab = page.locator(SELECTORS.ACTIVE_TAB).first
        blocks = active_tab.locator(SELECTORS.BLOCK_CONTENT)
        block_count = await blocks.count()
        if block_count == 0:
            raise BlogabetPublishError("find_match", "В активной вкладке нет матчей")

        scores: list[dict[str, Any]] = []
        home_target = _clean_team_label(home_team)
        away_target = _clean_team_label(away_team)

        for index in range(block_count):
            block = blocks.nth(index)
            home_locator = block.locator(SELECTORS.HOME_TEAM)
            away_locator = block.locator(SELECTORS.AWAY_TEAM)
            if await home_locator.count() == 0 or await away_locator.count() == 0:
                continue

            home_value = _clean_team_label(await home_locator.inner_text())
            away_value = _clean_team_label(await away_locator.inner_text())

            direct_home = similarity(home_target, home_value)
            direct_away = similarity(away_target, away_value)
            direct = (direct_home + direct_away) / 2
            direct_weak_leg = min(direct_home, direct_away)

            swapped_home = similarity(home_target, away_value)
            swapped_away = similarity(away_target, home_value)
            swapped = (swapped_home + swapped_away) / 2 - 0.08
            swapped_weak_leg = min(swapped_home, swapped_away)

            score = max(direct, swapped)
            swapped_mode = swapped > direct
            weak_leg = swapped_weak_leg if swapped_mode else direct_weak_leg

            scores.append(
                {
                    "index": index,
                    "home": home_value,
                    "away": away_value,
                    "score": round(score, 4),
                    "weak_leg": round(weak_leg, 4),
                    "swapped": swapped_mode,
                }
            )

        if not scores:
            raise BlogabetPublishError("find_match", "Не удалось прочитать карточки матчей")

        scores.sort(key=lambda item: item["score"], reverse=True)
        best = scores[0]
        # Считаем матчинг неуспешным, если хотя бы одна команда сопоставлена слишком слабо.
        if float(best["score"]) < 0.72 or float(best.get("weak_leg", 0.0)) < 0.58:
            raise BlogabetPublishError(
                "find_match",
                "Не найден подходящий матч по командам",
                diagnostics={"match_candidates": scores[:10]},
            )

        return int(best["index"]), {"best": best, "top_candidates": scores[:10]}

    async def _pick_odd_button(self, block: Any, intent: BetIntent) -> dict[str, Any]:
        buttons = block.locator(SELECTORS.ODD_BUTTON)
        count = await buttons.count()
        if count == 0:
            raise BlogabetPublishError("find_market", "В матче нет доступных коэффициентов")

        candidates: list[dict[str, Any]] = []

        for index in range(count):
            button = buttons.nth(index)
            raw_text = await button.inner_text()
            text = normalize(raw_text)
            badge_node = button.locator(SELECTORS.ODD_BADGE)
            badge = ""
            if await badge_node.count() > 0:
                badge = _extract_badge(await badge_node.first.inner_text())
            onclick_attr = await button.get_attribute("onclick")
            onclick_meta = _parse_odd_button_onclick_payload(onclick_attr or "")
            market_name = str(onclick_meta.get("market_name", ""))
            pick_name = str(onclick_meta.get("pick", ""))
            line_from_payload = (
                float(onclick_meta["line"]) if isinstance(onclick_meta.get("line"), (int, float)) else None
            )
            candidate_scope = ""
            if pick_name in {"home", "away"}:
                candidate_scope = pick_name
            elif badge in {"h", "a"}:
                candidate_scope = "home" if badge == "h" else "away"
            scope_match = bool(candidate_scope and candidate_scope == intent.scope)

            line = _extract_float(text)
            effective_line = line_from_payload if line_from_payload is not None else line
            line_diff = _line_diff_for_market(intent.market, effective_line, intent.line)
            text_lower = text.lower()
            has_total_marker = _contains_total_marker(raw_text)
            has_handicap_marker = _contains_handicap_marker(raw_text)
            numeric_tokens = _extract_numeric_tokens(raw_text)

            score = 0.0

            if intent.market == "moneyline":
                expected_badge = {"home": "h", "away": "a", "draw": "d"}.get(intent.side, "")
                score += 1.0 if badge == expected_badge else -1.0
                if market_name:
                    score += 2.6 if market_name == "moneyline" else -2.6
                expected_pick = {"home": "home", "away": "away", "draw": "draw"}.get(intent.side, "")
                if pick_name and expected_pick:
                    score += 1.2 if pick_name == expected_pick else -1.2
                # Для moneyline отсекаем Total/Handicap кнопки.
                if has_total_marker:
                    score -= 1.2
                if has_handicap_marker:
                    score -= 1.8
                # В moneyline обычно одна числовая котировка. Две и более цифры чаще означают line+odds.
                if len(numeric_tokens) >= 2:
                    score -= 0.9

            elif intent.market == "handicap":
                expected_badge = "h" if intent.scope == "home" else "a"
                score += 0.9 if badge == expected_badge else -0.8
                if market_name:
                    score += 1.6 if market_name == "spreads" else -1.6
                if candidate_scope:
                    score += 1.2 if scope_match else -1.0
                if has_total_marker:
                    score -= 1.2
                if has_handicap_marker:
                    score += 0.6

                if line_diff is not None:
                    score += max(0.0, 0.8 - line_diff)
                if intent.side == "plus":
                    score += 0.2 if (effective_line is not None and effective_line >= 0) else -0.2
                elif intent.side == "minus":
                    score += 0.2 if (effective_line is not None and effective_line <= 0) else -0.2

            elif intent.market == "team_total":
                expected_badge = "h" if intent.scope == "home" else "a"
                score += 0.9 if badge == expected_badge else -0.8
                if market_name:
                    score += 1.2 if market_name == "totals" else -1.2
                if has_handicap_marker:
                    score -= 1.2
                if intent.side == "over":
                    score += 0.7 if "over" in text_lower else -0.7
                elif intent.side == "under":
                    score += 0.7 if "under" in text_lower else -0.7
                if effective_line is not None and intent.line is not None:
                    score += max(0.0, 0.8 - abs(effective_line - intent.line))

            else:  # total
                if market_name:
                    score += 1.4 if market_name == "totals" else -1.4
                if has_handicap_marker:
                    score -= 1.2
                if intent.side == "over":
                    score += 0.8 if "over" in text_lower else -0.8
                elif intent.side == "under":
                    score += 0.8 if "under" in text_lower else -0.8
                if effective_line is not None and intent.line is not None:
                    score += max(0.0, 0.9 - abs(effective_line - intent.line))

            candidates.append(
                {
                    "index": index,
                    "text": text,
                    "badge": badge,
                    "market_name": market_name,
                    "pick_name": pick_name,
                    "candidate_scope": candidate_scope,
                    "scope_match": scope_match,
                    "line": effective_line,
                    "line_diff": line_diff,
                    "numbers": numeric_tokens[:5],
                    "has_total_marker": has_total_marker,
                    "has_handicap_marker": has_handicap_marker,
                    "score": round(score, 4),
                }
            )

        candidates.sort(key=lambda item: item["score"], reverse=True)

        best: dict[str, Any] | None = None
        if intent.market in {"total", "team_total", "handicap"} and intent.line is not None:
            if intent.market == "handicap":
                exact_candidates = [
                    item
                    for item in candidates
                    if item.get("line") is not None
                    and item.get("line_diff") is not None
                    and float(item["line_diff"]) <= 0.01
                    and bool(item.get("scope_match", False))
                    and float(item["score"]) > 0.0
                ]
            else:
                exact_candidates = [
                    item
                    for item in candidates
                    if item.get("line") is not None
                    and item.get("line_diff") is not None
                    and float(item["line_diff"]) <= 0.01
                    and float(item["score"]) > 0.0
                ]
            if exact_candidates:
                exact_candidates.sort(key=lambda item: item["score"], reverse=True)
                best = exact_candidates[0]
            else:
                nearest_pool = [item for item in candidates if item.get("line") is not None]
                if intent.market == "handicap":
                    nearest = sorted(
                        nearest_pool,
                        key=lambda item: (
                            0 if bool(item.get("scope_match", False)) else 1,
                            float(item.get("line_diff") or 9999.0),
                        ),
                    )
                else:
                    nearest = sorted(
                        nearest_pool,
                        key=lambda item: float(item.get("line_diff") or 9999.0),
                    )
                raise BlogabetPublishError(
                    "find_market",
                    f"Точная линия не найдена (нужна {intent.line})",
                    diagnostics={
                        "requested_line": intent.line,
                        "market_candidates": candidates[:20],
                        "nearest_lines": nearest[:10],
                    },
                )

        if best is None:
            best = candidates[0]
        if float(best["score"]) < 0.2:
            raise BlogabetPublishError(
                "find_market",
                "Не удалось сопоставить ставку в росписи матча",
                diagnostics={"market_candidates": candidates[:20]},
            )

        best_button = buttons.nth(int(best["index"]))
        await best_button.scroll_into_view_if_needed()
        await best_button.click()

        return {"best": best, "top_candidates": candidates[:20]}

    async def _set_analysis_text(self, page: AsyncPage, analysis_text: str) -> None:
        safe_text = analysis_text or ""
        try:
            await page.evaluate(
                """
(text) => {
  const setTextareaValue = () => {
    const textarea = document.querySelector('#_analyze');
    if (!textarea) {
      return false;
    }
    textarea.value = text;
    textarea.dispatchEvent(new Event('input', { bubbles: true }));
    textarea.dispatchEvent(new Event('change', { bubbles: true }));
    return true;
  };

  // Base fallback: update hidden/native textarea first.
  setTextareaValue();

  try {
    if (!window.CKEDITOR || !CKEDITOR.instances) {
      return;
    }
    const instance = CKEDITOR.instances._analyze || Object.values(CKEDITOR.instances)[0];
    if (!instance || typeof instance.setData !== 'function') {
      return;
    }
    // На некоторых страницах Blogabet CKEditor бросает TypeError в getSelection.
    // Делаем best-effort и не валим публикацию.
    instance.setData(text, {
      callback: () => {
        try {
          if (typeof instance.updateElement === 'function') {
            instance.updateElement();
          } else {
            setTextareaValue();
          }
        } catch (_err) {
          setTextareaValue();
        }
      },
    });
  } catch (_err) {
    setTextareaValue();
  }
}
""",
                safe_text,
            )
        except Exception:  # noqa: BLE001
            # Analysis поле опционально; ошибка редактора не должна ломать публикацию.
            return

    async def _set_live_score_if_required(self, page: AsyncPage, score_value: str) -> bool:
        score = _normalize_score(score_value)
        if not score:
            return False
        inputs = page.locator(SELECTORS.COUPON_LIVE_SCORE_INPUT)
        count = await inputs.count()
        if count == 0:
            return False
        filled = False
        for index in range(count):
            node = inputs.nth(index)
            try:
                if not await node.is_visible():
                    continue
                await node.fill(score)
                await node.dispatch_event("input")
                await node.dispatch_event("change")
                filled = True
            except Exception:  # noqa: BLE001
                continue
        return filled

    async def _live_score_state(self, page: AsyncPage) -> dict[str, Any]:
        return await page.evaluate(
            """
() => {
  const nodes = Array.from(
    document.querySelectorAll("#_couponUpdate div[id^='couponBox_'] input[name^='score_']")
  );
  const visible = nodes.filter((node) => node.offsetParent !== null);
  const values = visible.map((node) => String(node.value || '').trim());
  const filled = values.filter((value) => /^\\d{1,2}:\\d{1,2}$/.test(value)).length;
  return {
    visible_count: visible.length,
    values,
    filled_count: filled,
    all_filled: visible.length === 0 ? true : filled === visible.length,
  };
}
""",
        )

    async def _collect_coupon_error_text(self, page: AsyncPage) -> str:
        error_locator = page.locator(SELECTORS.COUPON_ERROR)
        if await error_locator.count() == 0:
            return ""
        try:
            chunks: list[str] = []
            limit = min(await error_locator.count(), 5)
            for index in range(limit):
                txt = normalize(await error_locator.nth(index).inner_text())
                if txt:
                    chunks.append(txt)
            return " | ".join(chunks)
        except Exception:  # noqa: BLE001
            return ""

    async def _coupon_state_snapshot(self, page: AsyncPage) -> dict[str, Any]:
        snapshot: dict[str, Any] = {
            "url": normalize(page.url),
            "coupon_error_text": "",
            "card_count": 0,
            "badge_text": "",
            "remove_button_count": 0,
            "remove_button_visible_count": 0,
            "first_card_title": "",
            "first_card_text": "",
            "first_card_has_live_score_input": False,
            "first_card_live_score_value": "",
            "remove_onclick_tokens": [],
        }

        try:
            snapshot["coupon_error_text"] = await self._collect_coupon_error_text(page)
        except Exception:  # noqa: BLE001
            snapshot["coupon_error_text"] = ""

        cards = page.locator(SELECTORS.COUPON_PICK_CARD)
        try:
            card_count = await cards.count()
        except Exception:  # noqa: BLE001
            card_count = 0
        snapshot["card_count"] = card_count

        badge = page.locator(SELECTORS.COUPON_BADGE).first
        try:
            if await badge.count() > 0:
                snapshot["badge_text"] = normalize(await badge.inner_text())
        except Exception:  # noqa: BLE001
            snapshot["badge_text"] = ""

        remove_buttons = page.locator(SELECTORS.COUPON_REMOVE_BUTTON)
        try:
            remove_count = await remove_buttons.count()
        except Exception:  # noqa: BLE001
            remove_count = 0
        snapshot["remove_button_count"] = remove_count

        visible_remove = 0
        onclick_tokens: list[str] = []
        for index in range(min(remove_count, 6)):
            node = remove_buttons.nth(index)
            try:
                if await node.is_visible():
                    visible_remove += 1
            except Exception:  # noqa: BLE001
                pass
            try:
                onclick_raw = normalize(await node.get_attribute("onclick") or "")
                if onclick_raw:
                    onclick_tokens.append(onclick_raw[:200])
            except Exception:  # noqa: BLE001
                continue
        snapshot["remove_button_visible_count"] = visible_remove
        snapshot["remove_onclick_tokens"] = onclick_tokens

        if card_count > 0:
            first = cards.first
            try:
                title_node = first.locator(SELECTORS.COUPON_PICK_TITLE).first
                if await title_node.count() > 0:
                    snapshot["first_card_title"] = normalize(await title_node.inner_text())
            except Exception:  # noqa: BLE001
                snapshot["first_card_title"] = ""
            try:
                raw = normalize(await first.inner_text())
                snapshot["first_card_text"] = raw[:1000]
            except Exception:  # noqa: BLE001
                snapshot["first_card_text"] = ""
            try:
                score_input = first.locator("input[name^='score_']").first
                has_score_input = await score_input.count() > 0
                snapshot["first_card_has_live_score_input"] = has_score_input
                if has_score_input:
                    snapshot["first_card_live_score_value"] = normalize(
                        await score_input.input_value()
                    )
            except Exception:  # noqa: BLE001
                snapshot["first_card_has_live_score_input"] = False
                snapshot["first_card_live_score_value"] = ""

        return snapshot

    async def _ensure_coupon_ready_before_submit(
        self,
        page: AsyncPage,
        match: Any,
        bet_intent: BetIntent,
        score_candidate: str,
    ) -> tuple[bool, dict[str, Any], str]:
        diagnostics: dict[str, Any] = {}

        need_reselect, coupon_diag = await self._coupon_needs_reselect(
            page,
            match.home_team,
            match.away_team,
            bet_intent,
        )
        diagnostics["coupon_check"] = coupon_diag
        if need_reselect:
            if bool(coupon_diag.get("coupon_has_reset_alert")):
                return False, diagnostics, "coupon_alert_requires_reset"
            return False, diagnostics, "wrong_coupon"

        live_state_before = await self._live_score_state(page)
        diagnostics["live_score_state_before"] = live_state_before

        if int(live_state_before.get("visible_count", 0)) > 0:
            normalized_score = _normalize_score(score_candidate)
            diagnostics["live_score_candidate"] = normalized_score
            if not normalized_score:
                return False, diagnostics, "missing_live_score"

            filled = await self._set_live_score_if_required(page, normalized_score)
            diagnostics["live_score_fill_attempt"] = filled
            live_state_after = await self._live_score_state(page)
            diagnostics["live_score_state_after"] = live_state_after
            if not bool(live_state_after.get("all_filled", False)):
                return False, diagnostics, "live_score_not_filled"

        diagnostics["auto_accept_policy"] = await self._set_auto_accept_policy(page, allow_any=True)
        return True, diagnostics, ""

    async def _set_stake(self, page: AsyncPage, stake: int) -> None:
        stake_value = min(max(int(stake), 1), 10)
        target_value = str(stake_value)
        await page.wait_for_function(
            """
({ value }) => {
  const nodes = Array.from(document.querySelectorAll('#_couponBox select#stake, select#stake'));
  const select = nodes.find((node) => !node.disabled && node.offsetParent !== null) || nodes[0] || null;
  if (!select) return false;
  return Array.from(select.options || []).some((option) => String(option.value) === String(value));
}
""",
            arg={"value": target_value},
            timeout=10000,
        )

        for _ in range(5):
            await page.evaluate(
                """
({ value }) => {
  const nodes = Array.from(document.querySelectorAll('#_couponBox select#stake, select#stake'));
  const select = nodes.find((node) => !node.disabled && node.offsetParent !== null) || nodes[0] || null;
  if (!select) return '';
  select.scrollIntoView({ block: 'center', inline: 'nearest' });
  try { select.focus(); } catch (_err) {}
  try { select.click(); } catch (_err) {}

  const option = Array.from(select.options || []).find((item) => String(item.value) === String(value));
  if (!option) return String(select.value || '');

  select.value = String(value);
  option.selected = true;
  select.dispatchEvent(new Event('input', { bubbles: true }));
  select.dispatchEvent(new Event('change', { bubbles: true }));
  return String(select.value || '');
}
""",
                {"value": target_value},
            )
            await page.wait_for_timeout(250)
            selected_value = await page.evaluate(
                """
() => {
  const nodes = Array.from(document.querySelectorAll('#_couponBox select#stake, select#stake'));
  const select = nodes.find((node) => !node.disabled && node.offsetParent !== null) || nodes[0] || null;
  return select ? String(select.value || '') : '';
}
""",
            )
            if selected_value == target_value:
                return

        final_value = await page.evaluate(
            """
() => {
  const nodes = Array.from(document.querySelectorAll('#_couponBox select#stake, select#stake'));
  const select = nodes.find((node) => !node.disabled && node.offsetParent !== null) || nodes[0] || null;
  return select ? String(select.value || '') : '';
}
""",
        )
        if final_value != target_value:
            raise BlogabetPublishError(
                "fill_coupon",
                f"Не удалось установить stake={target_value}, текущее value={final_value or '-'}",
            )

    async def _set_auto_accept_policy(self, page: AsyncPage, *, allow_any: bool) -> dict[str, Any]:
        result = await page.evaluate(
            """
({ allowAny }) => {
  const getState = (node) => {
    if (!node) return null;
    return {
      checked: !!node.checked,
      disabled: !!node.disabled,
      visible: node.offsetParent !== null,
    };
  };

  const pickNode = (selectors) => {
    const nodes = Array.from(document.querySelectorAll(selectors.join(", ")));
    const active = nodes.find((node) => !node.disabled && node.offsetParent !== null)
      || nodes.find((node) => !node.disabled)
      || nodes[0]
      || null;
    return { active, total: nodes.length };
  };

  const anyPick = pickNode([
    "#_couponBox input#auto_accept_any",
    "#_couponBox input[name='auto_accept_any']",
    "input#auto_accept_any",
    "input[name='auto_accept_any']",
  ]);
  const betterPick = pickNode([
    "#_couponBox input#auto_accept_better",
    "#_couponBox input[name='auto_accept_better']",
    "input#auto_accept_better",
    "input[name='auto_accept_better']",
  ]);
  const anyNode = anyPick.active;
  const betterNode = betterPick.active;
  const before = {
    any: getState(anyNode),
    better: getState(betterNode),
  };

  const setChecked = (node, checked) => {
    if (!node || node.disabled) return false;
    const target = !!checked;
    if (!!node.checked === target) return true;

    try { node.scrollIntoView({ block: "center", inline: "nearest" }); } catch (_err) {}
    try { node.focus({ preventScroll: true }); } catch (_err) {}
    try { node.click(); } catch (_err) {}

    if (!!node.checked !== target) {
      const label = node.closest("label") || (node.id ? document.querySelector("label[for='" + node.id + "']") : null);
      if (label) {
        try { label.click(); } catch (_err) {}
      }
    }

    if (!!node.checked !== target) {
      node.checked = target;
    }
    node.dispatchEvent(new Event("input", { bubbles: true }));
    node.dispatchEvent(new Event("change", { bubbles: true }));
    return !!node.checked === target;
  };

  if (allowAny) {
    setChecked(betterNode, false);
    setChecked(anyNode, true);
  } else {
    setChecked(anyNode, false);
    setChecked(betterNode, true);
  }

  const after = {
    any: getState(anyNode),
    better: getState(betterNode),
  };
  const success = allowAny
    ? (!!after.any && after.any.checked === true && (!after.better || after.better.checked === false))
    : (!!after.better && after.better.checked === true && (!after.any || after.any.checked === false));
  return {
    before,
    after,
    allow_any: !!allowAny,
    success,
    candidates: {
      any: anyPick.total,
      better: betterPick.total,
    },
  };
}
""",
            {"allowAny": bool(allow_any)},
        )
        if isinstance(result, dict):
            return result
        return {"allow_any": bool(allow_any)}

    async def _ensure_auto_accept_policy(
        self,
        page: AsyncPage,
        *,
        allow_any: bool,
        step_name: str,
        attempts: int = 4,
    ) -> dict[str, Any]:
        last_result: dict[str, Any] = {"allow_any": bool(allow_any)}
        for attempt in range(1, attempts + 1):
            current = await self._set_auto_accept_policy(page, allow_any=allow_any)
            if isinstance(current, dict):
                current["attempt"] = attempt
                last_result = current
            else:
                last_result = {"allow_any": bool(allow_any), "attempt": attempt}

            if bool(last_result.get("success", False)):
                return last_result
            await page.wait_for_timeout(180 + attempt * 120)

        raise BlogabetPublishError(
            step_name,
            "Не удалось гарантированно выставить auto-accept odds перед публикацией",
            diagnostics={"auto_accept_policy": last_result},
        )

    async def _clear_coupon(
        self,
        page: AsyncPage,
        *,
        max_rounds: int = 8,
        strict: bool = True,
    ) -> int:
        async def _coupon_snapshot() -> tuple[int, str]:
            cards = page.locator(SELECTORS.COUPON_PICK_CARD)
            card_count = await cards.count()
            badge_text = ""
            badge = page.locator(SELECTORS.COUPON_BADGE).first
            try:
                if await badge.count() > 0:
                    badge_text = normalize(await badge.inner_text())
            except Exception:  # noqa: BLE001
                badge_text = ""
            return card_count, badge_text

        trace: list[dict[str, Any]] = []
        initial_state = await self._coupon_state_snapshot(page)
        self._log(
            "info",
            "Blogabet clear_coupon: start strict=%s max_rounds=%s state=%s",
            strict,
            max_rounds,
            initial_state,
        )

        removed = 0
        for attempt_no in range(1, max_rounds * 2 + 1):
            trace_item: dict[str, Any] = {"attempt": attempt_no}
            card_count, badge_text = await _coupon_snapshot()
            trace_item["card_count_before"] = card_count
            trace_item["badge_before"] = badge_text
            if card_count == 0 and badge_text in {"", "0"}:
                trace_item["action"] = "already_empty"
                trace.append(trace_item)
                self._log("debug", "Blogabet clear_coupon attempt=%s details=%s", attempt_no, trace_item)
                break

            remove_buttons = page.locator(SELECTORS.COUPON_REMOVE_BUTTON)
            count = await remove_buttons.count()
            trace_item["remove_buttons_count"] = count
            if count == 0:
                # Иногда кнопка remove не кликается напрямую, но updateCoupon доступен.
                # Пробуем удалить через onclick токен.
                trace_item["action"] = "remove_button_not_found_try_js_update_coupon"
                removed_by_js = False
                try:
                    js_result = await page.evaluate(
                        """
() => {
  const card = document.querySelector("#_couponUpdate div[id^='couponBox_']");
  if (!card) return { ok: false, reason: "card_not_found" };
  const node =
    card.querySelector("a[onclick*='updateCoupon']") ||
    card.querySelector("a.btn.btn-xs.btn-darken.pull-right") ||
    document.querySelector("#_couponUpdate a[onclick*='updateCoupon']");
  if (!node) return { ok: false, reason: "remove_link_not_found" };
  const onclick = String(node.getAttribute('onclick') || '');
  const m = onclick.match(/updateCoupon\\((['"])(.*?)\\1\\)/);
  if (!m) {
    try {
      node.click();
      return { ok: true, method: "node_click_without_token" };
    } catch (_err) {
      return { ok: false, reason: "onclick_token_not_found" };
    }
  }
  if (typeof window.updateCoupon === 'function') {
    try {
      window.updateCoupon(m[2]);
      return { ok: true, method: "updateCoupon" };
    } catch (_err) {
      return { ok: false, reason: "updateCoupon_failed" };
    }
  }
  try {
    node.click();
    return { ok: true, method: "node_click_fallback" };
  } catch (_err) {
    return { ok: false, reason: "node_click_failed" };
  }
}
""",
                    )
                    removed_by_js = bool(js_result.get("ok")) if isinstance(js_result, dict) else bool(js_result)
                    trace_item["js_result"] = js_result
                except Exception as js_exc:  # noqa: BLE001
                    trace_item["js_error"] = normalize(str(js_exc))[:240]
                    removed_by_js = False
                trace_item["removed_by_js"] = bool(removed_by_js)
                if removed_by_js:
                    removed += 1
                    trace.append(trace_item)
                    self._log("debug", "Blogabet clear_coupon attempt=%s details=%s", attempt_no, trace_item)
                    await page.wait_for_timeout(700)
                    continue
                trace.append(trace_item)
                self._log("debug", "Blogabet clear_coupon attempt=%s details=%s", attempt_no, trace_item)
                break
            button = remove_buttons.first
            try:
                await button.scroll_into_view_if_needed()
                await button.click(timeout=3000)
                trace_item["action"] = "remove_click"
                removed += 1
                trace.append(trace_item)
                self._log("debug", "Blogabet clear_coupon attempt=%s details=%s", attempt_no, trace_item)
                await page.wait_for_timeout(700)
            except Exception as click_exc:  # noqa: BLE001
                # Fallback: dispatch click из JS и/или updateCoupon.
                trace_item["action"] = "remove_click_failed_try_js"
                trace_item["click_error"] = normalize(str(click_exc))[:240]
                removed_by_js = False
                try:
                    js_result = await page.evaluate(
                        """
() => {
  const card = document.querySelector("#_couponUpdate div[id^='couponBox_']");
  if (!card) return { ok: false, reason: "card_not_found" };
  const node =
    card.querySelector("a[onclick*='updateCoupon']") ||
    card.querySelector("a.btn.btn-xs.btn-darken.pull-right") ||
    document.querySelector("#_couponUpdate a[onclick*='updateCoupon']");
  if (!node) return { ok: false, reason: "remove_link_not_found" };
  try { node.click(); return { ok: true, method: "node_click_direct" }; } catch (_err) {}
  const onclick = String(node.getAttribute('onclick') || '');
  const m = onclick.match(/updateCoupon\\((['"])(.*?)\\1\\)/);
  if (!m) return { ok: false, reason: "onclick_token_not_found" };
  if (typeof window.updateCoupon === 'function') {
    try { window.updateCoupon(m[2]); return { ok: true, method: "updateCoupon" }; } catch (_err) {}
  }
  return { ok: false, reason: "updateCoupon_unavailable" };
}
""",
                    )
                    removed_by_js = bool(js_result.get("ok")) if isinstance(js_result, dict) else bool(js_result)
                    trace_item["js_result"] = js_result
                except Exception as js_exc:  # noqa: BLE001
                    trace_item["js_error"] = normalize(str(js_exc))[:240]
                    removed_by_js = False
                trace_item["removed_by_js"] = bool(removed_by_js)
                if removed_by_js:
                    removed += 1
                    trace.append(trace_item)
                    self._log("debug", "Blogabet clear_coupon attempt=%s details=%s", attempt_no, trace_item)
                    await page.wait_for_timeout(700)
                    continue
                trace.append(trace_item)
                self._log("debug", "Blogabet clear_coupon attempt=%s details=%s", attempt_no, trace_item)
                break

        # Гарантированная проверка: купон должен быть пуст (нет карточек + badge=0/empty).
        for _ in range(30):
            card_count, badge_text = await _coupon_snapshot()
            if card_count == 0 and badge_text in {"", "0"}:
                self._log(
                    "info",
                    "Blogabet clear_coupon: success removed=%s final_cards=%s final_badge=%s",
                    removed,
                    card_count,
                    badge_text,
                )
                return removed
            await page.wait_for_timeout(250)
        final_state = await self._coupon_state_snapshot(page)
        final_cards = int(final_state.get("card_count", 0))
        final_badge = normalize(str(final_state.get("badge_text", "")))
        trace_tail = trace[-25:]
        self._log(
            "warning",
            "Blogabet clear_coupon: failed strict=%s removed=%s initial=%s final=%s trace=%s",
            strict,
            removed,
            initial_state,
            final_state,
            trace_tail,
        )
        if strict:
            raise BlogabetPublishError(
                "clear_coupon",
                f"Не удалось очистить купон (cards={final_cards}, badge={final_badge or '-'})",
                diagnostics={
                    "coupon_clear_initial_state": initial_state,
                    "coupon_clear_final_state": final_state,
                    "coupon_clear_trace": trace_tail,
                    "coupon_cards_after_clear": final_cards,
                    "coupon_badge_after_clear": final_badge,
                    "removed_attempts": removed,
                },
            )
        return removed

    async def _coupon_needs_reselect(
        self,
        page: AsyncPage,
        expected_home: str,
        expected_away: str,
        intent: BetIntent,
    ) -> tuple[bool, dict[str, Any]]:
        diagnostics: dict[str, Any] = {}

        error_text = await self._collect_coupon_error_text(page)
        diagnostics["coupon_error_text"] = error_text

        pick_cards = page.locator(SELECTORS.COUPON_PICK_CARD)
        card_count = await pick_cards.count()
        diagnostics["coupon_card_count"] = card_count
        if card_count == 0:
            return False, diagnostics

        first_card = pick_cards.first
        title_text = ""
        card_raw_text = ""
        try:
            title_node = first_card.locator(SELECTORS.COUPON_PICK_TITLE).first
            if await title_node.count() > 0:
                title_text = normalize(await title_node.inner_text())
            else:
                title_text = normalize(await first_card.inner_text())
            card_raw_text = await first_card.inner_text()
        except Exception:  # noqa: BLE001
            title_text = ""
            card_raw_text = ""
        diagnostics["coupon_first_title"] = title_text

        matches_home = _contains_team_token(title_text, expected_home)
        matches_away = _contains_team_token(title_text, expected_away)
        diagnostics["coupon_matches_home"] = matches_home
        diagnostics["coupon_matches_away"] = matches_away

        market_ok, market_diag = _coupon_matches_intent(card_raw_text, intent)
        diagnostics["coupon_market_matches_intent"] = market_ok
        diagnostics["coupon_market_diag"] = market_diag

        has_reset_alert = _coupon_reset_alert_present(error_text)
        wrong_match = not (matches_home and matches_away)
        wrong_market = not market_ok
        need_reselect = wrong_match or wrong_market or has_reset_alert
        diagnostics["coupon_has_reset_alert"] = has_reset_alert
        diagnostics["coupon_need_reselect"] = need_reselect
        return need_reselect, diagnostics

    async def _resolve_create_pick_button(self, page: AsyncPage) -> Any:
        combined = page.locator(
            f"{SELECTORS.CREATE_PICK_BUTTON}, {SELECTORS.CREATE_PICK_BUTTON_FALLBACK}"
        )
        total = await combined.count()
        for index in range(total):
            candidate = combined.nth(index)
            try:
                if not await candidate.is_visible():
                    continue
                if not await candidate.is_enabled():
                    continue
                return candidate
            except Exception:  # noqa: BLE001
                continue
        return None

    async def _collect_create_pick_candidates(self, page: AsyncPage) -> list[str]:
        combined = page.locator(
            f"{SELECTORS.CREATE_PICK_BUTTON}, {SELECTORS.CREATE_PICK_BUTTON_FALLBACK}, button"
        )
        total = await combined.count()
        samples: list[str] = []
        limit = min(total, 15)
        for index in range(limit):
            node = combined.nth(index)
            try:
                text = normalize(await node.inner_text())
                cls = normalize(await node.get_attribute("class") or "")
                onclick = normalize(await node.get_attribute("onclick") or "")
                visible = await node.is_visible()
                samples.append(
                    f"text='{text}' class='{cls}' onclick='{onclick}' visible={visible}"
                )
            except Exception:  # noqa: BLE001
                continue
        return samples

    async def _extract_pick_url(self, page: AsyncPage, start_url: str, before_badge: str) -> Optional[str]:
        for _ in range(30):
            await page.wait_for_timeout(500)

            error_locator = page.locator(SELECTORS.COUPON_ERROR)
            if await error_locator.count() > 0:
                try:
                    text = normalize(await error_locator.first.inner_text())
                except Exception:  # noqa: BLE001
                    text = ""
                raise BlogabetPublishError("submit_pick", f"Ошибка публикации купона: {text or 'неизвестно'}")

            current_url = normalize(page.url)
            if current_url and current_url != normalize(start_url):
                return current_url

            pick_link = page.locator("a[href*='/pick/'], a[href*='/picks/'], a[href*='/tips/']")
            if await pick_link.count() > 0:
                href = await pick_link.first.get_attribute("href")
                if href:
                    return href

            badge_locator = page.locator(SELECTORS.COUPON_BADGE)
            if await badge_locator.count() > 0:
                current_badge = normalize(await badge_locator.first.inner_text())
                if before_badge and current_badge and current_badge != before_badge:
                    if current_badge == "0":
                        return current_url or start_url

        return None

    @staticmethod
    def _pick_id_from_url(url: Optional[str]) -> Optional[int]:
        if not url:
            return None
        match = re.search(r"/(?:pick|picks|tips)/(\d+)", url)
        if match is None:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    async def publish_pick(
        self,
        match: Any,
        bet_intent: BetIntent,
        stake: int,
        analysis_text: str,
        *,
        dry_run: bool = False,
        diagnostics_context: Optional[dict[str, Any]] = None,
    ) -> PublishResult:
        async with self._lock:
            self._context = await self._ensure_browser_context()
            if self._context is None:
                raise BlogabetAuthRequired("Контекст Blogabet не инициализирован")

            page = await self._context.new_page()
            step_name = "open_upcoming"
            diagnostics: dict[str, Any] = dict(diagnostics_context or {})
            diagnostics["bet_intent"] = bet_intent.__dict__

            try:
                await page.goto(self.cfg.upcoming_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(900)
                current_url = normalize(page.url).lower()
                if "login" in current_url:
                    raise BlogabetAuthRequired(
                        "Сессия Blogabet недействительна. Выполните ручной логин и сохраните storage state."
                    )
                await self._dismiss_age_confirmation(page)

                sport_key = _detect_match_sport_key(match, bet_intent)
                sport_target = _sport_target(sport_key)
                sport_title = _sport_title(sport_key)
                league_links_selector = _league_links_selector_for_sport(sport_key)
                diagnostics["sport"] = {
                    "key": sport_key,
                    "target": sport_target,
                    "title": sport_title,
                }

                step_name = "select_sport"
                await self._select_sport(page, sport_target=sport_target, sport_title=sport_title)

                step_name = "find_league"
                best_block_index, match_result, selected_league = await self._find_match_with_league_fallback(
                    page,
                    match,
                    bet_intent,
                    diagnostics,
                    league_links_selector=league_links_selector,
                    sport_title=sport_title,
                )
                diagnostics["selected_league"] = selected_league
                diagnostics["match_candidates"] = match_result.get("top_candidates", [])
                diagnostics["selected_match"] = match_result.get("best")

                active_tab = page.locator(SELECTORS.ACTIVE_TAB).first
                block = active_tab.locator(SELECTORS.BLOCK_CONTENT).nth(best_block_index)

                if not dry_run:
                    step_name = "pre_pick_coupon_alert_check"
                    coupon_error_before_pick = await self._collect_coupon_error_text(page)
                    diagnostics["coupon_error_before_pick"] = coupon_error_before_pick
                    if _coupon_reset_alert_present(coupon_error_before_pick):
                        step_name = "clear_coupon"
                        cleared_before_pick = await self._clear_coupon(page, strict=True)
                        diagnostics["coupon_items_removed_before_pick"] = cleared_before_pick

                step_name = "find_market"
                market_result = await self._pick_odd_button(block, bet_intent)
                diagnostics["market_candidates"] = market_result.get("top_candidates", [])
                diagnostics["selected_market"] = market_result.get("best")

                if not dry_run:
                    step_name = "validate_coupon_after_pick"
                    need_reselect, coupon_diag = await self._coupon_needs_reselect(
                        page,
                        match.home_team,
                        match.away_team,
                        bet_intent,
                    )
                    diagnostics["coupon_check_after_pick"] = coupon_diag
                    if need_reselect:
                        if bool(coupon_diag.get("coupon_has_reset_alert")):
                            step_name = "coupon_reselect"
                            removed_for_reselect = await self._clear_coupon(page, strict=True)
                            diagnostics["coupon_items_removed_for_reselect"] = removed_for_reselect
                            # Повторяем выбор рынка после очистки.
                            market_result_retry = await self._pick_odd_button(block, bet_intent)
                            diagnostics["selected_market_after_reselect"] = market_result_retry.get("best")
                            diagnostics["market_candidates_after_reselect"] = market_result_retry.get(
                                "top_candidates", []
                            )
                            need_reselect_retry, coupon_diag_retry = await self._coupon_needs_reselect(
                                page,
                                match.home_team,
                                match.away_team,
                                bet_intent,
                            )
                            diagnostics["coupon_check_after_reselect"] = coupon_diag_retry
                            if need_reselect_retry:
                                raise BlogabetPublishError(
                                    "find_market",
                                    "Купон после reselect не соответствует целевой ставке",
                                    diagnostics=diagnostics,
                                )
                        else:
                            raise BlogabetPublishError(
                                "find_market",
                                "Купон не соответствует целевой ставке, но reset-alert в купоне отсутствует",
                                diagnostics=diagnostics,
                            )

                if dry_run:
                    return PublishResult(success=True, pick_url=None, diagnostics=diagnostics)

                step_name = "fill_coupon"
                await self._set_stake(page, stake)
                await self._set_analysis_text(page, analysis_text)
                score_candidate = bet_intent.current_score or _normalize_score(getattr(match, "score", ""))
                live_score_filled = await self._set_live_score_if_required(page, score_candidate)
                diagnostics["live_score_candidate"] = score_candidate
                diagnostics["live_score_filled"] = live_score_filled

                step_name = "pre_submit_guard"
                for precheck_attempt in range(2):
                    guard_ok, guard_diag, guard_reason = await self._ensure_coupon_ready_before_submit(
                        page,
                        match,
                        bet_intent,
                        score_candidate,
                    )
                    diagnostics[f"pre_submit_guard_{precheck_attempt + 1}"] = guard_diag
                    if guard_ok:
                        break
                    if guard_reason == "missing_live_score":
                        raise BlogabetPublishError(
                            "fill_coupon",
                            "Для live купона требуется current score, но он не распознан",
                            diagnostics=diagnostics,
                        )
                    if guard_reason == "live_score_not_filled":
                        diagnostics["pre_submit_guard_recovery"] = guard_reason
                        await self._set_live_score_if_required(page, score_candidate)
                        await page.wait_for_timeout(400)
                        if precheck_attempt >= 1:
                            raise BlogabetPublishError(
                                "fill_coupon",
                                "Поле current score в купоне не заполнено",
                                diagnostics=diagnostics,
                            )
                        continue
                    if guard_reason == "coupon_alert_requires_reset":
                        diagnostics["pre_submit_guard_recovery"] = guard_reason
                        removed_for_pre_submit_recovery = await self._clear_coupon(page, strict=True)
                        diagnostics["coupon_items_removed_for_pre_submit_recovery"] = (
                            removed_for_pre_submit_recovery
                        )
                        market_result_pre_submit_retry = await self._pick_odd_button(block, bet_intent)
                        diagnostics["selected_market_after_pre_submit_recovery"] = (
                            market_result_pre_submit_retry.get("best")
                        )
                        await self._set_stake(page, stake)
                        await self._set_analysis_text(page, analysis_text)
                        await self._set_live_score_if_required(page, score_candidate)
                        if precheck_attempt >= 1:
                            raise BlogabetPublishError(
                                "fill_coupon",
                                "Купон содержит reset-alert даже после повторного выбора ставки",
                                diagnostics=diagnostics,
                            )
                        continue
                    raise BlogabetPublishError(
                        "fill_coupon",
                        "Купон не соответствует целевому матчу/ставке перед submit",
                        diagnostics=diagnostics,
                    )

                badge_locator = page.locator(SELECTORS.COUPON_BADGE)
                before_badge = ""
                if await badge_locator.count() > 0:
                    before_badge = normalize(await badge_locator.first.inner_text())

                step_name = "submit_pick"
                await page.wait_for_selector(
                    f"{SELECTORS.CREATE_PICK_BUTTON}, {SELECTORS.CREATE_PICK_BUTTON_FALLBACK}",
                    timeout=15000,
                )
                create_button = await self._resolve_create_pick_button(page)
                if create_button is None:
                    diagnostics["create_pick_candidates"] = await self._collect_create_pick_candidates(page)
                    raise BlogabetPublishError("submit_pick", "Кнопка Create pick не найдена")
                diagnostics["auto_accept_policy_before_submit"] = await self._ensure_auto_accept_policy(
                    page,
                    allow_any=True,
                    step_name="submit_pick",
                )
                start_url = page.url
                await create_button.scroll_into_view_if_needed()
                await create_button.click()
                await page.wait_for_timeout(6000)

                try:
                    pick_url = await self._extract_pick_url(page, start_url, before_badge)
                except BlogabetPublishError as submit_exc:
                    lowered_reason = normalize(submit_exc.reason).lower()
                    is_recoverable_price_change = is_recoverable_submit_error(lowered_reason)
                    should_retry_submit = (
                        _coupon_reset_alert_present(lowered_reason)
                        or is_recoverable_price_change
                    )
                    if should_retry_submit:
                        max_recovery_attempts = 3 if is_recoverable_price_change else 1
                        diagnostics["submit_recovery"] = (
                            "price_changed_multi_retry"
                            if is_recoverable_price_change
                            else "clear_coupon_and_retry_once"
                        )
                        last_retry_exc: Optional[BlogabetPublishError] = submit_exc
                        pick_url = None
                        for recovery_attempt in range(1, max_recovery_attempts + 1):
                            diagnostics[f"submit_recovery_attempt_{recovery_attempt}_reason"] = lowered_reason
                            if recovery_attempt > 1:
                                await page.wait_for_timeout(900 + recovery_attempt * 500)
                            recovered_removed = await self._clear_coupon(page, strict=True)
                            diagnostics[f"coupon_items_removed_for_recovery_{recovery_attempt}"] = (
                                recovered_removed
                            )
                            if recovered_removed <= 0:
                                raise
                            market_result_submit_retry = await self._pick_odd_button(block, bet_intent)
                            diagnostics[f"selected_market_after_submit_recovery_{recovery_attempt}"] = (
                                market_result_submit_retry.get("best")
                            )
                            await self._set_stake(page, stake)
                            await self._set_analysis_text(page, analysis_text)
                            score_candidate_retry = (
                                bet_intent.current_score or _normalize_score(getattr(match, "score", ""))
                            )
                            live_score_filled_retry = await self._set_live_score_if_required(
                                page,
                                score_candidate_retry,
                            )
                            diagnostics[f"live_score_candidate_retry_{recovery_attempt}"] = score_candidate_retry
                            diagnostics[f"live_score_filled_retry_{recovery_attempt}"] = live_score_filled_retry
                            odds_policy = await self._set_auto_accept_policy(page, allow_any=True)
                            diagnostics[f"odds_policy_retry_{recovery_attempt}"] = odds_policy

                            need_reselect_submit_retry, coupon_diag_submit_retry = await self._coupon_needs_reselect(
                                page,
                                match.home_team,
                                match.away_team,
                                bet_intent,
                            )
                            diagnostics[f"coupon_check_after_submit_recovery_pick_{recovery_attempt}"] = (
                                coupon_diag_submit_retry
                            )
                            if need_reselect_submit_retry:
                                raise BlogabetPublishError(
                                    "submit_pick",
                                    "Купон после submit-recovery не соответствует целевой ставке",
                                    diagnostics=diagnostics,
                                )
                            guard_ok_retry, guard_diag_retry, guard_reason_retry = (
                                await self._ensure_coupon_ready_before_submit(
                                    page,
                                    match,
                                    bet_intent,
                                    score_candidate_retry,
                                )
                            )
                            diagnostics[f"pre_submit_guard_submit_recovery_{recovery_attempt}"] = guard_diag_retry
                            if not guard_ok_retry:
                                if guard_reason_retry == "missing_live_score":
                                    raise BlogabetPublishError(
                                        "submit_pick",
                                        "Для live купона требуется current score, но он не распознан",
                                        diagnostics=diagnostics,
                                    )
                                if guard_reason_retry == "live_score_not_filled":
                                    raise BlogabetPublishError(
                                        "submit_pick",
                                        "Поле current score в купоне не заполнено после recovery",
                                        diagnostics=diagnostics,
                                    )
                                raise BlogabetPublishError(
                                    "submit_pick",
                                    "Купон после submit-recovery не готов к публикации",
                                    diagnostics=diagnostics,
                                )

                            await page.wait_for_selector(
                                f"{SELECTORS.CREATE_PICK_BUTTON}, {SELECTORS.CREATE_PICK_BUTTON_FALLBACK}",
                                timeout=15000,
                            )
                            retry_button = await self._resolve_create_pick_button(page)
                            if retry_button is None:
                                diagnostics["create_pick_candidates_retry"] = (
                                    await self._collect_create_pick_candidates(page)
                                )
                                raise BlogabetPublishError(
                                    "submit_pick",
                                    "Кнопка Create pick не найдена после recovery",
                                )
                            diagnostics[f"auto_accept_policy_before_retry_submit_{recovery_attempt}"] = (
                                await self._ensure_auto_accept_policy(
                                    page,
                                    allow_any=True,
                                    step_name="submit_pick",
                                )
                            )
                            retry_start_url = page.url
                            await retry_button.scroll_into_view_if_needed()
                            await retry_button.click()
                            await page.wait_for_timeout(6000)
                            try:
                                pick_url = await self._extract_pick_url(page, retry_start_url, before_badge)
                                break
                            except BlogabetPublishError as retry_exc:
                                last_retry_exc = retry_exc
                                lowered_retry_reason = normalize(retry_exc.reason).lower()
                                diagnostics[f"submit_recovery_attempt_{recovery_attempt}_error"] = (
                                    lowered_retry_reason
                                )
                                if (
                                    is_recoverable_price_change
                                    and is_recoverable_submit_error(lowered_retry_reason)
                                    and recovery_attempt < max_recovery_attempts
                                ):
                                    await page.wait_for_timeout(900 + recovery_attempt * 500)
                                    continue
                                raise
                        if not pick_url and last_retry_exc is not None:
                            raise last_retry_exc
                    else:
                        raise
                if not pick_url:
                    raise BlogabetPublishError(
                        "submit_pick",
                        "Не удалось подтвердить успешную публикацию в Blogabet",
                        diagnostics=diagnostics,
                    )

                pick_id = self._pick_id_from_url(pick_url)
                return PublishResult(
                    success=True,
                    pick_id=pick_id,
                    pick_url=pick_url,
                    diagnostics=diagnostics,
                )

            except BlogabetPublishError as exc:
                merged_diagnostics = dict(diagnostics)
                merged_diagnostics.update(exc.diagnostics)
                if exc.step_name in {"find_league", "switch_period_tab", "find_match"}:
                    self._append_mismatch_diagnostics(
                        match=match,
                        bet_intent=bet_intent,
                        diagnostics=merged_diagnostics,
                        failure_step=exc.step_name,
                        failure_reason=exc.reason,
                    )
                screenshot_path, html_dump_path = await self._capture_debug_artifacts(page, exc.step_name)
                raise BlogabetPublishError(
                    step_name=exc.step_name,
                    reason=exc.reason,
                    diagnostics=merged_diagnostics,
                    screenshot_path=screenshot_path,
                    html_dump_path=html_dump_path,
                ) from exc
            except Exception as exc:  # noqa: BLE001
                screenshot_path, html_dump_path = await self._capture_debug_artifacts(page, step_name)
                raise BlogabetPublishError(
                    step_name=step_name,
                    reason=str(exc),
                    diagnostics=diagnostics,
                    screenshot_path=screenshot_path,
                    html_dump_path=html_dump_path,
                ) from exc
            finally:
                await page.close()

    @staticmethod
    def format_diagnostics(diagnostics: Optional[dict[str, Any]]) -> str:
        if not diagnostics:
            return ""
        try:
            return json.dumps(diagnostics, ensure_ascii=False, indent=2)
        except Exception:  # noqa: BLE001
            return str(diagnostics)
