import asyncio
import html
import json
import logging
import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional
from urllib.parse import (
    parse_qsl,
    unquote,
    urlencode,
    urljoin,
    urlparse,
    urlsplit,
    urlunsplit,
)

import aiohttp
from dotenv import load_dotenv
from flask import Flask, redirect, render_template_string, request, url_for
from playwright.async_api import (
    Page as AsyncPage,
    Playwright as AsyncPlaywright,
    async_playwright,
)
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, Playwright, sync_playwright

from bet_intent import BetIntent, parse_bet_intent
from blogabet_publisher import (
    BlogabetAuthRequired,
    BlogabetConfig,
    BlogabetPublishError,
    BlogabetPublisher,
    PublishResult,
)
from ocr_client import OcrError, OcrSpaceClient

app = Flask(__name__)
logging.basicConfig(
    level=getattr(logging, os.getenv("APP_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("alpinbet_parser")


def configure_message_edit_logger() -> logging.Logger:
    log_level = getattr(
        logging,
        os.getenv("APP_LOG_LEVEL", "INFO").upper(),
        logging.INFO,
    )
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "message_edits.log"

    max_bytes_raw = (os.getenv("MESSAGE_EDIT_LOG_MAX_BYTES", "1048576") or "").strip()
    backup_count_raw = (os.getenv("MESSAGE_EDIT_LOG_BACKUP_COUNT", "5") or "").strip()
    try:
        max_bytes = max(1024, int(max_bytes_raw))
    except ValueError:
        max_bytes = 1048576
    try:
        backup_count = max(1, int(backup_count_raw))
    except ValueError:
        backup_count = 5

    message_logger = logging.getLogger("alpinbet_parser.message_edit")
    message_logger.setLevel(log_level)
    message_logger.propagate = False

    if not message_logger.handlers:
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        message_logger.addHandler(file_handler)

    return message_logger


message_edit_logger = configure_message_edit_logger()


def configure_blogabet_error_logger() -> logging.Logger:
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "blogabet_errors.log"

    max_bytes_raw = (os.getenv("BLOGABET_ERROR_LOG_MAX_BYTES", "1048576") or "").strip()
    backup_count_raw = (os.getenv("BLOGABET_ERROR_LOG_BACKUP_COUNT", "5") or "").strip()
    try:
        max_bytes = max(1024, int(max_bytes_raw))
    except ValueError:
        max_bytes = 1048576
    try:
        backup_count = max(1, int(backup_count_raw))
    except ValueError:
        backup_count = 5

    blogabet_logger = logging.getLogger("alpinbet_parser.blogabet_errors")
    blogabet_logger.setLevel(logging.INFO)
    blogabet_logger.propagate = False

    if not blogabet_logger.handlers:
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        blogabet_logger.addHandler(file_handler)

    return blogabet_logger


blogabet_error_logger = configure_blogabet_error_logger()


def log_message_edit_info(message: str, *args: Any) -> None:
    logger.info(message, *args)
    message_edit_logger.info(message, *args)


def log_message_edit_exception(message: str, *args: Any) -> None:
    logger.exception(message, *args)
    message_edit_logger.exception(message, *args)


def log_blogabet_error(message: str, *args: Any) -> None:
    logger.error(message, *args)
    blogabet_error_logger.error(message, *args)


def log_blogabet_exception(message: str, *args: Any) -> None:
    logger.exception(message, *args)
    blogabet_error_logger.exception(message, *args)

DEFAULT_PARSER_URL = "https://alpinbet.com/dispatch/id1631660353/pbd-1-fon"
DEFAULT_PARSE_ITEM_SELECTOR = ".rTableLine"
DEFAULT_PANEL_CONTAINER_SELECTOR = ".panel-container"
DEFAULT_PARSER_INTERVAL_SECONDS = 10
DEFAULT_PARSER_PAGE_MAX_AGE_SECONDS = 60
DEFAULT_TELEGRAM_REQUEST_TIMEOUT_SECONDS = 20
DEFAULT_VK_REQUEST_TIMEOUT_SECONDS = 20
DEFAULT_VK_API_VERSION = "5.199"
DEFAULT_DAILY_STATS_SEND_HOUR_MSK = 9
DEFAULT_WEEKLY_STATS_SEND_HOUR_MSK = 9
DEFAULT_MONTHLY_STATS_SEND_HOUR_MSK = 9
DEFAULT_DAILY_STATS_LOOKUP_MAX_PAGES = 20
ACTIVE_MATCH_IMAGE_RETRY_WAIT_MS = 1200
PARSER_SOURCES_STORAGE_FILENAME = "parser_sources.json"
DEFAULT_MATCH_DATABASE_URL = "sqlite:///parser_matches.db"
MAX_PENDING_SETTLEMENT_CANDIDATES = 500
DEFAULT_BLOGABET_STORAGE_STATE_PATH = "./blogabet_state.json"
DEFAULT_BLOGABET_LEAGUE_ALIASES_PATH = "./blogabet_league_aliases.json"
DEFAULT_BLOGABET_STAKE = 3


@dataclass
class TargetConfig:
    login_url: str
    data_url: str
    open_login_selector: str
    login_username: str
    email_selector: str
    password_selector: str
    submit_selector: str
    code_selector: str
    code_submit_selector: str
    parse_item_selector: str
    panel_container_selector: str
    login_form_selector: str
    login_error_selector: str
    parser_interval_seconds: int
    parser_page_max_age_seconds: int
    parser_send_existing_on_start: bool
    daily_stats_send_hour_msk: int
    weekly_stats_send_hour_msk: int
    monthly_stats_send_hour_msk: int
    headless: bool


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str
    request_timeout_seconds: int = DEFAULT_TELEGRAM_REQUEST_TIMEOUT_SECONDS
    use_system_proxy: bool = False


@dataclass(frozen=True)
class VkConfig:
    user_token: str
    api_version: str = DEFAULT_VK_API_VERSION
    request_timeout_seconds: int = DEFAULT_VK_REQUEST_TIMEOUT_SECONDS
    use_system_proxy: bool = False


@dataclass(frozen=True)
class ParsedMatch:
    home_team: str
    away_team: str
    tournament: str
    event_time: str
    score: str
    rate: str
    rate_description: str
    href: str
    image_url: str
    unique_key: str


@dataclass(frozen=True)
class DailyStatsSnapshot:
    dispatch_title: str
    stats_date: str
    win_count: int
    lose_count: int
    return_count: int
    profit_percent: str
    verifier_url: str


@dataclass(frozen=True)
class WeeklyStatsDaySnapshot:
    stats_date: str
    profit_percent: str
    settlement_status: str  # win | lose | return


@dataclass(frozen=True)
class WeeklyStatsSnapshot:
    dispatch_title: str
    period_label: str
    total_profit_percent: str
    day_items: tuple[WeeklyStatsDaySnapshot, ...]
    verifier_url: str


@dataclass(frozen=True)
class MonthlyStatsSnapshot:
    dispatch_title: str
    month_label: str
    profit_percent: str
    verifier_url: str


@dataclass(frozen=True)
class SettledMatchSnapshot:
    home_team: str
    away_team: str
    tournament: str
    rate: str
    rate_description: str
    href: str
    unique_key: str
    match_signature: str
    score: str
    net_profit_units: int
    settlement_status: str  # win | lose | return


@dataclass
class SentMatchRecord:
    delivery_key: str
    source_id: str
    source_url: str
    chat_id: str
    message_id: int
    message_text: str
    match_unique_key: str
    match_signature: str
    match_href: str
    match_lookup_key: str
    home_team: str
    away_team: str
    settled: bool = False
    settlement_status: str = ""
    settlement_profit_units: int = 0
    settlement_score: str = ""
    settlement_updated_at: str = ""
    settlement_error: str = ""
    tracked_status: str = ""
    tracked_settlement_status: str = ""
    tracked_settlement_profit_units: int = 0
    tracked_settlement_score: str = ""


@dataclass
class ParserSource:
    source_id: str
    url: str
    chat_id: str = ""
    vk_chat_ids: tuple[str, ...] = ()
    enabled: bool = True


@dataclass
class SourcePageRuntime:
    page: AsyncPage
    created_at_monotonic: float
    last_match_count: int = 0


class LoginRequiredError(RuntimeError):
    pass


class DailyStatsNotFoundError(RuntimeError):
    pass


class WeeklyStatsNotFoundError(RuntimeError):
    pass


class MonthlyStatsNotFoundError(RuntimeError):
    pass


class BrowserState:
    def __init__(self) -> None:
        self.lock = threading.RLock()

        self.playwright: Optional[Playwright] = None
        self.page: Optional[Page] = None
        self.auth_storage_state: Optional[dict[str, Any]] = None

        self.step: str = "idle"  # idle | await_code | ready
        self.error: str = ""
        self.info: str = ""

        self.preview: str = ""
        self.last_message_id: Optional[int] = None

        self.parser_thread: Optional[threading.Thread] = None
        self.parser_stop_event: Optional[threading.Event] = None
        self.parser_running: bool = False

        self.parser_sources: list[ParserSource] = []
        self.parser_source_seq: int = 0
        self.parser_interval_seconds: int = DEFAULT_PARSER_INTERVAL_SECONDS
        self.parser_interval_initialized: bool = False
        self.parser_page_max_age_seconds: int = DEFAULT_PARSER_PAGE_MAX_AGE_SECONDS
        self.parser_page_max_age_initialized: bool = False
        self.pending_match_keys: set[str] = set()
        self.pending_settlement_keys: set[str] = set()

        self.parser_last_check_at: str = ""
        self.parser_last_sent_at: str = ""
        self.parser_last_match_title: str = ""
        self.parser_last_settled_at: str = ""
        self.parser_last_settled_title: str = ""
        self.parser_last_daily_sent_at: str = ""
        self.parser_last_daily_date: str = ""
        self.parser_last_daily_title: str = ""
        self.parser_last_weekly_sent_at: str = ""
        self.parser_last_weekly_period: str = ""
        self.parser_last_weekly_title: str = ""
        self.parser_last_monthly_sent_at: str = ""
        self.parser_last_monthly_period: str = ""
        self.parser_last_monthly_title: str = ""
        self.parser_error: str = ""
        self.daily_stats_sent_by_source: dict[str, str] = {}
        self.daily_stats_inflight_sources: set[str] = set()
        self.weekly_stats_sent_by_source: dict[str, str] = {}
        self.weekly_stats_inflight_sources: set[str] = set()
        self.monthly_stats_sent_by_source: dict[str, str] = {}
        self.monthly_stats_inflight_sources: set[str] = set()
        self.vk_chat_lookup_results: list[dict[str, Any]] = []
        self.vk_chat_lookup_loaded: bool = False
        self.vk_chat_lookup_limit: int = 200
        self.vk_chat_lookup_last_at: str = ""
        self.blogabet_test_log: str = ""
        self.blogabet_test_pick_url: str = ""
        self.blogabet_test_screenshot_path: str = ""
        self.blogabet_test_html_dump_path: str = ""
        self.blogabet_test_diagnostics: str = ""
        self.blogabet_ocr_log: str = ""

    def clear_runtime(self) -> None:
        with self.lock:
            page = self.page
            playwright = self.playwright
            self.page = None
            self.playwright = None

        if page is not None:
            try:
                page.context.browser.close()
            except PlaywrightError:
                pass

        if playwright is not None:
            try:
                playwright.stop()
            except PlaywrightError:
                pass

    def stop_parser(self) -> None:
        with self.lock:
            stop_event = self.parser_stop_event
            thread = self.parser_thread
            self.parser_stop_event = None

        if stop_event is not None:
            stop_event.set()

        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=8)

        with self.lock:
            self.parser_thread = None
            self.parser_running = False

    def reset(self) -> None:
        self.stop_parser()
        self.clear_runtime()

        with self.lock:
            self.step = "idle"
            self.error = ""
            self.info = ""
            self.preview = ""
            self.last_message_id = None
            self.auth_storage_state = None

            self.parser_sources = []
            self.parser_source_seq = 0
            self.parser_interval_seconds = DEFAULT_PARSER_INTERVAL_SECONDS
            self.parser_interval_initialized = False
            self.parser_page_max_age_seconds = DEFAULT_PARSER_PAGE_MAX_AGE_SECONDS
            self.parser_page_max_age_initialized = False
            self.pending_match_keys = set()
            self.pending_settlement_keys = set()
            self.parser_last_check_at = ""
            self.parser_last_sent_at = ""
            self.parser_last_match_title = ""
            self.parser_last_settled_at = ""
            self.parser_last_settled_title = ""
            self.parser_last_daily_sent_at = ""
            self.parser_last_daily_date = ""
            self.parser_last_daily_title = ""
            self.parser_last_weekly_sent_at = ""
            self.parser_last_weekly_period = ""
            self.parser_last_weekly_title = ""
            self.parser_last_monthly_sent_at = ""
            self.parser_last_monthly_period = ""
            self.parser_last_monthly_title = ""
            self.parser_error = ""
            self.daily_stats_sent_by_source = {}
            self.daily_stats_inflight_sources = set()
            self.weekly_stats_sent_by_source = {}
            self.weekly_stats_inflight_sources = set()
            self.monthly_stats_sent_by_source = {}
            self.monthly_stats_inflight_sources = set()
            self.vk_chat_lookup_results = []
            self.vk_chat_lookup_loaded = False
            self.vk_chat_lookup_limit = 200
            self.vk_chat_lookup_last_at = ""
            self.blogabet_test_log = ""
            self.blogabet_test_pick_url = ""
            self.blogabet_test_screenshot_path = ""
            self.blogabet_test_html_dump_path = ""
            self.blogabet_test_diagnostics = ""
            self.blogabet_ocr_log = ""


state = BrowserState()

MSK_TIMEZONE = timezone(timedelta(hours=3), name="MSK")
RUS_MONTH_SHORT = {
    1: "Янв",
    2: "Фев",
    3: "Мар",
    4: "Апр",
    5: "Май",
    6: "Июн",
    7: "Июл",
    8: "Авг",
    9: "Сен",
    10: "Окт",
    11: "Ноя",
    12: "Дек",
}
RUS_MONTH_ALIASES = {
    "янв": 1,
    "январь": 1,
    "фев": 2,
    "февраль": 2,
    "мар": 3,
    "март": 3,
    "апр": 4,
    "апрель": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июнь": 6,
    "июл": 7,
    "июль": 7,
    "авг": 8,
    "август": 8,
    "сен": 9,
    "сент": 9,
    "сентябрь": 9,
    "окт": 10,
    "октябрь": 10,
    "ноя": 11,
    "ноябрь": 11,
    "дек": 12,
    "декабрь": 12,
}


def now_label() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


def now_label_msk() -> str:
    return datetime.now(MSK_TIMEZONE).strftime("%d.%m.%Y %H:%M:%S")


def now_storage_label_msk() -> str:
    return datetime.now(MSK_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")


def normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def mask_token(token: str) -> str:
    token = normalize_text(token)
    if not token:
        return "не задан"
    if len(token) <= 10:
        return token[:2] + "..." + token[-2:]
    return token[:6] + "..." + token[-6:]


def parse_bool_env(value: str, default: bool = False) -> bool:
    normalized = value.strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "y", "on"}


def upsert_env_value(key: str, value: str, env_path: Optional[Path] = None) -> None:
    target_path = env_path or (Path(__file__).resolve().parent / ".env")
    line_re = re.compile(rf"^\\s*{re.escape(key)}=")

    if target_path.exists():
        lines = target_path.read_text(encoding="utf-8").splitlines()
    else:
        lines = []

    updated = False
    duplicate_count = 0
    new_lines: list[str] = []
    for line in lines:
        if line_re.match(line):
            if not updated:
                new_lines.append(f"{key}={value}")
                updated = True
            else:
                duplicate_count += 1
            continue
        new_lines.append(line)

    if not updated:
        if new_lines and new_lines[-1].strip():
            new_lines.append("")
        new_lines.append(f"{key}={value}")

    target_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    if duplicate_count > 0:
        logger.info(
            "Удалены дубли параметра в .env. key=%s duplicates=%s",
            key,
            duplicate_count,
        )


def remove_query_param(url: str, key: str) -> str:
    normalized_url = normalize_text(url)
    if not normalized_url:
        return normalized_url

    try:
        parts = urlsplit(normalized_url)
        filtered_query = [
            (param_key, param_value)
            for param_key, param_value in parse_qsl(parts.query, keep_blank_values=True)
            if param_key != key
        ]
        rebuilt_query = urlencode(filtered_query, doseq=True)
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                rebuilt_query,
                parts.fragment,
            )
        )
    except Exception:  # noqa: BLE001
        return normalized_url


def upsert_query_param(url: str, key: str, value: str) -> str:
    normalized_url = normalize_text(url)
    if not normalized_url:
        return normalized_url

    try:
        parts = urlsplit(normalized_url)
        query_items = parse_qsl(parts.query, keep_blank_values=True)
        filtered_query = [
            (param_key, param_value)
            for param_key, param_value in query_items
            if param_key != key
        ]
        filtered_query.append((key, value))
        rebuilt_query = urlencode(filtered_query, doseq=True)
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                rebuilt_query,
                parts.fragment,
            )
        )
    except Exception:  # noqa: BLE001
        return normalized_url


def parse_interval_seconds(raw_value: str, *, clamp_min: bool = False) -> int:
    value = normalize_text(raw_value)
    if not value:
        raise ValueError("Интервал проверки не задан")

    try:
        interval_seconds = int(value)
    except ValueError as exc:
        raise ValueError("Интервал проверки должен быть целым числом") from exc

    if interval_seconds < 10:
        if clamp_min:
            return 10
        raise ValueError("Интервал проверки должен быть не меньше 10 секунд")

    return interval_seconds


def parse_hour_value(
    raw_value: str,
    *,
    field_label: str,
) -> int:
    value = normalize_text(raw_value)
    if not value:
        raise ValueError(f"{field_label} не задан")

    try:
        hour = int(value)
    except ValueError as exc:
        raise ValueError(f"{field_label} должен быть целым числом") from exc

    if hour < 0 or hour > 23:
        raise ValueError(f"{field_label} должен быть в диапазоне 0..23")

    return hour


def parse_days_ago(raw_value: str) -> int:
    value = normalize_text(raw_value)
    if not value:
        raise ValueError("Нужно указать число дней назад")

    try:
        days_ago = int(value)
    except ValueError as exc:
        raise ValueError("Число дней назад должно быть целым числом") from exc

    if days_ago < 0:
        raise ValueError("Число дней назад не может быть отрицательным")
    if days_ago > 365:
        raise ValueError("Для теста доступно не более 365 дней назад")
    return days_ago


def parse_min_seconds_value(
    raw_value: str,
    *,
    field_label: str,
    minimum_seconds: int,
) -> int:
    value = normalize_text(raw_value)
    if not value:
        raise ValueError(f"{field_label} не задан")

    try:
        seconds = int(value)
    except ValueError as exc:
        raise ValueError(f"{field_label} должен быть целым числом") from exc

    if seconds < minimum_seconds:
        raise ValueError(
            f"{field_label} должен быть не меньше {minimum_seconds} секунд"
        )

    return seconds


def normalize_source_url(url: str) -> str:
    normalized = normalize_text(url)
    if normalized.endswith("/"):
        normalized = normalized.rstrip("/")
    return normalized


def normalize_chat_id(chat_id: str) -> str:
    return normalize_text(chat_id)


def validate_chat_id(chat_id: str) -> str:
    normalized_chat_id = normalize_chat_id(chat_id)
    if not normalized_chat_id:
        raise ValueError("Нужно указать chat_id Telegram-канала")
    if normalized_chat_id.startswith("@"):
        return normalized_chat_id
    if re.fullmatch(r"-?\d+", normalized_chat_id):
        return normalized_chat_id
    raise ValueError("chat_id должен быть числом (например -100...) или @username")


def normalize_vk_chat_id(chat_id: str) -> str:
    return normalize_text(chat_id)


def validate_vk_chat_id(chat_id: str) -> str:
    normalized_chat_id = normalize_vk_chat_id(chat_id)
    if not normalized_chat_id:
        raise ValueError("Нужно указать chat_id VK (peer_id)")
    if re.fullmatch(r"-?\d+", normalized_chat_id):
        return normalized_chat_id
    raise ValueError("chat_id VK должен быть числом (peer_id)")


def split_chat_ids(raw_value: str) -> list[str]:
    if raw_value is None:
        return []
    prepared = str(raw_value).replace("\r", "\n")
    chunks = re.split(r"[,\n;]+", prepared)
    return [normalize_text(chunk) for chunk in chunks if normalize_text(chunk)]


def parse_telegram_chat_ids(raw_value: str, *, require_non_empty: bool = False) -> tuple[str, ...]:
    raw_chat_ids = split_chat_ids(raw_value)
    validated: list[str] = []
    seen: set[str] = set()

    for chat_id in raw_chat_ids:
        normalized_chat_id = validate_chat_id(chat_id)
        if normalized_chat_id in seen:
            continue
        validated.append(normalized_chat_id)
        seen.add(normalized_chat_id)

    if require_non_empty and not validated:
        raise ValueError("Нужно указать хотя бы один chat_id Telegram")
    return tuple(validated)


def parse_vk_chat_ids(raw_value: str, *, require_non_empty: bool = False) -> tuple[str, ...]:
    raw_chat_ids = split_chat_ids(raw_value)
    validated: list[str] = []
    seen: set[str] = set()

    for chat_id in raw_chat_ids:
        normalized_chat_id = validate_vk_chat_id(chat_id)
        if normalized_chat_id in seen:
            continue
        validated.append(normalized_chat_id)
        seen.add(normalized_chat_id)

    if require_non_empty and not validated:
        raise ValueError("Нужно указать хотя бы один chat_id VK")
    return tuple(validated)


def compose_delivery_key(source_id: str, match_key: str) -> str:
    return f"{source_id}::{match_key}"


def compose_platform_delivery_key(
    source_id: str,
    match_key: str,
    target_kind: str,
    target_chat_id: str,
) -> str:
    return f"{source_id}::{match_key}::{target_kind}::{target_chat_id}"


def iter_source_delivery_targets(source: ParserSource) -> list[tuple[str, str]]:
    targets: list[tuple[str, str]] = []
    if normalize_chat_id(source.chat_id):
        targets.append(("telegram", source.chat_id))
    for chat_id in source.vk_chat_ids:
        targets.append(("vk", chat_id))
    return targets


def iter_source_match_delivery_targets(
    source: ParserSource,
    *,
    include_blogabet: bool,
) -> list[tuple[str, str]]:
    targets = iter_source_delivery_targets(source)
    if include_blogabet:
        targets.append(("blogabet", "default"))
    return targets


def build_stats_target_key(
    source_id: str,
    stats_kind: str,
    target_kind: str,
    target_chat_id: str,
) -> str:
    return compose_platform_delivery_key(
        source_id,
        f"stats:{stats_kind}",
        target_kind,
        target_chat_id,
    )


def stats_target_label(target_kind: str) -> str:
    if target_kind == "telegram":
        return "Telegram"
    if target_kind == "vk":
        return "VK"
    if target_kind == "blogabet":
        return "Blogabet"
    return target_kind


def build_vk_plain_text_from_html(message_html: str) -> str:
    value = message_html or ""
    if not value:
        return ""

    def _replace_link(match: re.Match[str]) -> str:
        raw_href = html.unescape(match.group(1) or "")
        raw_caption = html.unescape(re.sub(r"<[^>]+>", "", match.group(2) or ""))
        href = normalize_text(raw_href)
        caption = normalize_text(raw_caption)
        if caption and href:
            return f"{caption}: {href}"
        return href or caption

    value = re.sub(
        r"<a\s+[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        _replace_link,
        value,
        flags=re.IGNORECASE | re.DOTALL,
    )
    value = re.sub(r"<[^>]+>", "", value)
    return html.unescape(value)


async def send_stats_message_to_target(
    tg_session: Optional[aiohttp.ClientSession],
    tg_cfg: Optional[TelegramConfig],
    vk_session: Optional[aiohttp.ClientSession],
    vk_cfg: Optional[VkConfig],
    target_kind: str,
    target_chat_id: str,
    message_html: str,
) -> int:
    if target_kind == "telegram":
        if tg_session is None or tg_cfg is None:
            raise RuntimeError("Telegram конфигурация не загружена")
        return await send_telegram_match_message(
            tg_session,
            tg_cfg,
            target_chat_id,
            message_html,
            parse_mode="HTML",
        )
    if target_kind == "vk":
        if vk_session is None or vk_cfg is None:
            raise RuntimeError("VK конфигурация не загружена")
        return await send_vk_match_message(
            vk_session,
            vk_cfg,
            target_chat_id,
            build_vk_plain_text_from_html(message_html),
        )
    raise RuntimeError(f"Неизвестный тип канала доставки: {target_kind}")


def compose_match_signature(
    home_team: str,
    away_team: str,
    tournament: str,
    rate: str,
    rate_description: str,
) -> str:
    return "|".join(
        [
            normalize_text(home_team),
            normalize_text(away_team),
            normalize_text(tournament),
            normalize_text(rate),
            normalize_text(rate_description),
        ]
    )


def build_match_lookup_key(href: str) -> str:
    normalized_href = normalize_text(href)
    if not normalized_href:
        return ""

    parsed = urlparse(normalized_href)
    path = normalize_text(parsed.path).rstrip("/")
    if not path:
        return normalized_href.lower()
    return path.lower()


def classify_settlement_status(net_profit_units: int) -> str:
    if net_profit_units > 0:
        return "win"
    if net_profit_units < 0:
        return "lose"
    return "return"


def settlement_status_icon(status: str) -> str:
    if status == "win":
        return "✅"
    if status == "lose":
        return "✖️"
    return "♻️"


def format_profit_percent(net_profit_units: int) -> str:
    # Базовая ставка 1000 = 1.00%
    return f"{(net_profit_units / 1000):.2f}%"


def build_settlement_footer_line(
    settlement_status: str,
    net_profit_units: int,
    _score: str,
) -> str:
    return (
        f"Прибыль: {format_profit_percent(net_profit_units)} "
        f"{settlement_status_icon(settlement_status)}"
    )


def append_settlement_footer(message_text: str, footer_line: str) -> str:
    base_message = (message_text or "").strip()
    if not base_message:
        base_message = "Ставка"
    normalized_footer = normalize_text(footer_line)
    if not normalized_footer:
        normalized_footer = "Прибыль: 0.00% ♻️"
    return (
        f"{base_message}\n"
        "------------------------------\n"
        f"{normalized_footer}"
    )


class MatchTrackingStore:
    def __init__(self, database_url: str) -> None:
        self.lock = threading.RLock()
        self.database_url = normalize_text(database_url) or DEFAULT_MATCH_DATABASE_URL
        self.backend = ""
        self.connection: Any = None
        self._connect()
        self._initialize_schema()

    def _connect(self) -> None:
        database_url = self.database_url
        lowered = database_url.lower()

        if lowered.startswith(("postgresql://", "postgres://")):
            self._connect_postgres(database_url)
            logger.info("Хранилище матчей подключено: PostgreSQL")
            return

        sqlite_path = self._resolve_sqlite_path(database_url)
        if sqlite_path not in {":memory:", "file::memory:"}:
            Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(
            sqlite_path,
            check_same_thread=False,
            timeout=30,
        )
        self.connection.row_factory = sqlite3.Row
        self.backend = "sqlite"
        logger.info("Хранилище матчей подключено: SQLite (%s)", sqlite_path)

    def _connect_postgres(self, database_url: str) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "Для PostgreSQL нужен пакет psycopg. Установи: pip install psycopg[binary]"
            ) from exc

        self.connection = psycopg.connect(database_url, row_factory=dict_row)
        self.backend = "postgresql"

    def _resolve_sqlite_path(self, database_url: str) -> str:
        normalized = normalize_text(database_url)
        if not normalized:
            normalized = DEFAULT_MATCH_DATABASE_URL

        raw_path = normalized
        if normalized.startswith("sqlite:///"):
            raw_path = normalized[len("sqlite:///"):]
        elif normalized.startswith("sqlite://"):
            raw_path = normalized[len("sqlite://"):]

        raw_path = normalize_text(raw_path)
        if not raw_path:
            raw_path = "parser_matches.db"

        if raw_path in {":memory:", "file::memory:"}:
            return raw_path

        path = Path(raw_path)
        if not path.is_absolute():
            path = Path(__file__).resolve().parent / path
        return str(path)

    def _sql(self, query: str) -> str:
        if self.backend == "postgresql":
            return query.replace("?", "%s")
        return query

    def _initialize_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS tracked_matches (
                source_id TEXT NOT NULL,
                source_url TEXT NOT NULL,
                match_unique_key TEXT NOT NULL,
                match_signature TEXT NOT NULL,
                match_href TEXT NOT NULL,
                match_lookup_key TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                tournament TEXT NOT NULL,
                rate TEXT NOT NULL,
                rate_description TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_active_at TEXT NOT NULL,
                disappeared_at TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                settlement_status TEXT NOT NULL DEFAULT '',
                settlement_profit_units INTEGER NOT NULL DEFAULT 0,
                settlement_score TEXT NOT NULL DEFAULT '',
                settlement_updated_at TEXT NOT NULL DEFAULT '',
                settlement_error TEXT NOT NULL DEFAULT '',
                closed_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_id, match_unique_key)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tracked_match_deliveries (
                delivery_key TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                source_url TEXT NOT NULL,
                match_unique_key TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_chat_id TEXT NOT NULL,
                message_id INTEGER NOT NULL DEFAULT 0,
                message_text TEXT NOT NULL DEFAULT '',
                match_signature TEXT NOT NULL,
                match_href TEXT NOT NULL,
                match_lookup_key TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                delivery_status TEXT NOT NULL DEFAULT 'sent',
                settled INTEGER NOT NULL DEFAULT 0,
                settlement_status TEXT NOT NULL DEFAULT '',
                settlement_profit_units INTEGER NOT NULL DEFAULT 0,
                settlement_score TEXT NOT NULL DEFAULT '',
                settlement_updated_at TEXT NOT NULL DEFAULT '',
                settlement_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_matches_source_status
            ON tracked_matches(source_id, status, last_seen_active_at)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_deliveries_source_state
            ON tracked_match_deliveries(source_id, target_kind, settled, delivery_status)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_deliveries_source_match
            ON tracked_match_deliveries(source_id, match_unique_key)
            """,
        ]

        with self.lock:
            cursor = self.connection.cursor()
            try:
                for statement in statements:
                    cursor.execute(self._sql(statement))
                self.connection.commit()
            except Exception:  # noqa: BLE001
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    def _fetchall(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(self._sql(query), params)
                rows = cursor.fetchall()
            finally:
                cursor.close()

        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, sqlite3.Row):
                normalized_rows.append(dict(row))
            elif isinstance(row, dict):
                normalized_rows.append(row)
            elif hasattr(row, "keys"):
                normalized_rows.append({key: row[key] for key in row.keys()})
            else:
                normalized_rows.append({})
        return normalized_rows

    def _fetchone(self, query: str, params: tuple[Any, ...] = ()) -> Optional[dict[str, Any]]:
        rows = self._fetchall(query, params)
        return rows[0] if rows else None

    def _execute_write(self, query: str, params: tuple[Any, ...] = ()) -> int:
        with self.lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(self._sql(query), params)
                affected = cursor.rowcount or 0
                self.connection.commit()
                return affected
            except Exception:  # noqa: BLE001
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    def register_active_matches(
        self,
        source: ParserSource,
        matches: tuple[ParsedMatch, ...],
        seen_at: str,
    ) -> None:
        if not matches:
            return

        query = """
            INSERT INTO tracked_matches (
                source_id,
                source_url,
                match_unique_key,
                match_signature,
                match_href,
                match_lookup_key,
                home_team,
                away_team,
                tournament,
                rate,
                rate_description,
                first_seen_at,
                last_seen_active_at,
                disappeared_at,
                status,
                settlement_error,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 'active', '', ?, ?)
            ON CONFLICT(source_id, match_unique_key) DO UPDATE SET
                source_url = excluded.source_url,
                match_signature = excluded.match_signature,
                match_href = excluded.match_href,
                match_lookup_key = excluded.match_lookup_key,
                home_team = excluded.home_team,
                away_team = excluded.away_team,
                tournament = excluded.tournament,
                rate = excluded.rate,
                rate_description = excluded.rate_description,
                last_seen_active_at = excluded.last_seen_active_at,
                disappeared_at = CASE
                    WHEN tracked_matches.status = 'settled' THEN tracked_matches.disappeared_at
                    ELSE ''
                END,
                status = CASE
                    WHEN tracked_matches.status = 'settled' THEN tracked_matches.status
                    ELSE 'active'
                END,
                settlement_error = CASE
                    WHEN tracked_matches.status = 'settled' THEN tracked_matches.settlement_error
                    ELSE ''
                END,
                updated_at = excluded.updated_at
        """

        with self.lock:
            cursor = self.connection.cursor()
            try:
                for match in matches:
                    signature = compose_match_signature(
                        match.home_team,
                        match.away_team,
                        match.tournament,
                        match.rate,
                        match.rate_description,
                    )
                    match_href = normalize_text(match.href) or source.url
                    cursor.execute(
                        self._sql(query),
                        (
                            source.source_id,
                            source.url,
                            match.unique_key,
                            signature,
                            match_href,
                            build_match_lookup_key(match_href),
                            match.home_team,
                            match.away_team,
                            match.tournament,
                            match.rate,
                            match.rate_description,
                            seen_at,
                            seen_at,
                            seen_at,
                            seen_at,
                        ),
                    )
                self.connection.commit()
            except Exception:  # noqa: BLE001
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    def mark_disappeared_matches(
        self,
        source_id: str,
        active_match_keys: set[str],
        event_at: str,
    ) -> int:
        if not source_id:
            return 0

        params: list[Any] = [event_at, event_at, source_id]
        exclusion_clause = ""
        if active_match_keys:
            placeholders = ", ".join("?" for _ in active_match_keys)
            exclusion_clause = f" AND match_unique_key NOT IN ({placeholders})"
            params.extend(sorted(active_match_keys))

        query = f"""
            UPDATE tracked_matches
            SET
                status = 'disappeared',
                disappeared_at = CASE
                    WHEN disappeared_at = '' THEN ?
                    ELSE disappeared_at
                END,
                settlement_error = '',
                updated_at = ?
            WHERE source_id = ?
              AND status = 'active'
              {exclusion_clause}
        """
        return self._execute_write(query, tuple(params))

    def delivery_exists(self, delivery_key: str) -> bool:
        row = self._fetchone(
            """
            SELECT 1 AS exists_flag
            FROM tracked_match_deliveries
            WHERE delivery_key = ?
            LIMIT 1
            """,
            (delivery_key,),
        )
        return row is not None

    def upsert_sent_delivery(
        self,
        source: ParserSource,
        match: ParsedMatch,
        target_kind: str,
        target_chat_id: str,
        delivery_key: str,
        message_id: int,
        message_text: str,
        event_at: str,
    ) -> None:
        signature = compose_match_signature(
            match.home_team,
            match.away_team,
            match.tournament,
            match.rate,
            match.rate_description,
        )
        match_href = normalize_text(match.href) or source.url
        match_lookup_key = build_match_lookup_key(match_href)

        query = """
            INSERT INTO tracked_match_deliveries (
                delivery_key,
                source_id,
                source_url,
                match_unique_key,
                target_kind,
                target_chat_id,
                message_id,
                message_text,
                match_signature,
                match_href,
                match_lookup_key,
                home_team,
                away_team,
                delivery_status,
                settled,
                settlement_status,
                settlement_profit_units,
                settlement_score,
                settlement_updated_at,
                settlement_error,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'sent', 0, '', 0, '', '', '', ?, ?)
            ON CONFLICT(delivery_key) DO UPDATE SET
                source_id = excluded.source_id,
                source_url = excluded.source_url,
                match_unique_key = excluded.match_unique_key,
                target_kind = excluded.target_kind,
                target_chat_id = excluded.target_chat_id,
                message_id = excluded.message_id,
                message_text = excluded.message_text,
                match_signature = excluded.match_signature,
                match_href = excluded.match_href,
                match_lookup_key = excluded.match_lookup_key,
                home_team = excluded.home_team,
                away_team = excluded.away_team,
                delivery_status = 'sent',
                settled = 0,
                settlement_status = '',
                settlement_profit_units = 0,
                settlement_score = '',
                settlement_updated_at = '',
                settlement_error = '',
                updated_at = excluded.updated_at
        """
        self._execute_write(
            query,
            (
                delivery_key,
                source.source_id,
                source.url,
                match.unique_key,
                target_kind,
                target_chat_id,
                int(message_id),
                message_text,
                signature,
                match_href,
                match_lookup_key,
                match.home_team,
                match.away_team,
                event_at,
                event_at,
            ),
        )

    def upsert_ignored_delivery(
        self,
        source: ParserSource,
        match: ParsedMatch,
        target_kind: str,
        target_chat_id: str,
        delivery_key: str,
        event_at: str,
    ) -> None:
        signature = compose_match_signature(
            match.home_team,
            match.away_team,
            match.tournament,
            match.rate,
            match.rate_description,
        )
        match_href = normalize_text(match.href) or source.url
        match_lookup_key = build_match_lookup_key(match_href)

        query = """
            INSERT INTO tracked_match_deliveries (
                delivery_key,
                source_id,
                source_url,
                match_unique_key,
                target_kind,
                target_chat_id,
                message_id,
                message_text,
                match_signature,
                match_href,
                match_lookup_key,
                home_team,
                away_team,
                delivery_status,
                settled,
                settlement_status,
                settlement_profit_units,
                settlement_score,
                settlement_updated_at,
                settlement_error,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, '', ?, ?, ?, ?, ?, 'ignored_start', 1, '', 0, '', '', '', ?, ?)
            ON CONFLICT(delivery_key) DO NOTHING
        """
        self._execute_write(
            query,
            (
                delivery_key,
                source.source_id,
                source.url,
                match.unique_key,
                target_kind,
                target_chat_id,
                signature,
                match_href,
                match_lookup_key,
                match.home_team,
                match.away_team,
                event_at,
                event_at,
            ),
        )

    def upsert_failed_delivery(
        self,
        source: ParserSource,
        match: ParsedMatch,
        target_kind: str,
        target_chat_id: str,
        delivery_key: str,
        error_text: str,
        event_at: str,
    ) -> None:
        signature = compose_match_signature(
            match.home_team,
            match.away_team,
            match.tournament,
            match.rate,
            match.rate_description,
        )
        match_href = normalize_text(match.href) or source.url
        match_lookup_key = build_match_lookup_key(match_href)
        normalized_error = normalize_text(error_text)

        query = """
            INSERT INTO tracked_match_deliveries (
                delivery_key,
                source_id,
                source_url,
                match_unique_key,
                target_kind,
                target_chat_id,
                message_id,
                message_text,
                match_signature,
                match_href,
                match_lookup_key,
                home_team,
                away_team,
                delivery_status,
                settled,
                settlement_status,
                settlement_profit_units,
                settlement_score,
                settlement_updated_at,
                settlement_error,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 'failed', 1, '', 0, '', '', ?, ?, ?)
            ON CONFLICT(delivery_key) DO UPDATE SET
                source_id = excluded.source_id,
                source_url = excluded.source_url,
                match_unique_key = excluded.match_unique_key,
                target_kind = excluded.target_kind,
                target_chat_id = excluded.target_chat_id,
                message_id = 0,
                message_text = excluded.message_text,
                match_signature = excluded.match_signature,
                match_href = excluded.match_href,
                match_lookup_key = excluded.match_lookup_key,
                home_team = excluded.home_team,
                away_team = excluded.away_team,
                delivery_status = 'failed',
                settled = 1,
                settlement_status = '',
                settlement_profit_units = 0,
                settlement_score = '',
                settlement_updated_at = '',
                settlement_error = excluded.settlement_error,
                updated_at = excluded.updated_at
        """
        self._execute_write(
            query,
            (
                delivery_key,
                source.source_id,
                source.url,
                match.unique_key,
                target_kind,
                target_chat_id,
                normalized_error,
                signature,
                match_href,
                match_lookup_key,
                match.home_team,
                match.away_team,
                normalized_error,
                event_at,
                event_at,
            ),
        )

    def list_pending_settlement_candidates(
        self,
        source_id: str,
        limit: int = MAX_PENDING_SETTLEMENT_CANDIDATES,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, int(limit))
        return self._fetchall(
            """
            SELECT
                d.delivery_key,
                d.source_id,
                d.source_url,
                d.target_chat_id AS chat_id,
                d.message_id,
                d.message_text,
                d.match_unique_key,
                d.match_signature,
                d.match_href,
                d.match_lookup_key,
                d.home_team,
                d.away_team,
                m.status AS tracked_status,
                m.settlement_status AS tracked_settlement_status,
                m.settlement_profit_units AS tracked_settlement_profit_units,
                m.settlement_score AS tracked_settlement_score
            FROM tracked_match_deliveries AS d
            INNER JOIN tracked_matches AS m
                ON m.source_id = d.source_id
               AND m.match_unique_key = d.match_unique_key
            WHERE d.source_id = ?
              AND d.target_kind = 'telegram'
              AND d.delivery_status = 'sent'
              AND d.settled = 0
              AND d.message_id > 0
              AND m.status IN ('disappeared', 'settled')
            ORDER BY
                m.disappeared_at ASC,
                m.last_seen_active_at ASC,
                d.created_at ASC
            LIMIT ?
            """,
            (source_id, safe_limit),
        )

    def has_disappeared_matches(self, source_id: str) -> bool:
        row = self._fetchone(
            """
            SELECT COUNT(*) AS row_count
            FROM tracked_matches
            WHERE source_id = ?
              AND status = 'disappeared'
            """,
            (source_id,),
        )
        if row is None:
            return False
        try:
            return int(row.get("row_count", 0)) > 0
        except (TypeError, ValueError):
            return False

    def reconcile_disappeared_matches(
        self,
        source_id: str,
        settled_matches: list[SettledMatchSnapshot],
        event_at: str,
    ) -> int:
        if not source_id or not settled_matches:
            return 0

        disappeared_rows = self._fetchall(
            """
            SELECT
                match_unique_key,
                match_signature,
                match_lookup_key
            FROM tracked_matches
            WHERE source_id = ?
              AND status = 'disappeared'
            """,
            (source_id,),
        )
        if not disappeared_rows:
            return 0

        by_lookup_key, by_unique_key, by_signature = build_settlement_lookup_indexes(
            settled_matches
        )
        updates: list[tuple[str, SettledMatchSnapshot]] = []
        for row in disappeared_rows:
            match_unique_key = normalize_text(str(row.get("match_unique_key", "")))
            match_signature = normalize_text(str(row.get("match_signature", "")))
            match_lookup_key = normalize_text(str(row.get("match_lookup_key", "")))
            if not match_unique_key:
                continue

            settled_match: Optional[SettledMatchSnapshot] = None
            if match_lookup_key:
                settled_match = by_lookup_key.get(match_lookup_key)
            if settled_match is None:
                settled_match = by_unique_key.get(match_unique_key)
            if settled_match is None and match_signature:
                settled_match = by_signature.get(match_signature)
            if settled_match is None:
                continue
            updates.append((match_unique_key, settled_match))

        if not updates:
            return 0

        with self.lock:
            cursor = self.connection.cursor()
            updated_count = 0
            try:
                query = self._sql(
                    """
                    UPDATE tracked_matches
                    SET
                        status = 'settled',
                        settlement_status = ?,
                        settlement_profit_units = ?,
                        settlement_score = ?,
                        settlement_updated_at = ?,
                        settlement_error = '',
                        closed_at = ?,
                        updated_at = ?
                    WHERE source_id = ?
                      AND match_unique_key = ?
                      AND status <> 'settled'
                    """
                )
                for match_unique_key, settled_match in updates:
                    cursor.execute(
                        query,
                        (
                            settled_match.settlement_status,
                            settled_match.net_profit_units,
                            settled_match.score,
                            event_at,
                            event_at,
                            event_at,
                            source_id,
                            match_unique_key,
                        ),
                    )
                    updated_count += max(cursor.rowcount or 0, 0)
                self.connection.commit()
                return updated_count
            except Exception:  # noqa: BLE001
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    def fetch_status_counters(self) -> dict[str, int]:
        counters = {
            "active": 0,
            "disappeared": 0,
            "settled": 0,
            "pending_settlement": 0,
            "total_matches": 0,
        }
        status_rows = self._fetchall(
            """
            SELECT status, COUNT(*) AS row_count
            FROM tracked_matches
            GROUP BY status
            """
        )
        for row in status_rows:
            status_name = normalize_text(str(row.get("status", ""))).lower()
            if status_name not in counters:
                continue
            try:
                counters[status_name] = int(row.get("row_count", 0))
            except (TypeError, ValueError):
                counters[status_name] = 0

        total_row = self._fetchone("SELECT COUNT(*) AS row_count FROM tracked_matches")
        if total_row is not None:
            try:
                counters["total_matches"] = int(total_row.get("row_count", 0))
            except (TypeError, ValueError):
                counters["total_matches"] = 0

        pending_row = self._fetchone(
            """
            SELECT COUNT(*) AS row_count
            FROM tracked_match_deliveries AS d
            INNER JOIN tracked_matches AS m
                ON m.source_id = d.source_id
               AND m.match_unique_key = d.match_unique_key
            WHERE d.target_kind = 'telegram'
              AND d.delivery_status = 'sent'
              AND d.settled = 0
              AND m.status IN ('disappeared', 'settled')
            """
        )
        if pending_row is not None:
            try:
                counters["pending_settlement"] = int(pending_row.get("row_count", 0))
            except (TypeError, ValueError):
                counters["pending_settlement"] = 0

        return counters

    def clear_runtime_data(self) -> None:
        with self.lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(self._sql("DELETE FROM tracked_match_deliveries"))
                cursor.execute(self._sql("DELETE FROM tracked_matches"))
                self.connection.commit()
            except Exception:  # noqa: BLE001
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    def mark_settlement_success(
        self,
        record: SentMatchRecord,
        settled_match: SettledMatchSnapshot,
        updated_message: str,
        event_at: str,
    ) -> None:
        with self.lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    self._sql(
                        """
                        UPDATE tracked_match_deliveries
                        SET
                            message_text = ?,
                            delivery_status = 'settled',
                            settled = 1,
                            settlement_status = ?,
                            settlement_profit_units = ?,
                            settlement_score = ?,
                            settlement_updated_at = ?,
                            settlement_error = '',
                            updated_at = ?
                        WHERE delivery_key = ?
                        """
                    ),
                    (
                        updated_message,
                        settled_match.settlement_status,
                        settled_match.net_profit_units,
                        settled_match.score,
                        event_at,
                        event_at,
                        record.delivery_key,
                    ),
                )
                cursor.execute(
                    self._sql(
                        """
                        UPDATE tracked_matches
                        SET
                            status = 'settled',
                            settlement_status = ?,
                            settlement_profit_units = ?,
                            settlement_score = ?,
                            settlement_updated_at = ?,
                            settlement_error = '',
                            closed_at = ?,
                            updated_at = ?
                        WHERE source_id = ?
                          AND match_unique_key = ?
                        """
                    ),
                    (
                        settled_match.settlement_status,
                        settled_match.net_profit_units,
                        settled_match.score,
                        event_at,
                        event_at,
                        event_at,
                        record.source_id,
                        record.match_unique_key,
                    ),
                )
                self.connection.commit()
            except Exception:  # noqa: BLE001
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    def mark_settlement_error(
        self,
        record: SentMatchRecord,
        error_message: str,
        event_at: str,
    ) -> None:
        with self.lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    self._sql(
                        """
                        UPDATE tracked_match_deliveries
                        SET
                            settlement_error = ?,
                            updated_at = ?
                        WHERE delivery_key = ?
                        """
                    ),
                    (
                        error_message,
                        event_at,
                        record.delivery_key,
                    ),
                )
                cursor.execute(
                    self._sql(
                        """
                        UPDATE tracked_matches
                        SET
                            settlement_error = ?,
                            updated_at = ?
                        WHERE source_id = ?
                          AND match_unique_key = ?
                          AND status <> 'settled'
                        """
                    ),
                    (
                        error_message,
                        event_at,
                        record.source_id,
                        record.match_unique_key,
                    ),
                )
                self.connection.commit()
            except Exception:  # noqa: BLE001
                self.connection.rollback()
                raise
            finally:
                cursor.close()


_match_store: Optional[MatchTrackingStore] = None
_match_store_lock = threading.RLock()


def resolve_match_database_url() -> str:
    load_dotenv()
    configured = normalize_text(os.getenv("MATCH_DATABASE_URL", ""))
    return configured or DEFAULT_MATCH_DATABASE_URL


def get_match_store() -> MatchTrackingStore:
    global _match_store
    with _match_store_lock:
        if _match_store is None:
            _match_store = MatchTrackingStore(resolve_match_database_url())
        return _match_store


def parser_sources_storage_path() -> Path:
    return Path(__file__).resolve().parent / PARSER_SOURCES_STORAGE_FILENAME


def clone_parser_sources(sources: list[ParserSource]) -> list[ParserSource]:
    return [
        ParserSource(
            source_id=source.source_id,
            url=source.url,
            chat_id=source.chat_id,
            vk_chat_ids=tuple(source.vk_chat_ids),
            enabled=source.enabled,
        )
        for source in sources
    ]


def write_parser_sources_to_storage(sources: list[ParserSource]) -> None:
    path = parser_sources_storage_path()
    payload = [
        {
            "source_id": source.source_id,
            "url": source.url,
            "chat_id": source.chat_id,
            "vk_chat_ids": list(source.vk_chat_ids),
            "vk_chat_id": source.vk_chat_ids[0] if source.vk_chat_ids else "",
            "enabled": source.enabled,
        }
        for source in sources
    ]
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temp_path.replace(path)


def persist_parser_sources_snapshot(sources: list[ParserSource]) -> None:
    write_parser_sources_to_storage(clone_parser_sources(sources))


def load_parser_sources_from_storage() -> list[ParserSource]:
    path = parser_sources_storage_path()
    if not path.exists():
        return []

    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Не удалось прочитать %s: %s", path, exc)
        return []

    if not isinstance(raw_payload, list):
        logger.warning("Некорректный формат %s: ожидается список", path)
        return []

    loaded_sources: list[ParserSource] = []
    seen_urls: set[str] = set()
    for item in raw_payload:
        if not isinstance(item, dict):
            continue

        source_url = normalize_source_url(str(item.get("url", "")))
        if not source_url or not source_url.startswith(("http://", "https://")):
            continue
        if source_url in seen_urls:
            continue

        chat_id_raw = normalize_chat_id(str(item.get("chat_id", "")))
        if chat_id_raw:
            try:
                chat_id_raw = validate_chat_id(chat_id_raw)
            except Exception:  # noqa: BLE001
                logger.warning("Пропущен источник с некорректным chat_id: %s", source_url)
                continue

        raw_vk_chat_ids = item.get("vk_chat_ids", item.get("vk_chat_id", ""))
        if isinstance(raw_vk_chat_ids, list):
            vk_input = "\n".join(str(value) for value in raw_vk_chat_ids)
        else:
            vk_input = str(raw_vk_chat_ids or "")
        try:
            vk_chat_ids = parse_vk_chat_ids(vk_input, require_non_empty=False)
        except Exception:  # noqa: BLE001
            logger.warning("Пропущен источник с некорректными VK chat_id: %s", source_url)
            continue

        source_enabled = bool(item.get("enabled", True))
        source_id = str(len(loaded_sources) + 1)
        loaded_sources.append(
            ParserSource(
                source_id=source_id,
                url=source_url,
                chat_id=chat_id_raw,
                vk_chat_ids=vk_chat_ids,
                enabled=source_enabled,
            )
        )
        seen_urls.add(source_url)

    return loaded_sources


def ensure_parser_runtime_defaults(cfg: TargetConfig) -> None:
    with state.lock:
        if not state.parser_interval_initialized:
            state.parser_interval_seconds = max(cfg.parser_interval_seconds, 10)
            state.parser_interval_initialized = True

        if not state.parser_page_max_age_initialized:
            state.parser_page_max_age_seconds = max(cfg.parser_page_max_age_seconds, 10)
            state.parser_page_max_age_initialized = True

        if not state.parser_sources:
            persisted_sources = load_parser_sources_from_storage()
            if persisted_sources:
                state.parser_sources = persisted_sources
                state.parser_source_seq = len(persisted_sources)


def add_parser_source(url: str, chat_id: str, vk_chat_ids_raw: str = "") -> tuple[bool, ParserSource]:
    normalized_url = normalize_source_url(url)
    if not normalized_url:
        raise ValueError("Ссылка не задана")
    if not normalized_url.startswith(("http://", "https://")):
        raise ValueError("Ссылка должна начинаться с http:// или https://")
    normalized_chat_id = validate_chat_id(chat_id)
    vk_chat_ids = parse_vk_chat_ids(vk_chat_ids_raw, require_non_empty=False)
    is_added = False

    with state.lock:
        for source in state.parser_sources:
            if normalize_source_url(source.url) == normalized_url:
                source.chat_id = normalized_chat_id
                source.vk_chat_ids = vk_chat_ids
                snapshot = clone_parser_sources(state.parser_sources)
                updated_source = ParserSource(
                    source_id=source.source_id,
                    url=source.url,
                    chat_id=source.chat_id,
                    vk_chat_ids=source.vk_chat_ids,
                    enabled=source.enabled,
                )
                break
        else:
            state.parser_source_seq += 1
            source = ParserSource(
                source_id=str(state.parser_source_seq),
                url=normalized_url,
                chat_id=normalized_chat_id,
                vk_chat_ids=vk_chat_ids,
                enabled=True,
            )
            state.parser_sources.append(source)
            snapshot = clone_parser_sources(state.parser_sources)
            updated_source = source
            is_added = True

    persist_parser_sources_snapshot(snapshot)
    return is_added, updated_source


def toggle_parser_source(source_id: str) -> ParserSource:
    with state.lock:
        for source in state.parser_sources:
            if source.source_id != source_id:
                continue
            source.enabled = not source.enabled
            snapshot = clone_parser_sources(state.parser_sources)
            toggled = ParserSource(
                source_id=source.source_id,
                url=source.url,
                chat_id=source.chat_id,
                vk_chat_ids=source.vk_chat_ids,
                enabled=source.enabled,
            )
            break
        else:
            raise ValueError("Ссылка не найдена")

    persist_parser_sources_snapshot(snapshot)
    return toggled


def remove_parser_source(source_id: str) -> ParserSource:
    with state.lock:
        for idx, source in enumerate(state.parser_sources):
            if source.source_id != source_id:
                continue
            removed = state.parser_sources.pop(idx)
            snapshot = clone_parser_sources(state.parser_sources)
            break
        else:
            raise ValueError("Ссылка не найдена")

    persist_parser_sources_snapshot(snapshot)
    return removed


def update_parser_source_chat_id(source_id: str, chat_id: str) -> ParserSource:
    normalized_chat_id = validate_chat_id(chat_id)

    with state.lock:
        for source in state.parser_sources:
            if source.source_id != source_id:
                continue
            source.chat_id = normalized_chat_id
            snapshot = clone_parser_sources(state.parser_sources)
            updated = ParserSource(
                source_id=source.source_id,
                url=source.url,
                chat_id=source.chat_id,
                vk_chat_ids=source.vk_chat_ids,
                enabled=source.enabled,
            )
            break
        else:
            raise ValueError("Ссылка не найдена")

    persist_parser_sources_snapshot(snapshot)
    return updated


def update_parser_source_vk_chat_ids(source_id: str, vk_chat_ids_raw: str) -> ParserSource:
    vk_chat_ids = parse_vk_chat_ids(vk_chat_ids_raw, require_non_empty=False)

    with state.lock:
        for source in state.parser_sources:
            if source.source_id != source_id:
                continue
            source.vk_chat_ids = vk_chat_ids
            snapshot = clone_parser_sources(state.parser_sources)
            updated = ParserSource(
                source_id=source.source_id,
                url=source.url,
                chat_id=source.chat_id,
                vk_chat_ids=source.vk_chat_ids,
                enabled=source.enabled,
            )
            break
        else:
            raise ValueError("Ссылка не найдена")

    persist_parser_sources_snapshot(snapshot)
    return updated


def load_target_config() -> TargetConfig:
    load_dotenv()

    interval_raw = os.getenv(
        "PARSER_INTERVAL_SECONDS",
        str(DEFAULT_PARSER_INTERVAL_SECONDS),
    ).strip()
    parser_interval_seconds = parse_interval_seconds(interval_raw, clamp_min=True)
    page_max_age_raw = os.getenv(
        "PARSER_PAGE_MAX_AGE_SECONDS",
        str(DEFAULT_PARSER_PAGE_MAX_AGE_SECONDS),
    ).strip()
    parser_page_max_age_seconds = parse_min_seconds_value(
        page_max_age_raw,
        field_label="Интервал пересоздания страниц парсера",
        minimum_seconds=10,
    )
    daily_stats_send_hour_raw = os.getenv(
        "DAILY_STATS_SEND_HOUR_MSK",
        str(DEFAULT_DAILY_STATS_SEND_HOUR_MSK),
    ).strip()
    daily_stats_send_hour_msk = parse_hour_value(
        daily_stats_send_hour_raw,
        field_label="Час авторассылки суточной статистики (МСК)",
    )
    weekly_stats_send_hour_raw = os.getenv(
        "WEEKLY_STATS_SEND_HOUR_MSK",
        str(DEFAULT_WEEKLY_STATS_SEND_HOUR_MSK),
    ).strip()
    weekly_stats_send_hour_msk = parse_hour_value(
        weekly_stats_send_hour_raw,
        field_label="Час авторассылки недельной статистики (МСК)",
    )
    monthly_stats_send_hour_raw = os.getenv(
        "MONTHLY_STATS_SEND_HOUR_MSK",
        str(DEFAULT_MONTHLY_STATS_SEND_HOUR_MSK),
    ).strip()
    monthly_stats_send_hour_msk = parse_hour_value(
        monthly_stats_send_hour_raw,
        field_label="Час авторассылки месячной статистики (МСК)",
    )

    cfg = TargetConfig(
        login_url=os.getenv("TARGET_LOGIN_URL", "").strip(),
        data_url=os.getenv("TARGET_DATA_URL",
                           DEFAULT_PARSER_URL).strip() or DEFAULT_PARSER_URL,
        open_login_selector=os.getenv(
            "TARGET_OPEN_LOGIN_SELECTOR", "").strip(),
        login_username=os.getenv("TARGET_LOGIN_USERNAME", "").strip(),
        email_selector=os.getenv(
            "TARGET_EMAIL_SELECTOR", "#loginform-username").strip(),
        password_selector=os.getenv(
            "TARGET_PASSWORD_SELECTOR", "#loginform-password").strip(),
        submit_selector=os.getenv(
            "TARGET_SUBMIT_SELECTOR",
            "#login-form button[type='submit']",
        ).strip(),
        code_selector=os.getenv("TARGET_CODE_SELECTOR",
                                "input[name*='code']").strip(),
        code_submit_selector=os.getenv(
            "TARGET_CODE_SUBMIT_SELECTOR",
            "button[type='submit']",
        ).strip(),
        parse_item_selector=os.getenv(
            "TARGET_PARSE_ITEM_SELECTOR",
            DEFAULT_PARSE_ITEM_SELECTOR,
        ).strip(),
        panel_container_selector=os.getenv(
            "TARGET_PANEL_CONTAINER_SELECTOR",
            DEFAULT_PANEL_CONTAINER_SELECTOR,
        ).strip(),
        login_form_selector=os.getenv(
            "TARGET_LOGIN_FORM_SELECTOR", "#login-form").strip(),
        login_error_selector=os.getenv(
            "TARGET_LOGIN_ERROR_SELECTOR",
            "#login-form .help-block",
        ).strip(),
        parser_interval_seconds=parser_interval_seconds,
        parser_page_max_age_seconds=parser_page_max_age_seconds,
        parser_send_existing_on_start=parse_bool_env(
            os.getenv("PARSER_SEND_EXISTING_ON_START", "1"),
            default=True,
        ),
        daily_stats_send_hour_msk=daily_stats_send_hour_msk,
        weekly_stats_send_hour_msk=weekly_stats_send_hour_msk,
        monthly_stats_send_hour_msk=monthly_stats_send_hour_msk,
        headless=parse_bool_env(
            os.getenv("TARGET_HEADLESS", "0"), default=False),
    )

    required = [
        ("TARGET_LOGIN_URL", cfg.login_url),
        ("TARGET_PASSWORD_SELECTOR", cfg.password_selector),
        ("TARGET_SUBMIT_SELECTOR", cfg.submit_selector),
        ("TARGET_CODE_SELECTOR", cfg.code_selector),
        ("TARGET_CODE_SUBMIT_SELECTOR", cfg.code_submit_selector),
    ]
    missing = [name for name, value in required if not value]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Не заполнены переменные в .env: {joined}")

    return cfg


def load_telegram_config() -> TelegramConfig:
    load_dotenv()

    bot_token = normalize_text(os.getenv("TELEGRAM_BOT_TOKEN", ""))
    if not bot_token:
        raise ValueError("Не задан TELEGRAM_BOT_TOKEN в .env")

    timeout_raw = normalize_text(
        os.getenv(
            "TELEGRAM_REQUEST_TIMEOUT_SECONDS",
            str(DEFAULT_TELEGRAM_REQUEST_TIMEOUT_SECONDS),
        )
    )
    try:
        request_timeout_seconds = int(timeout_raw)
    except ValueError as exc:
        raise ValueError("TELEGRAM_REQUEST_TIMEOUT_SECONDS должен быть числом") from exc
    if request_timeout_seconds <= 0:
        raise ValueError("TELEGRAM_REQUEST_TIMEOUT_SECONDS должен быть больше 0")
    use_system_proxy = parse_bool_env(
        os.getenv("TELEGRAM_USE_SYSTEM_PROXY", "0"),
        default=False,
    )

    return TelegramConfig(
        bot_token=bot_token,
        request_timeout_seconds=request_timeout_seconds,
        use_system_proxy=use_system_proxy,
    )


def load_vk_config() -> VkConfig:
    load_dotenv()

    user_token = normalize_text(os.getenv("VK_USER_TOKEN", ""))
    if not user_token:
        raise ValueError("Не задан VK_USER_TOKEN в .env")

    timeout_raw = normalize_text(
        os.getenv(
            "VK_REQUEST_TIMEOUT_SECONDS",
            str(DEFAULT_VK_REQUEST_TIMEOUT_SECONDS),
        )
    )
    try:
        request_timeout_seconds = int(timeout_raw)
    except ValueError as exc:
        raise ValueError("VK_REQUEST_TIMEOUT_SECONDS должен быть числом") from exc
    if request_timeout_seconds <= 0:
        raise ValueError("VK_REQUEST_TIMEOUT_SECONDS должен быть больше 0")

    api_version = normalize_text(os.getenv("VK_API_VERSION", DEFAULT_VK_API_VERSION))
    if not api_version:
        api_version = DEFAULT_VK_API_VERSION

    use_system_proxy = parse_bool_env(
        os.getenv("VK_USE_SYSTEM_PROXY", "0"),
        default=False,
    )

    return VkConfig(
        user_token=user_token,
        api_version=api_version,
        request_timeout_seconds=request_timeout_seconds,
        use_system_proxy=use_system_proxy,
    )


def resolve_local_path(raw_path: str, default_path: str) -> str:
    value = normalize_text(raw_path) or default_path
    path = Path(value)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    return str(path)


def load_blogabet_config() -> BlogabetConfig:
    load_dotenv()

    enabled = parse_bool_env(os.getenv("BLOGABET_ENABLED", "0"), default=False)
    storage_state_path = resolve_local_path(
        os.getenv("BLOGABET_STORAGE_STATE_PATH", DEFAULT_BLOGABET_STORAGE_STATE_PATH),
        DEFAULT_BLOGABET_STORAGE_STATE_PATH,
    )
    league_aliases_path = resolve_local_path(
        os.getenv("BLOGABET_LEAGUE_ALIASES_PATH", DEFAULT_BLOGABET_LEAGUE_ALIASES_PATH),
        DEFAULT_BLOGABET_LEAGUE_ALIASES_PATH,
    )
    headless = parse_bool_env(os.getenv("BLOGABET_HEADLESS", "1"), default=True)

    stake_raw = normalize_text(os.getenv("BLOGABET_DEFAULT_STAKE", str(DEFAULT_BLOGABET_STAKE)))
    try:
        default_stake = int(stake_raw)
    except ValueError as exc:
        raise ValueError("BLOGABET_DEFAULT_STAKE должен быть целым числом") from exc
    if default_stake < 1 or default_stake > 10:
        raise ValueError("BLOGABET_DEFAULT_STAKE должен быть в диапазоне 1..10")

    interactive_timeout_raw = normalize_text(
        os.getenv("BLOGABET_INTERACTIVE_LOGIN_TIMEOUT_SECONDS", "600")
    )
    try:
        interactive_login_timeout_seconds = int(interactive_timeout_raw)
    except ValueError as exc:
        raise ValueError(
            "BLOGABET_INTERACTIVE_LOGIN_TIMEOUT_SECONDS должен быть целым числом"
        ) from exc
    if interactive_login_timeout_seconds < 60:
        interactive_login_timeout_seconds = 60

    return BlogabetConfig(
        enabled=enabled,
        storage_state_path=storage_state_path,
        headless=headless,
        default_stake=default_stake,
        admin_tg_chat_id=normalize_text(os.getenv("BLOGABET_ADMIN_TG_CHAT_ID", "")),
        league_aliases_path=league_aliases_path,
        upcoming_url=normalize_text(
            os.getenv("BLOGABET_UPCOMING_URL", "https://blogabet.com/pinnacle/live")
        ) or "https://blogabet.com/pinnacle/live",
        login_url=normalize_text(os.getenv("BLOGABET_LOGIN_URL", "https://blogabet.com"))
        or "https://blogabet.com",
        interactive_login_timeout_seconds=interactive_login_timeout_seconds,
        login_email=(os.getenv("BLOGABET_LOGIN_EMAIL", "") or "").strip(),
        login_password=os.getenv("BLOGABET_LOGIN_PASSWORD", "") or "",
    )


def load_ocr_client() -> OcrSpaceClient:
    load_dotenv()

    api_key = normalize_text(os.getenv("OCR_SPACE_API_KEY", ""))
    if not api_key:
        raise ValueError("Не задан OCR_SPACE_API_KEY в .env")

    timeout_raw = normalize_text(os.getenv("OCR_SPACE_TIMEOUT_SECONDS", "30"))
    try:
        timeout_seconds = int(timeout_raw)
    except ValueError as exc:
        raise ValueError("OCR_SPACE_TIMEOUT_SECONDS должен быть целым числом") from exc
    if timeout_seconds <= 0:
        raise ValueError("OCR_SPACE_TIMEOUT_SECONDS должен быть больше 0")

    return OcrSpaceClient(
        api_key=api_key,
        cache_path=normalize_text(os.getenv("OCR_CACHE_PATH", "./ocr_cache.json"))
        or "./ocr_cache.json",
        request_timeout_seconds=timeout_seconds,
        use_system_proxy=parse_bool_env(
            os.getenv("OCR_USE_SYSTEM_PROXY", "0"),
            default=False,
        ),
    )


def build_active_match_message(match: ParsedMatch, source_url: str) -> str:
    sport_emoji, sport_label = detect_match_sport(match)
    tournament = match.tournament or "Турнир не указан"
    teams = f"{match.home_team} - {match.away_team}"
    event_time = match.event_time or "Не указано"
    score = match.score or "Не указан"
    rate = match.rate or "Не указан"
    match_url = match.href or source_url

    return (
        f"{sport_emoji} {sport_label}\n"
        f"💡 Лайв по {sport_label.lower()}\n"
        f"🏳️ {tournament}\n"
        f"⚔️ {teams}\n"
        f"⏱️ Время: {event_time}\n"
        f"🥅 Счет: {score}\n"
        "------------------------------\n"
        f"📈 Коэффициент: {rate}\n"
        f"🔗 Ссылка на матч: {match_url}"
    )


def build_active_match_message_html(match: ParsedMatch, source_url: str) -> str:
    sport_emoji, sport_label = detect_match_sport(match)
    tournament = match.tournament or "Турнир не указан"
    teams = f"{match.home_team} - {match.away_team}"
    event_time = match.event_time or "Не указано"
    score = match.score or "Не указан"
    rate = match.rate or "Не указан"
    match_url = normalize_text(match.href) or normalize_text(source_url)

    if match_url:
        escaped_url = html.escape(match_url, quote=True)
        link_line = f'🔗 <a href="{escaped_url}">Ссылка на матч</a>'
    else:
        link_line = "🔗 Ссылка на матч"

    return (
        f"{sport_emoji} {html.escape(sport_label, quote=False)}\n"
        f"💡 Лайв по {html.escape(sport_label.lower(), quote=False)}\n"
        f"🏳️ {html.escape(tournament, quote=False)}\n"
        f"⚔️ {html.escape(teams, quote=False)}\n"
        f"⏱️ Время: {html.escape(event_time, quote=False)}\n"
        f"🥅 Счет: {html.escape(score, quote=False)}\n"
        "------------------------------\n"
        f"📈 Коэффициент: {html.escape(rate, quote=False)}\n"
        f"{link_line}"
    )


def build_blogabet_analysis_text(
    match: ParsedMatch,
    bet_intent: BetIntent,
    ocr_text: str,
) -> str:
    lines = [
        f"Match: {match.home_team} - {match.away_team}",
        f"Tournament: {match.tournament}",
        f"Market: {bet_intent.market}",
        f"Period: {bet_intent.period}",
        f"Side: {bet_intent.side}",
        f"Line: {bet_intent.line if bet_intent.line is not None else '-'}",
        f"Metric: {bet_intent.metric}",
    ]
    raw = normalize_text(ocr_text)
    if raw:
        lines.append(f"OCR: {raw[:320]}")
    return "\n".join(lines)


def build_blogabet_admin_alert_message(
    match: ParsedMatch,
    bet_raw_text: str,
    error_text: str,
) -> str:
    tournament = normalize_text(match.tournament) or "-"
    teams = f"{normalize_text(match.home_team)} - {normalize_text(match.away_team)}"
    bet_text = normalize_text(bet_raw_text) or "-"
    match_link = normalize_text(match.href) or "-"
    reason = normalize_text(error_text) or "-"
    return (
        "⚠️ Не опубликовано в Blogabet\n"
        f"Турнир: {tournament}\n"
        f"Матч: {teams}\n"
        f"Ставка: {bet_text}\n"
        f"Ссылка: {match_link}\n"
        f"Причина: {reason}"
    )


def detect_match_sport(match: ParsedMatch) -> tuple[str, str]:
    text_parts = [
        match.tournament,
        match.home_team,
        match.away_team,
        match.rate_description,
        match.href,
    ]
    normalized_text_parts = [normalize_text(value).lower() for value in text_parts]
    search_blob = " ".join(part for part in normalized_text_parts if part)

    basketball_markers = (
        "баскет",
        "basket",
        "nba",
        "wnba",
        "euroleague",
        "euroliga",
        "евролига",
    )
    if any(marker in search_blob for marker in basketball_markers):
        return "🏀", "Баскетбол"

    return "⚽️", "Футбол"


def dispatch_title_from_url(source_url: str) -> str:
    normalized = normalize_source_url(source_url)
    parsed = urlparse(normalized)
    path_parts = [unquote(part) for part in parsed.path.split("/") if part]

    title_candidate = ""
    for part in reversed(path_parts):
        normalized_part = normalize_text(part)
        if not normalized_part or normalized_part.lower() == "dispatch":
            continue
        if re.fullmatch(r"id\d+", normalized_part.lower()):
            continue
        title_candidate = normalized_part
        break

    if not title_candidate:
        title_candidate = normalized

    title_candidate = re.sub(r"[-_]+", " ", title_candidate)
    title_candidate = normalize_text(title_candidate)
    if not title_candidate:
        return "БЕЗ НАЗВАНИЯ"
    return title_candidate.upper()


def parse_int_from_text(value: str) -> int:
    digits = re.sub(r"[^\d-]", "", normalize_text(value))
    if not digits:
        return 0
    try:
        return int(digits)
    except ValueError:
        return 0


def parse_int_from_text_strict(value: str) -> Optional[int]:
    normalized = normalize_text(value)
    if not normalized:
        return None

    numeric_match = re.search(r"[+-]?\d[\d\s]*", normalized)
    if numeric_match is None:
        return None

    digits = re.sub(r"[^\d-]", "", numeric_match.group(0))
    if not digits or digits == "-":
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def extract_settlement_outcome(
    result_text: str,
    result_class: str,
) -> Optional[tuple[int, str]]:
    normalized_text = normalize_text(result_text)
    normalized_class = normalize_text(result_class).lower()

    parsed_units = parse_int_from_text_strict(normalized_text)

    if "return" in normalized_class:
        return 0, "return"

    if "lose" in normalized_class:
        if parsed_units is None:
            return None
        return (-abs(parsed_units), "lose")

    if "win" in normalized_class:
        if parsed_units is None:
            return None
        return (abs(parsed_units), "win")

    if parsed_units is None:
        return None
    if parsed_units > 0:
        return parsed_units, "win"
    if parsed_units < 0:
        return parsed_units, "lose"

    # Нулевой исход принимаем только при явно указанном 0 в тексте.
    if re.search(r"\b0+\b", normalized_text):
        return 0, "return"
    return None


def normalize_percent_value(value: str) -> str:
    normalized = normalize_text(value).replace(",", ".")
    if not normalized:
        return "0.00%"
    if normalized.endswith("%"):
        return normalized
    return f"{normalized}%"


def parse_percent_number(value: str) -> float:
    normalized = normalize_text(value).replace(",", ".").replace("%", "")
    if not normalized:
        return 0.0
    match = re.search(r"[+-]?\d+(?:\.\d+)?", normalized)
    if match is None:
        return 0.0
    try:
        return float(match.group(0))
    except ValueError:
        return 0.0


def format_percent_value(value: float, *, with_sign: bool = False) -> str:
    if with_sign:
        return f"{value:+.2f}%"
    return f"{value:.2f}%"


def parse_stats_date(value: str) -> Optional[date]:
    normalized = normalize_text(value)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%d.%m.%Y").date()
    except ValueError:
        return None


def parse_stats_month_label(value: str) -> Optional[tuple[int, int]]:
    normalized = normalize_text(value).lower().replace(".", "")
    if not normalized:
        return None

    year_match = re.search(r"\b(\d{4})\b", normalized)
    if year_match is None:
        return None

    month_value: Optional[int] = None
    for token in normalized.split():
        month_value = RUS_MONTH_ALIASES.get(token)
        if month_value is not None:
            break

    if month_value is None:
        return None

    return int(year_match.group(1)), month_value


def settlement_status_by_percent(percent_value: float) -> str:
    if percent_value > 0:
        return "win"
    if percent_value < 0:
        return "lose"
    return "return"


def escape_html_text(value: str) -> str:
    return html.escape(value or "", quote=False)


def build_verifier_link_html(verifier_url: str) -> str:
    normalized = normalize_text(verifier_url)
    if not normalized:
        return "Ссылка на верификатор"
    escaped_url = html.escape(normalized, quote=True)
    return f'<a href="{escaped_url}">Ссылка на верификатор</a>'


def daily_stats_date_label(days_ago: int) -> str:
    target_day = datetime.now(MSK_TIMEZONE).date() - timedelta(days=days_ago)
    return target_day.strftime("%d.%m.%Y")


def parse_week_input(raw_value: str) -> tuple[date, date]:
    value = normalize_text(raw_value)
    if not value:
        raise ValueError("Нужно выбрать неделю")

    match = re.fullmatch(r"(\d{4})-W(\d{2})", value)
    if match is None:
        raise ValueError("Неделя должна быть в формате YYYY-Www")

    year = int(match.group(1))
    week = int(match.group(2))
    try:
        week_start = date.fromisocalendar(year, week, 1)
    except ValueError as exc:
        raise ValueError("Указана некорректная ISO-неделя") from exc
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


def parse_month_input(raw_value: str) -> tuple[int, int]:
    value = normalize_text(raw_value)
    if not value:
        raise ValueError("Нужно выбрать месяц")

    match = re.fullmatch(r"(\d{4})-(\d{2})", value)
    if match is None:
        raise ValueError("Месяц должен быть в формате YYYY-MM")

    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        raise ValueError("Нужно выбрать корректный месяц")
    return year, month


def iso_week_input_value(target_date: date) -> str:
    iso_year, iso_week, _ = target_date.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def month_input_value(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def build_daily_stats_message(snapshot: DailyStatsSnapshot) -> str:
    link = build_verifier_link_html(snapshot.verifier_url)
    return (
        f"Статистика рассылки {escape_html_text(snapshot.dispatch_title)} "
        f"за {escape_html_text(snapshot.stats_date)}\n"
        f"{snapshot.win_count}✅/{snapshot.lose_count}✖️/{snapshot.return_count}♻️\n"
        f"Прибыль составила {escape_html_text(snapshot.profit_percent)}\n"
        f"{link}\n"
        "☝️☝️☝️"
    )


def previous_week_period(reference_dt: Optional[datetime] = None) -> tuple[date, date]:
    now_msk = reference_dt or datetime.now(MSK_TIMEZONE)
    current_monday = now_msk.date() - timedelta(days=now_msk.weekday())
    previous_week_start = current_monday - timedelta(days=7)
    previous_week_end = current_monday - timedelta(days=1)
    return previous_week_start, previous_week_end


def weekly_stats_period_key(week_start: date, week_end: date) -> str:
    return f"{week_start.strftime('%d.%m.%Y')}..{week_end.strftime('%d.%m.%Y')}"


def weekly_stats_period_label(week_start: date, week_end: date) -> str:
    return f"{week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}"


def previous_month_period(reference_dt: Optional[datetime] = None) -> tuple[int, int]:
    now_msk = reference_dt or datetime.now(MSK_TIMEZONE)
    current_month_start = now_msk.date().replace(day=1)
    previous_month_last_day = current_month_start - timedelta(days=1)
    return previous_month_last_day.year, previous_month_last_day.month


def monthly_stats_period_key(year: int, month: int) -> str:
    return f"{month:02d}.{year}"


def month_short_label(year: int, month: int) -> str:
    return f"{RUS_MONTH_SHORT.get(month, str(month))} {year}"


def build_weekly_stats_message(snapshot: WeeklyStatsSnapshot) -> str:
    day_lines: list[str] = []
    for day_item in snapshot.day_items:
        parsed_day = parse_stats_date(day_item.stats_date)
        day_label = parsed_day.strftime("%d.%m") if parsed_day is not None else day_item.stats_date
        day_lines.append(
            f"{day_label} "
            f"{settlement_status_icon(day_item.settlement_status)}"
            f"{escape_html_text(day_item.profit_percent)}"
        )
    body_lines = "\n".join(day_lines)
    link = build_verifier_link_html(snapshot.verifier_url)
    return (
        f"{escape_html_text(snapshot.dispatch_title)}\n"
        "Всем доброго дня!\n"
        f"За прошедшую неделю прибыль составила {escape_html_text(snapshot.total_profit_percent)}\n"
        f"{body_lines}\n\n"
        f"{link}\n"
        "☝️☝️☝️"
    )


def build_monthly_stats_message(snapshot: MonthlyStatsSnapshot) -> str:
    link = build_verifier_link_html(snapshot.verifier_url)
    return (
        f"{escape_html_text(snapshot.dispatch_title)}\n"
        "Всем доброго дня!\n"
        "За прошедший месяц "
        f"прибыль составила {escape_html_text(snapshot.profit_percent)}\n"
        f"{link}\n"
        "☝️☝️☝️"
    )


def extract_telegram_message_id(result: Any) -> Optional[int]:
    if not isinstance(result, dict):
        return None

    message_id = result.get("message_id")
    if isinstance(message_id, int):
        return message_id
    if isinstance(message_id, str) and message_id.isdigit():
        return int(message_id)
    return None


def extract_vk_message_id(result: Any) -> Optional[int]:
    if isinstance(result, int):
        return result
    if isinstance(result, str) and result.lstrip("-").isdigit():
        return int(result)
    if isinstance(result, dict):
        message_id = result.get("message_id")
        if isinstance(message_id, int):
            return message_id
        if isinstance(message_id, str) and message_id.lstrip("-").isdigit():
            return int(message_id)
    return None


def format_exception_details(exc: Exception) -> str:
    message = normalize_text(str(exc))
    if message:
        return f"{exc.__class__.__name__}: {message}"
    return f"{exc.__class__.__name__}: {repr(exc)}"


async def telegram_api_post(
    session: aiohttp.ClientSession,
    tg_cfg: TelegramConfig,
    method: str,
    payload: dict[str, str],
) -> Any:
    endpoint = f"https://api.telegram.org/bot{tg_cfg.bot_token}/{method}"
    status_code = 0
    try:
        async with session.post(endpoint, data=payload) as response:
            status_code = response.status
            try:
                data = await response.json(content_type=None)
            except Exception as exc:  # noqa: BLE001
                raw_payload = normalize_text(await response.text())
                raise RuntimeError(
                    f"Telegram API вернул некорректный JSON: {raw_payload[:180]}"
                ) from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Ошибка запроса в Telegram API: {format_exception_details(exc)}") from exc

    if not isinstance(data, dict):
        raise RuntimeError("Telegram API вернул неожиданный формат ответа")

    if not data.get("ok"):
        description = normalize_text(str(data.get("description", "unknown error")))
        error_code = data.get("error_code")
        if error_code is not None:
            raise RuntimeError(f"Telegram API error {error_code}: {description or 'unknown error'}")
        raise RuntimeError(description or "Telegram API request failed")

    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code} от Telegram API")

    return data.get("result")


async def telegram_api_post_multipart(
    session: aiohttp.ClientSession,
    tg_cfg: TelegramConfig,
    method: str,
    payload: dict[str, str],
    *,
    file_field_name: str,
    file_bytes: bytes,
    filename: str,
    content_type: str,
) -> Any:
    endpoint = f"https://api.telegram.org/bot{tg_cfg.bot_token}/{method}"
    status_code = 0
    form = aiohttp.FormData()
    for key, value in payload.items():
        form.add_field(key, value)
    form.add_field(
        file_field_name,
        file_bytes,
        filename=filename,
        content_type=content_type,
    )

    try:
        async with session.post(endpoint, data=form) as response:
            status_code = response.status
            try:
                data = await response.json(content_type=None)
            except Exception as exc:  # noqa: BLE001
                raw_payload = normalize_text(await response.text())
                raise RuntimeError(
                    f"Telegram API вернул некорректный JSON: {raw_payload[:180]}"
                ) from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Ошибка запроса в Telegram API: {format_exception_details(exc)}") from exc

    if not isinstance(data, dict):
        raise RuntimeError("Telegram API вернул неожиданный формат ответа")

    if not data.get("ok"):
        description = normalize_text(str(data.get("description", "unknown error")))
        error_code = data.get("error_code")
        if error_code is not None:
            raise RuntimeError(f"Telegram API error {error_code}: {description or 'unknown error'}")
        raise RuntimeError(description or "Telegram API request failed")

    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code} от Telegram API")

    return data.get("result")


async def vk_api_post(
    session: aiohttp.ClientSession,
    vk_cfg: VkConfig,
    method: str,
    payload: dict[str, str],
) -> Any:
    endpoint = f"https://api.vk.com/method/{method}"
    request_payload = dict(payload)
    request_payload["access_token"] = vk_cfg.user_token
    request_payload["v"] = vk_cfg.api_version

    status_code = 0
    try:
        async with session.post(endpoint, data=request_payload) as response:
            status_code = response.status
            try:
                data = await response.json(content_type=None)
            except Exception as exc:  # noqa: BLE001
                raw_payload = normalize_text(await response.text())
                raise RuntimeError(
                    f"VK API вернул некорректный JSON: {raw_payload[:180]}"
                ) from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Ошибка запроса в VK API: {format_exception_details(exc)}") from exc

    if not isinstance(data, dict):
        raise RuntimeError("VK API вернул неожиданный формат ответа")

    error_payload = data.get("error")
    if isinstance(error_payload, dict):
        error_code = error_payload.get("error_code")
        error_msg = normalize_text(str(error_payload.get("error_msg", "unknown error")))
        if error_code is not None:
            raise RuntimeError(f"VK API error {error_code}: {error_msg or 'unknown error'}")
        raise RuntimeError(error_msg or "VK API request failed")

    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code} от VK API")

    return data.get("response")


async def fetch_vk_chat_peer_ids(
    session: aiohttp.ClientSession,
    vk_cfg: VkConfig,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    safe_limit = min(max(int(limit), 1), 200)
    response = await vk_api_post(
        session,
        vk_cfg,
        "messages.getConversations",
        {"count": str(safe_limit)},
    )
    if not isinstance(response, dict):
        raise RuntimeError("VK API не вернул данные диалогов")

    items = response.get("items")
    if not isinstance(items, list):
        raise RuntimeError("VK API вернул некорректный список диалогов")

    chats: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        conversation = item.get("conversation")
        if not isinstance(conversation, dict):
            continue
        peer = conversation.get("peer")
        if not isinstance(peer, dict):
            continue
        if normalize_text(str(peer.get("type", ""))) != "chat":
            continue
        peer_id_raw = peer.get("id")
        try:
            peer_id = int(peer_id_raw)
        except (TypeError, ValueError):
            continue

        title = "Без названия"
        chat_settings = conversation.get("chat_settings")
        if isinstance(chat_settings, dict):
            title_candidate = normalize_text(str(chat_settings.get("title", "")))
            if title_candidate:
                title = title_candidate

        chats.append(
            {
                "title": title,
                "peer_id": peer_id,
                "chat_id": peer_id - 2000000000,
            }
        )

    chats.sort(key=lambda item: str(item.get("title", "")).lower())
    return chats


async def download_image_bytes(
    session: aiohttp.ClientSession,
    image_url: str,
) -> tuple[bytes, str]:
    try:
        async with session.get(image_url) as response:
            if response.status >= 400:
                raise RuntimeError(f"HTTP {response.status}")
            content = await response.read()
            if not content:
                raise RuntimeError("empty image content")
            content_type = normalize_text(response.headers.get("Content-Type", "image/jpeg"))
            normalized_content_type = content_type.split(";")[0].strip() or "image/jpeg"
            return content, normalized_content_type
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Не удалось скачать изображение: {format_exception_details(exc)}") from exc


async def send_telegram_match_message(
    session: aiohttp.ClientSession,
    tg_cfg: TelegramConfig,
    chat_id: str,
    text: str,
    image_url: str = "",
    parse_mode: str = "",
    require_photo: bool = False,
) -> int:
    target_chat_id = validate_chat_id(chat_id)
    normalized_image_url = normalize_text(image_url)
    is_external_image_url = normalized_image_url.startswith(("http://", "https://"))
    normalized_parse_mode = normalize_text(parse_mode)
    photo_errors: list[str] = []

    if require_photo and not is_external_image_url:
        raise ValueError("Нельзя отправить матч без корректного URL изображения")

    if is_external_image_url:
        caption = text if len(text) <= 1024 else text[:1021] + "..."
        try:
            photo_payload = {
                "chat_id": target_chat_id,
                "caption": caption,
            }
            if normalized_parse_mode:
                photo_payload["parse_mode"] = normalized_parse_mode
            photo_bytes, content_type = await download_image_bytes(session, normalized_image_url)
            photo_result = await telegram_api_post_multipart(
                session,
                tg_cfg,
                "sendPhoto",
                photo_payload,
                file_field_name="photo",
                file_bytes=photo_bytes,
                filename="match.jpg",
                content_type=content_type,
            )
            message_id = extract_telegram_message_id(photo_result)
            if message_id is not None:
                return message_id
        except Exception as exc:  # noqa: BLE001
            error_details = format_exception_details(exc)
            photo_errors.append(f"upload: {error_details}")
            logger.warning(
                "Не удалось отправить фото upload в Telegram, пробую URL. chat_id=%s error=%s",
                target_chat_id,
                error_details,
            )

        try:
            photo_payload = {
                "chat_id": target_chat_id,
                "photo": normalized_image_url,
                "caption": caption,
            }
            if normalized_parse_mode:
                photo_payload["parse_mode"] = normalized_parse_mode
            photo_result = await telegram_api_post(
                session,
                tg_cfg,
                "sendPhoto",
                photo_payload,
            )
            message_id = extract_telegram_message_id(photo_result)
            if message_id is not None:
                return message_id
        except Exception as exc:  # noqa: BLE001
            error_details = format_exception_details(exc)
            photo_errors.append(f"url: {error_details}")
            logger.warning(
                "Не удалось отправить фото URL в Telegram. chat_id=%s error=%s",
                target_chat_id,
                error_details,
            )

    if require_photo:
        if photo_errors:
            raise RuntimeError(
                "Не удалось отправить обязательное изображение матча: "
                + "; ".join(photo_errors)
            )
        raise RuntimeError("Нельзя отправить матч без изображения")

    message_payload = {
        "chat_id": target_chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    if normalized_parse_mode:
        message_payload["parse_mode"] = normalized_parse_mode
    message_result = await telegram_api_post(
        session,
        tg_cfg,
        "sendMessage",
        message_payload,
    )
    message_id = extract_telegram_message_id(message_result)
    if message_id is None:
        raise RuntimeError("Telegram не вернул message_id")

    return message_id


async def upload_vk_message_photo_from_url(
    session: aiohttp.ClientSession,
    vk_cfg: VkConfig,
    image_url: str,
) -> str:
    upload_server_response = await vk_api_post(
        session,
        vk_cfg,
        "photos.getMessagesUploadServer",
        {},
    )
    if not isinstance(upload_server_response, dict):
        raise RuntimeError("VK API не вернул upload_url")
    upload_url = normalize_text(str(upload_server_response.get("upload_url", "")))
    if not upload_url.startswith(("http://", "https://")):
        raise RuntimeError("VK API вернул некорректный upload_url")

    photo_bytes, content_type = await download_image_bytes(session, image_url)

    form = aiohttp.FormData()
    form.add_field(
        "photo",
        photo_bytes,
        filename="match.jpg",
        content_type=content_type,
    )
    try:
        async with session.post(upload_url, data=form) as upload_response:
            if upload_response.status >= 400:
                raise RuntimeError(f"HTTP {upload_response.status}")
            try:
                upload_result = await upload_response.json(content_type=None)
            except Exception as exc:  # noqa: BLE001
                raw_payload = normalize_text(await upload_response.text())
                raise RuntimeError(
                    f"VK upload вернул некорректный JSON: {raw_payload[:180]}"
                ) from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Не удалось загрузить фото в VK: {format_exception_details(exc)}") from exc

    if not isinstance(upload_result, dict):
        raise RuntimeError("VK upload вернул неожиданный формат ответа")

    photo = normalize_text(str(upload_result.get("photo", "")))
    server = upload_result.get("server")
    upload_hash = normalize_text(str(upload_result.get("hash", "")))
    if not photo or server is None or not upload_hash:
        raise RuntimeError("VK upload не вернул обязательные поля photo/server/hash")

    save_response = await vk_api_post(
        session,
        vk_cfg,
        "photos.saveMessagesPhoto",
        {
            "photo": photo,
            "server": str(server),
            "hash": upload_hash,
        },
    )
    if not isinstance(save_response, list) or not save_response:
        raise RuntimeError("VK API не вернул данные сохраненного изображения")

    saved_photo = save_response[0]
    if not isinstance(saved_photo, dict):
        raise RuntimeError("VK API вернул некорректные данные изображения")

    owner_id = saved_photo.get("owner_id")
    photo_id = saved_photo.get("id")
    if owner_id is None or photo_id is None:
        raise RuntimeError("VK API не вернул owner_id/id для вложения")
    access_key = normalize_text(str(saved_photo.get("access_key", "")))
    attachment = f"photo{owner_id}_{photo_id}"
    if access_key:
        attachment = f"{attachment}_{access_key}"
    return attachment


async def send_vk_match_message(
    session: aiohttp.ClientSession,
    vk_cfg: VkConfig,
    chat_id: str,
    text: str,
    image_url: str = "",
) -> int:
    target_chat_id = validate_vk_chat_id(chat_id)
    normalized_image_url = normalize_text(image_url)
    is_external_image_url = normalized_image_url.startswith(("http://", "https://"))
    attachment = ""

    if is_external_image_url:
        try:
            attachment = await upload_vk_message_photo_from_url(
                session,
                vk_cfg,
                normalized_image_url,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Не удалось загрузить фото в VK, отправляю только текст. chat_id=%s error=%s",
                target_chat_id,
                format_exception_details(exc),
            )

    payload: dict[str, str] = {
        "peer_id": target_chat_id,
        "message": text,
        "random_id": str(int(time.time() * 1000) & 0x7FFFFFFF),
    }
    if attachment:
        payload["attachment"] = attachment

    response = await vk_api_post(session, vk_cfg, "messages.send", payload)
    message_id = extract_vk_message_id(response)
    if message_id is None:
        raise RuntimeError("VK не вернул message_id")
    return message_id


def is_telegram_message_not_modified_error(exc: Exception) -> bool:
    lowered = normalize_text(str(exc)).lower()
    return "message is not modified" in lowered


def build_telegram_edit_html(
    text: str,
    *,
    match_url: str = "",
) -> str:
    normalized_text = (text or "").strip()
    if not normalized_text:
        return ""

    fallback_url = normalize_text(match_url)
    escaped_lines: list[str] = []
    has_link_line = False
    for raw_line in normalized_text.splitlines():
        line = raw_line or ""
        normalized_line = normalize_text(line)
        link_match = re.search(r"https?://\S+", line)
        should_render_match_link = normalized_line.lower().startswith("🔗 ссылка на матч")
        if should_render_match_link:
            target_url = normalize_text(link_match.group(0)) if link_match else fallback_url
            if target_url.startswith(("http://", "https://")):
                escaped_url = html.escape(target_url, quote=True)
                escaped_lines.append(f'🔗 <a href="{escaped_url}">Ссылка на матч</a>')
                has_link_line = True
                continue
        escaped_lines.append(html.escape(line, quote=False))

    if not has_link_line and fallback_url.startswith(("http://", "https://")):
        escaped_url = html.escape(fallback_url, quote=True)
        escaped_lines.append(f'🔗 <a href="{escaped_url}">Ссылка на матч</a>')

    return "\n".join(escaped_lines)


async def edit_telegram_message(
    session: aiohttp.ClientSession,
    tg_cfg: TelegramConfig,
    chat_id: str,
    message_id: int,
    text: str,
    match_url: str = "",
) -> str:
    target_chat_id = validate_chat_id(chat_id)
    normalized_text = (text or "").strip()
    if not normalized_text:
        raise ValueError("Нельзя отправить пустое обновление сообщения")

    caption_text = build_telegram_edit_html(normalized_text, match_url=match_url)
    if not caption_text:
        raise ValueError("Нельзя отправить пустое обновление сообщения")
    if len(caption_text) > 1024:
        caption_text = caption_text[:1021] + "..."

    caption_error: Optional[Exception] = None
    try:
        await telegram_api_post(
            session,
            tg_cfg,
            "editMessageCaption",
            {
                "chat_id": target_chat_id,
                "message_id": str(message_id),
                "caption": caption_text,
                "parse_mode": "HTML",
            },
        )
        log_message_edit_info(
            "Telegram editMessage выполнен. chat_id=%s message_id=%s branch=caption",
            target_chat_id,
            message_id,
        )
        return "caption_updated"
    except Exception as exc:  # noqa: BLE001
        if is_telegram_message_not_modified_error(exc):
            log_message_edit_info(
                "Telegram editMessage: сообщение без изменений. chat_id=%s message_id=%s branch=caption",
                target_chat_id,
                message_id,
            )
            return "caption_not_modified"
        caption_error = exc

    if caption_error is not None:
        log_message_edit_info(
            "Telegram editMessage fallback на text. chat_id=%s message_id=%s reason=%s",
            target_chat_id,
            message_id,
            format_exception_details(caption_error),
        )

    text_error: Optional[Exception] = None
    try:
        text_payload = build_telegram_edit_html(normalized_text, match_url=match_url)
        await telegram_api_post(
            session,
            tg_cfg,
            "editMessageText",
            {
                "chat_id": target_chat_id,
                "message_id": str(message_id),
                "text": text_payload,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
        )
        log_message_edit_info(
            "Telegram editMessage выполнен. chat_id=%s message_id=%s branch=text",
            target_chat_id,
            message_id,
        )
        return "text_updated"
    except Exception as exc:  # noqa: BLE001
        if is_telegram_message_not_modified_error(exc):
            log_message_edit_info(
                "Telegram editMessage: сообщение без изменений. chat_id=%s message_id=%s branch=text",
                target_chat_id,
                message_id,
            )
            return "text_not_modified"
        text_error = exc

    details: list[str] = []
    if caption_error is not None:
        details.append(f"caption: {format_exception_details(caption_error)}")
    if text_error is not None:
        details.append(f"text: {format_exception_details(text_error)}")
    joined = " | ".join(details) if details else "unknown"
    raise RuntimeError(f"Не удалось изменить сообщение в Telegram: {joined}")


def try_wait_visible(page: Page, selector: str, timeout_ms: int = 2500) -> bool:
    try:
        page.locator(selector).first.wait_for(
            state="visible", timeout=timeout_ms)
        return True
    except Exception:  # noqa: BLE001
        return False


def get_visible_texts(page: Page, selector: str, limit: int = 8) -> list[str]:
    texts: list[str] = []
    if not selector:
        return texts

    nodes = page.locator(selector)
    count = min(nodes.count(), limit)
    for i in range(count):
        node = nodes.nth(i)
        try:
            if not node.is_visible():
                continue
            raw = node.inner_text().strip()
        except Exception:  # noqa: BLE001
            continue

        normalized = normalize_text(raw)
        if normalized:
            texts.append(normalized)
    return texts


def is_login_form_visible(page: Page, selector: str) -> bool:
    if not selector:
        return False

    try:
        locator = page.locator(selector)
        return locator.count() > 0 and locator.first.is_visible()
    except Exception:  # noqa: BLE001
        return False


async def is_async_login_form_visible(page: AsyncPage, selector: str) -> bool:
    if not selector:
        return False

    try:
        locator = page.locator(selector)
        count = await locator.count()
        if count <= 0:
            return False
        return await locator.first.is_visible()
    except Exception:  # noqa: BLE001
        return False


async def ensure_async_authorized(page: AsyncPage, cfg: TargetConfig) -> None:
    if await is_async_login_form_visible(page, cfg.login_form_selector):
        raise LoginRequiredError("На странице снова отображается форма логина")


async def click_active_tab(page: AsyncPage) -> None:
    try:
        active_now = page.locator(
            ".tab.tab_lg.active-tab:has-text('Активные')")
        if await active_now.count() > 0 and await active_now.first.is_visible():
            return
    except Exception:  # noqa: BLE001
        pass

    active_tab_selectors = [
        ".tab.tab_lg:has-text('Активные')",
        ".tab.tab_lg.active-tab:has-text('Активные')",
        "button:has-text('Активные')",
        "a:has-text('Активные')",
        "[role='tab']:has-text('Активные')",
        ".tabs__item:has-text('Активные')",
        ".tab-link:has-text('Активные')",
    ]

    for selector in active_tab_selectors:
        locator = page.locator(selector)
        if await locator.count() == 0:
            continue

        node = locator.first
        try:
            if not await node.is_visible():
                continue
            await node.click(timeout=3000)
            await page.wait_for_timeout(700)
            return
        except Exception:  # noqa: BLE001
            continue


async def ensure_show_by_40(page: AsyncPage, *, force_click: bool = False) -> None:
    container: Optional[Any] = None
    container_selectors = [
        "#tab-forecast-active .js-per-page-select",
        ".panel-container.active-tab .js-per-page-select",
        ".js-per-page-select",
    ]
    for selector in container_selectors:
        locator = page.locator(selector)
        count = await locator.count()
        if count == 0:
            continue
        for idx in range(count):
            candidate = locator.nth(idx)
            try:
                if await candidate.is_visible():
                    container = candidate
                    break
            except Exception:  # noqa: BLE001
                continue
        if container is not None:
            break

    if container is None:
        return

    is_already_selected = False
    try:
        selected_label = normalize_text(
            await container.locator(".ss-single-selected .placeholder").first.inner_text()
        )
        is_already_selected = "Показать по 40" in selected_label
    except Exception:  # noqa: BLE001
        pass

    if is_already_selected and not force_click:
        return

    try:
        await container.locator(".ss-single-selected").first.click(timeout=3000)
        await page.wait_for_timeout(250)
    except Exception:  # noqa: BLE001
        return

    option_selectors = [
        ".ss-content.ss-open .ss-option:has-text('Показать по 40')",
        ".ss-content .ss-option:has-text('Показать по 40')",
    ]
    for selector in option_selectors:
        option_locator = container.locator(selector)
        if await option_locator.count() == 0:
            option_locator = page.locator(selector)
        if await option_locator.count() == 0:
            continue

        option_node = option_locator.first
        try:
            if not await option_node.is_visible():
                continue
            option_classes = normalize_text(await option_node.get_attribute("class") or "")
            is_selected_and_locked = (
                "ss-option-selected" in option_classes
                and "ss-disabled" in option_classes
            )
            if not is_selected_and_locked:
                await option_node.click(timeout=3000)
                await page.wait_for_timeout(400)
            return
        except Exception:  # noqa: BLE001
            continue


async def wait_until_pagination_page_active(page: AsyncPage, target_page_index: int) -> bool:
    try:
        await page.wait_for_function(
            """
(targetPageIndex) => {
  const selectors = [
    "#tab-forecast-active .pagination .pagination__item_active .pagination__link[data-page]",
    ".panel-container.active-tab .pagination .pagination__item_active .pagination__link[data-page]",
    ".pagination .pagination__item_active .pagination__link[data-page]",
  ];

  for (const selector of selectors) {
    const activeLink = document.querySelector(selector);
    if (!activeLink) {
      continue;
    }
    const rawIndex = (activeLink.getAttribute("data-page") || "").trim();
    if (rawIndex === String(targetPageIndex)) {
      return true;
    }
  }
  return false;
}
""",
            target_page_index,
            timeout=8000,
        )
        return True
    except Exception:  # noqa: BLE001
        return False


async def ensure_scan_page_prefers_second(page: AsyncPage, parse_url: str) -> None:
    pagination_state_script = """
() => {
  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
  const isVisible = (element) => {
    if (!element) {
      return false;
    }
    const style = window.getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
  };

  const candidates = [
    ...Array.from(document.querySelectorAll("#tab-forecast-active .pagination")),
    ...Array.from(document.querySelectorAll(".panel-container.active-tab .pagination")),
    ...Array.from(document.querySelectorAll(".pagination")),
  ];

  const deduped = [];
  for (const candidate of candidates) {
    if (!candidate || deduped.includes(candidate)) {
      continue;
    }
    deduped.push(candidate);
  }

  const pagination = deduped.find((node) => isVisible(node));
  if (!pagination) {
    return {
      has_pagination: false,
      active_page_index: -1,
      has_second_page: false,
      page_1_href: "",
      page_2_href: "",
    };
  }

  const links = Array.from(pagination.querySelectorAll(".pagination__link[data-page]"));
  const href_by_page = {};

  for (const link of links) {
    if (!isVisible(link)) {
      continue;
    }
    const raw_page = normalize(link.getAttribute("data-page"));
    if (!raw_page || Object.prototype.hasOwnProperty.call(href_by_page, raw_page)) {
      continue;
    }
    href_by_page[raw_page] = normalize(link.getAttribute("href") || "");
  }

  const active_link = pagination.querySelector(".pagination__item_active .pagination__link[data-page]");
  const active_raw = normalize(active_link?.getAttribute("data-page"));
  const active_page_index = /^\\d+$/.test(active_raw) ? Number.parseInt(active_raw, 10) : -1;

  return {
    has_pagination: true,
    active_page_index,
    has_second_page: Object.prototype.hasOwnProperty.call(href_by_page, "1"),
    page_1_href: href_by_page["0"] || "",
    page_2_href: href_by_page["1"] || "",
  };
}
"""

    try:
        raw_state = await page.evaluate(pagination_state_script)
    except Exception:  # noqa: BLE001
        return

    if not isinstance(raw_state, dict):
        return

    has_pagination = bool(raw_state.get("has_pagination"))
    has_second_page = bool(raw_state.get("has_second_page"))
    target_page_index = 1 if has_second_page else 0

    if not has_pagination and target_page_index == 0:
        try:
            current_url = normalize_text(page.url)
            current_query = dict(
                parse_qsl(urlsplit(current_url).query, keep_blank_values=True)
            )
            current_page_raw = normalize_text(current_query.get("page", ""))
            current_page_number = int(current_page_raw) if current_page_raw.isdigit() else 1
            if current_page_number > 1:
                fallback_first_page_url = upsert_query_param(
                    parse_url,
                    "ForecastSearch[perPage]",
                    "40",
                )
                await page.goto(
                    fallback_first_page_url,
                    wait_until="domcontentloaded",
                    timeout=45000,
                )
                await page.wait_for_timeout(1200)
        except Exception:  # noqa: BLE001
            pass
        return

    try:
        active_page_index = int(raw_state.get("active_page_index", -1))
    except Exception:  # noqa: BLE001
        active_page_index = -1

    if active_page_index == target_page_index:
        return

    target_selectors = [
        f"#tab-forecast-active .pagination .pagination__link[data-page='{target_page_index}']",
        f".panel-container.active-tab .pagination .pagination__link[data-page='{target_page_index}']",
        f".pagination .pagination__link[data-page='{target_page_index}']",
    ]
    for selector in target_selectors:
        locator = page.locator(selector)
        count = await locator.count()
        if count == 0:
            continue
        for idx in range(count):
            node = locator.nth(idx)
            try:
                if not await node.is_visible():
                    continue
                await node.click(timeout=3000)
                if await wait_until_pagination_page_active(page, target_page_index):
                    await page.wait_for_timeout(700)
                    return
            except Exception:  # noqa: BLE001
                continue

    raw_target_href = normalize_text(
        str(
            raw_state.get("page_2_href", "")
            if target_page_index == 1
            else raw_state.get("page_1_href", "")
        )
    )
    fallback_url = parse_url
    if raw_target_href:
        fallback_url = remove_query_param(urljoin(parse_url, raw_target_href), "_pjax")
    fallback_url = upsert_query_param(
        fallback_url,
        "ForecastSearch[perPage]",
        "40",
    )

    try:
        await page.goto(fallback_url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(1200)
    except Exception:  # noqa: BLE001
        pass


async def click_completed_tab(page: AsyncPage) -> None:
    try:
        completed_now = page.locator(
            ".tab.tab_lg.active-tab:has-text('Прошедшие')"
        )
        if await completed_now.count() > 0 and await completed_now.first.is_visible():
            return
    except Exception:  # noqa: BLE001
        pass

    completed_tab_selectors = [
        ".tab.tab_lg:has-text('Прошедшие')",
        ".tab.tab_lg.active-tab:has-text('Прошедшие')",
        "a[href='#tab-forecast-zip']",
        "button:has-text('Прошедшие')",
        "a:has-text('Прошедшие')",
        "[role='tab']:has-text('Прошедшие')",
        ".tabs__item:has-text('Прошедшие')",
        ".tab-link:has-text('Прошедшие')",
    ]

    for selector in completed_tab_selectors:
        locator = page.locator(selector)
        if await locator.count() == 0:
            continue

        node = locator.first
        try:
            if not await node.is_visible():
                continue
            await node.click(timeout=3000)
            await page.wait_for_timeout(700)
            return
        except Exception:  # noqa: BLE001
            continue

    raise RuntimeError("Не удалось открыть вкладку 'Прошедшие'")


async def click_profit_tab(page: AsyncPage) -> None:
    try:
        profit_panel = page.locator("#tabs-profit")
        if await profit_panel.count() > 0 and await profit_panel.first.is_visible():
            return
    except Exception:  # noqa: BLE001
        pass

    profit_tab_selectors = [
        "a[href='#tabs-profit']",
        ".tab a:has-text('Прибыль')",
        "button:has-text('Прибыль')",
        "[role='tab']:has-text('Прибыль')",
    ]
    for selector in profit_tab_selectors:
        locator = page.locator(selector)
        if await locator.count() == 0:
            continue
        node = locator.first
        try:
            if not await node.is_visible():
                continue
            await node.click(timeout=4000)
            await page.wait_for_timeout(900)
            return
        except Exception:  # noqa: BLE001
            continue

    raise RuntimeError("Не удалось открыть вкладку 'Прибыль'")


async def click_profit_day_tab(page: AsyncPage) -> None:
    try:
        active_day_panel = page.locator("#tab-day.active, #tab-day.active-content-div")
        if await active_day_panel.count() > 0 and await active_day_panel.first.is_visible():
            return
    except Exception:  # noqa: BLE001
        pass

    day_tab_selectors = [
        "a[href='#tab-day']",
        ".tab a:has-text('День')",
        "button:has-text('День')",
        "[role='tab']:has-text('День')",
    ]
    for selector in day_tab_selectors:
        locator = page.locator(selector)
        if await locator.count() == 0:
            continue
        node = locator.first
        try:
            if not await node.is_visible():
                continue
            await node.click(timeout=4000)
            await page.wait_for_timeout(900)
            return
        except Exception:  # noqa: BLE001
            continue

    # На большинстве страниц tab-day открыт по умолчанию, поэтому отсутствие клика
    # не всегда ошибка. Проверим наличие таблицы.
    try:
        day_rows = page.locator("#tab-day .rTableBody .rTableLine")
        if await day_rows.count() > 0:
            return
    except Exception:  # noqa: BLE001
        pass
    raise RuntimeError("Не удалось открыть дневную статистику во вкладке 'Прибыль'")


async def click_profit_month_tab(page: AsyncPage) -> None:
    try:
        active_month_panel = page.locator(
            "#tab-month.active, #tab-month.active-content-div"
        )
        if await active_month_panel.count() > 0 and await active_month_panel.first.is_visible():
            return
    except Exception:  # noqa: BLE001
        pass

    month_tab_selectors = [
        "a[href='#tab-month']",
        ".tab a:has-text('Месяцы')",
        "button:has-text('Месяцы')",
        "[role='tab']:has-text('Месяцы')",
    ]
    for selector in month_tab_selectors:
        locator = page.locator(selector)
        if await locator.count() == 0:
            continue
        node = locator.first
        try:
            if not await node.is_visible():
                continue
            await node.click(timeout=4000)
            await page.wait_for_timeout(900)
            return
        except Exception:  # noqa: BLE001
            continue

    try:
        month_rows = page.locator("#tab-month .rTableBody .rTableLine")
        if await month_rows.count() > 0:
            return
    except Exception:  # noqa: BLE001
        pass
    raise RuntimeError("Не удалось открыть месячную статистику во вкладке 'Прибыль'")


async def extract_daily_profit_rows(page: AsyncPage) -> list[dict[str, str]]:
    script = """
() => {
  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
  const rows = Array.from(document.querySelectorAll("#tab-day .rTableBody .rTableLine"));
  return rows
    .map((row) => {
      const roiCells = Array.from(row.querySelectorAll(".rTableCell.cell-roi .cell-inner"));
      return {
        stats_date: normalize(row.querySelector(".rTableCell.cell-month .cell-inner")?.textContent),
        win_count: normalize(row.querySelector(".rating__data-total .win")?.textContent),
        lose_count: normalize(row.querySelector(".rating__data-total .lose")?.textContent),
        return_count: normalize(row.querySelector(".rating__data-total .return")?.textContent),
        profit_percent: normalize(roiCells[0]?.textContent),
      };
    })
    .filter((item) => item.stats_date);
}
"""
    try:
        raw_rows = await page.evaluate(script)
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(raw_rows, list):
        return []
    normalized_rows: list[dict[str, str]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        normalized_rows.append(
            {
                "stats_date": normalize_text(str(row.get("stats_date", ""))),
                "win_count": normalize_text(str(row.get("win_count", ""))),
                "lose_count": normalize_text(str(row.get("lose_count", ""))),
                "return_count": normalize_text(str(row.get("return_count", ""))),
                "profit_percent": normalize_text(str(row.get("profit_percent", ""))),
            }
        )
    return normalized_rows


async def extract_monthly_profit_rows(page: AsyncPage) -> list[dict[str, str]]:
    script = """
() => {
  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
  const rows = Array.from(document.querySelectorAll("#tab-month .rTableBody .rTableLine"));
  return rows
    .map((row) => {
      const roiCells = Array.from(row.querySelectorAll(".rTableCell.cell-roi .cell-inner"));
      return {
        stats_month: normalize(row.querySelector(".rTableCell.cell-month .cell-inner")?.textContent),
        win_count: normalize(row.querySelector(".rating__data-total .win")?.textContent),
        lose_count: normalize(row.querySelector(".rating__data-total .lose")?.textContent),
        return_count: normalize(row.querySelector(".rating__data-total .return")?.textContent),
        profit_percent: normalize(roiCells[0]?.textContent),
      };
    })
    .filter((item) => item.stats_month);
}
"""
    try:
        raw_rows = await page.evaluate(script)
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(raw_rows, list):
        return []
    normalized_rows: list[dict[str, str]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        normalized_rows.append(
            {
                "stats_month": normalize_text(str(row.get("stats_month", ""))),
                "win_count": normalize_text(str(row.get("win_count", ""))),
                "lose_count": normalize_text(str(row.get("lose_count", ""))),
                "return_count": normalize_text(str(row.get("return_count", ""))),
                "profit_percent": normalize_text(str(row.get("profit_percent", ""))),
            }
        )
    return normalized_rows


async def go_to_next_daily_profit_page(page: AsyncPage) -> bool:
    next_page_selectors = [
        "#pjax-tab-day .pagination__item_next:not(.pagination__item_disabled) a.pagination__link",
        "#tab-day .pagination__item_next:not(.pagination__item_disabled) a.pagination__link",
        "#pjax-tab-day .pagination__item_next a.pagination__link",
    ]
    for selector in next_page_selectors:
        locator = page.locator(selector)
        if await locator.count() == 0:
            continue
        node = locator.first
        try:
            if not await node.is_visible():
                continue
            await node.click(timeout=4000)
            await page.wait_for_timeout(1000)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def go_to_next_monthly_profit_page(page: AsyncPage) -> bool:
    next_page_selectors = [
        "#pjax-tab-month .pagination__item_next:not(.pagination__item_disabled) a.pagination__link",
        "#tab-month .pagination__item_next:not(.pagination__item_disabled) a.pagination__link",
        "#pjax-tab-month .pagination__item_next a.pagination__link",
    ]
    for selector in next_page_selectors:
        locator = page.locator(selector)
        if await locator.count() == 0:
            continue
        node = locator.first
        try:
            if not await node.is_visible():
                continue
            await node.click(timeout=4000)
            await page.wait_for_timeout(1000)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def fetch_weekly_profit_snapshot(
    page: AsyncPage,
    cfg: TargetConfig,
    source_url: str,
    week_start: date,
    week_end: date,
    *,
    navigate: bool = True,
) -> WeeklyStatsSnapshot:
    if week_end < week_start:
        raise ValueError("Некорректный период недельной статистики")

    if navigate:
        await page.goto(source_url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(1200)
    else:
        await page.wait_for_timeout(250)

    await ensure_async_authorized(page, cfg)
    await click_profit_tab(page)
    await click_profit_day_tab(page)

    found_rows: dict[date, dict[str, str]] = {}
    days_in_period = (week_end - week_start).days + 1
    for _ in range(DEFAULT_DAILY_STATS_LOOKUP_MAX_PAGES):
        daily_rows = await extract_daily_profit_rows(page)
        if not daily_rows:
            await page.wait_for_timeout(700)
            daily_rows = await extract_daily_profit_rows(page)

        for row in daily_rows:
            parsed_day = parse_stats_date(row.get("stats_date", ""))
            if parsed_day is None:
                continue
            if week_start <= parsed_day <= week_end:
                found_rows[parsed_day] = row

        if len(found_rows) >= days_in_period:
            break

        moved = await go_to_next_daily_profit_page(page)
        if not moved:
            break

    if not found_rows:
        raise WeeklyStatsNotFoundError(
            "Не удалось получить данные за прошедшую неделю"
        )

    day_items: list[WeeklyStatsDaySnapshot] = []
    total_profit_value = 0.0
    current_day = week_start
    while current_day <= week_end:
        row = found_rows.get(current_day)
        raw_profit_percent = row.get("profit_percent", "") if row is not None else "0.00%"
        profit_value = parse_percent_number(raw_profit_percent)
        total_profit_value += profit_value
        day_items.append(
            WeeklyStatsDaySnapshot(
                stats_date=current_day.strftime("%d.%m.%Y"),
                profit_percent=format_percent_value(profit_value, with_sign=True),
                settlement_status=settlement_status_by_percent(profit_value),
            )
        )
        current_day += timedelta(days=1)

    return WeeklyStatsSnapshot(
        dispatch_title=dispatch_title_from_url(source_url),
        period_label=weekly_stats_period_label(week_start, week_end),
        total_profit_percent=format_percent_value(total_profit_value, with_sign=False),
        day_items=tuple(day_items),
        verifier_url=source_url,
    )


async def fetch_monthly_profit_snapshot(
    page: AsyncPage,
    cfg: TargetConfig,
    source_url: str,
    target_year: int,
    target_month: int,
    *,
    navigate: bool = True,
) -> MonthlyStatsSnapshot:
    if target_month < 1 or target_month > 12:
        raise ValueError("Некорректный месяц для месячной статистики")

    if navigate:
        await page.goto(source_url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(1200)
    else:
        await page.wait_for_timeout(250)

    await ensure_async_authorized(page, cfg)
    await click_profit_tab(page)
    await click_profit_month_tab(page)

    expected_month_label = normalize_text(month_short_label(target_year, target_month))
    for _ in range(DEFAULT_DAILY_STATS_LOOKUP_MAX_PAGES):
        monthly_rows = await extract_monthly_profit_rows(page)
        if not monthly_rows:
            await page.wait_for_timeout(700)
            monthly_rows = await extract_monthly_profit_rows(page)
        for row in monthly_rows:
            row_label = normalize_text(row.get("stats_month", ""))
            parsed_month_period = parse_stats_month_label(row_label)
            if parsed_month_period != (target_year, target_month):
                continue
            return MonthlyStatsSnapshot(
                dispatch_title=dispatch_title_from_url(source_url),
                month_label=row_label or expected_month_label,
                profit_percent=normalize_percent_value(row.get("profit_percent", "")),
                verifier_url=source_url,
            )
        moved = await go_to_next_monthly_profit_page(page)
        if not moved:
            break

    raise MonthlyStatsNotFoundError(
        f"В таблице 'Месяцы' нет строки за {expected_month_label}"
    )


async def fetch_daily_profit_snapshot(
    page: AsyncPage,
    cfg: TargetConfig,
    source_url: str,
    stats_date: str,
    *,
    navigate: bool = True,
) -> DailyStatsSnapshot:
    target_date = normalize_text(stats_date)
    if not target_date:
        raise ValueError("Не задана дата суточной статистики")

    if navigate:
        await page.goto(source_url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(1200)
    else:
        await page.wait_for_timeout(250)

    await ensure_async_authorized(page, cfg)
    await click_profit_tab(page)
    await click_profit_day_tab(page)

    for _ in range(DEFAULT_DAILY_STATS_LOOKUP_MAX_PAGES):
        daily_rows = await extract_daily_profit_rows(page)
        if not daily_rows:
            await page.wait_for_timeout(700)
            daily_rows = await extract_daily_profit_rows(page)
        for row in daily_rows:
            if normalize_text(row.get("stats_date", "")) != target_date:
                continue
            return DailyStatsSnapshot(
                dispatch_title=dispatch_title_from_url(source_url),
                stats_date=target_date,
                win_count=parse_int_from_text(row.get("win_count", "")),
                lose_count=parse_int_from_text(row.get("lose_count", "")),
                return_count=parse_int_from_text(row.get("return_count", "")),
                profit_percent=normalize_percent_value(row.get("profit_percent", "")),
                verifier_url=source_url,
            )
        moved = await go_to_next_daily_profit_page(page)
        if not moved:
            break

    raise DailyStatsNotFoundError(
        f"В таблице 'Прибыль' нет строки за {target_date}"
    )


async def parse_active_matches(
    page: AsyncPage,
    cfg: TargetConfig,
    source_url: str,
) -> list[ParsedMatch]:
    await click_active_tab(page)

    row_selectors = [
        "#tab-forecast-active .js-tab-forecast-active-list .rTableLine",
        "#tab-forecast-active .rTableBody .rTableLine",
        cfg.parse_item_selector,
        DEFAULT_PARSE_ITEM_SELECTOR,
        ".dispatch-row",
    ]
    panel_selector = normalize_text(cfg.panel_container_selector)

    candidate_selectors: list[str] = []
    for row_selector in row_selectors:
        if not row_selector:
            continue
        if panel_selector:
            candidate_selectors.extend(
                [
                    f"{panel_selector}.active-tab {row_selector}",
                    f"{panel_selector}.active {row_selector}",
                    f"{panel_selector} {row_selector}",
                ]
            )
            continue
        candidate_selectors.append(row_selector)

    # Убираем дубли селекторов, сохраняя порядок.
    candidate_selectors = list(dict.fromkeys(candidate_selectors))
    seen_in_batch: set[str] = set()

    script = """
(rows, panelSelector) => {
  const isVisible = (element) => {
    if (!element) {
      return false;
    }
    const style = window.getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
  };

  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
  const pickFromSrcset = (value) => {
    const normalized = normalize(value);
    if (!normalized) {
      return "";
    }
    const firstPart = normalized.split(",")[0] || "";
    const urlPart = (firstPart.trim().split(/\\s+/)[0] || "").trim();
    return urlPart;
  };
  const pickFromNoscript = (row) => {
    const noscriptNodes = Array.from(row.querySelectorAll(".cell-prognos noscript, noscript"));
    for (const noscriptNode of noscriptNodes) {
      const rawHtml = normalize(noscriptNode.textContent || "");
      if (!rawHtml) {
        continue;
      }
      const srcMatch = rawHtml.match(/src\\s*=\\s*["']([^"']+)["']/i);
      if (srcMatch && srcMatch[1]) {
        return normalize(srcMatch[1]);
      }
    }
    return "";
  };
  const isRealImageUrl = (value) => {
    const normalized = normalize(value).toLowerCase();
    if (!normalized) {
      return false;
    }
    if (normalized.startsWith("data:") || normalized.startsWith("blob:") || normalized.startsWith("about:")) {
      return false;
    }
    return true;
  };
  const pickImageUrl = (row) => {
    const selectors = [
      ".rTableHead.cell-prognos img.img-light",
      ".rTableHead.cell-prognos img",
      ".cell-prognos img",
      "img.lazy",
    ];
    const imageNodes = selectors
      .flatMap((selector) => Array.from(row.querySelectorAll(selector)));

    for (const node of imageNodes) {
      const srcsetCandidates = [
        pickFromSrcset(node.getAttribute("data-srcset")),
        pickFromSrcset(node.getAttribute("srcset")),
      ];
      for (const srcsetUrl of srcsetCandidates) {
        if (isRealImageUrl(srcsetUrl)) {
          return srcsetUrl;
        }
      }

      const attrCandidates = [
        node.getAttribute("data-src"),
        node.getAttribute("data-original"),
        node.getAttribute("data-lazy"),
        node.getAttribute("data-url"),
        node.getAttribute("data-image"),
        node.currentSrc,
        node.getAttribute("src"),
      ];
      for (const candidate of attrCandidates) {
        if (isRealImageUrl(candidate)) {
          return normalize(candidate);
        }
      }
    }

    const noscriptUrl = pickFromNoscript(row);
    if (isRealImageUrl(noscriptUrl)) {
      return noscriptUrl;
    }

    return "";
  };

  return rows
    .map((row) => {
      if (!isVisible(row)) {
        return null;
      }

      if (panelSelector) {
        const panel = row.closest(panelSelector);
        if (panel && !isVisible(panel)) {
          return null;
        }
      }

      const teams = Array.from(row.querySelectorAll(".cell-team-command"))
        .map((item) => normalize(item.textContent))
        .filter(Boolean);

      if (teams.length < 2) {
        return null;
      }

      const tournament = normalize(row.querySelector(".cell-team-tnm")?.textContent);
      const score = normalize(row.querySelector(".cell-team-score")?.textContent);
      let eventTime = normalize(
        row.querySelector(".time-event")?.textContent ||
        row.querySelector(".time-event__wrap .time-event")?.textContent
      );
      if (eventTime && score) {
        eventTime = normalize(eventTime.replace(score, " "));
      }
      const rate = normalize(
        row.querySelector(".cell-coefficient__total")?.textContent ||
        row.querySelector(".rate")?.textContent
      );
      const rateDescription = normalize(
        row.querySelector(".rate-description")?.textContent ||
        row.querySelector(".cell-type .type-live")?.textContent ||
        row.querySelector(".cell-type")?.textContent
      );
      const href = row.querySelector("a")?.getAttribute("href") || "";
      const imageUrl = pickImageUrl(row);

      if (!tournament) {
        return null;
      }

      return {
        home_team: teams[0],
        away_team: teams[1],
        tournament,
        event_time: eventTime,
        score,
        rate,
        rate_description: rateDescription,
        href,
        image_url: imageUrl,
      };
    })
    .filter(Boolean);
}
"""

    for selector in candidate_selectors:
        if not selector:
            continue

        try:
            raw_rows = await page.eval_on_selector_all(selector, script, panel_selector or None)
        except Exception:  # noqa: BLE001
            continue

        parsed: list[ParsedMatch] = []
        for row in raw_rows:
            if not isinstance(row, dict):
                continue

            home_team = normalize_text(str(row.get("home_team", "")))
            away_team = normalize_text(str(row.get("away_team", "")))
            tournament = normalize_text(str(row.get("tournament", "")))
            event_time = normalize_text(str(row.get("event_time", "")))
            score = normalize_text(str(row.get("score", "")))
            rate = normalize_text(str(row.get("rate", "")))
            rate_description = normalize_text(
                str(row.get("rate_description", "")))
            href = normalize_text(str(row.get("href", "")))
            image_url = normalize_text(str(row.get("image_url", "")))

            if not all([home_team, away_team, tournament]):
                continue

            full_href = urljoin(source_url, href) if href else source_url
            full_image_url = urljoin(source_url, image_url) if image_url else ""
            unique_key = "|".join(
                [home_team, away_team, tournament, rate, rate_description, full_href])

            if unique_key in seen_in_batch:
                continue
            seen_in_batch.add(unique_key)

            parsed.append(
                ParsedMatch(
                    home_team=home_team,
                    away_team=away_team,
                    tournament=tournament,
                    event_time=event_time,
                    score=score,
                    rate=rate,
                    rate_description=rate_description,
                    href=full_href,
                    image_url=full_image_url,
                    unique_key=unique_key,
                )
            )

        if parsed:
            return parsed

    return []


def split_parsed_matches_by_image(
    matches: list[ParsedMatch],
) -> tuple[list[ParsedMatch], list[ParsedMatch]]:
    matches_with_images: list[ParsedMatch] = []
    matches_without_images: list[ParsedMatch] = []

    for match in matches:
        if normalize_text(match.image_url):
            matches_with_images.append(match)
            continue
        matches_without_images.append(match)

    return matches_with_images, matches_without_images


def summarize_match_titles_for_log(
    matches: list[ParsedMatch],
    *,
    limit: int = 3,
) -> str:
    if not matches:
        return ""

    titles = [f"{match.home_team} - {match.away_team}" for match in matches[:limit]]
    if len(matches) > limit:
        titles.append(f"+{len(matches) - limit} more")
    return "; ".join(titles)


async def wait_for_active_match_images(
    page: AsyncPage,
    *,
    navigate: bool,
) -> None:
    scroll_steps = 60 if navigate else 8
    after_scroll_wait_ms = 1200 if navigate else 250
    image_wait_timeout_ms = 5000 if navigate else 1800

    # Прокручиваем страницу для lazy-load изображений прогноза.
    try:
        await page.evaluate(
            """
({ maxSteps }) => {
  const step = Math.max(Math.floor(window.innerHeight * 0.8), 300);
  let prevHeight = -1;
  let sameHeightTicks = 0;

  for (let i = 0; i < maxSteps; i += 1) {
    window.scrollBy(0, step);
    const currentHeight = Math.max(
      document.body.scrollHeight || 0,
      document.documentElement.scrollHeight || 0
    );
    if (currentHeight === prevHeight) {
      sameHeightTicks += 1;
      if (sameHeightTicks >= 3) {
        break;
      }
    } else {
      sameHeightTicks = 0;
      prevHeight = currentHeight;
    }
  }
}
""",
            {"maxSteps": scroll_steps},
        )
        await page.wait_for_timeout(after_scroll_wait_ms)
    except Exception:  # noqa: BLE001
        pass

    # Дожидаемся lazy-load картинок в блоке активных матчей.
    try:
        await page.evaluate(
            """
async ({ timeoutMs }) => {
  const normalize = (value) => (value || "").trim();
  const pickFromSrcset = (value) => {
    const normalized = normalize(value);
    if (!normalized) {
      return "";
    }
    const firstPart = normalized.split(",")[0] || "";
    return (firstPart.trim().split(/\\s+/)[0] || "").trim();
  };
  const pickFromNoscript = (img) => {
    const cell = img.closest(".cell-prognos");
    if (!cell) {
      return "";
    }
    const noscriptNode = cell.querySelector("noscript");
    if (!noscriptNode) {
      return "";
    }
    const rawHtml = normalize(noscriptNode.textContent || "");
    if (!rawHtml) {
      return "";
    }
    const srcMatch = rawHtml.match(/src\\s*=\\s*["']([^"']+)["']/i);
    if (!srcMatch || !srcMatch[1]) {
      return "";
    }
    return normalize(srcMatch[1]);
  };
  const isRealUrl = (value) => {
    const normalized = normalize(value).toLowerCase();
    if (!normalized) {
      return false;
    }
    if (normalized.startsWith("data:") || normalized.startsWith("blob:") || normalized.startsWith("about:")) {
      return false;
    }
    return true;
  };
  const pickCandidate = (img) => {
    const srcsetCandidates = [
      pickFromSrcset(img.getAttribute("data-srcset")),
      pickFromSrcset(img.getAttribute("srcset")),
    ];
    for (const candidate of srcsetCandidates) {
      if (isRealUrl(candidate)) {
        return candidate;
      }
    }

    const directCandidates = [
      img.getAttribute("data-src"),
      img.getAttribute("data-original"),
      img.getAttribute("data-lazy"),
      img.getAttribute("data-url"),
      img.getAttribute("data-image"),
      img.currentSrc,
      img.getAttribute("src"),
    ];
    for (const candidate of directCandidates) {
      if (isRealUrl(candidate)) {
        return normalize(candidate);
      }
    }
    const noscriptCandidate = pickFromNoscript(img);
    if (isRealUrl(noscriptCandidate)) {
      return noscriptCandidate;
    }
    return "";
  };

  const selectors = [
    "#tab-forecast-active .cell-prognos img",
    "#tab-forecast-active .rTableHead.cell-prognos img",
    ".dispatch-row .cell-prognos img",
    "#tab-forecast-active img.lazy",
  ];
  const images = selectors
    .flatMap((selector) => Array.from(document.querySelectorAll(selector)));

  if (!images.length) {
    return;
  }

  for (const img of images) {
    const candidate = pickCandidate(img);
    if (candidate) {
      const current = normalize(img.currentSrc || img.getAttribute("src") || "");
      if (!isRealUrl(current)) {
        img.setAttribute("src", candidate);
      }
    }
    img.loading = "eager";
    img.decoding = "sync";
    try {
      img.scrollIntoView({ block: "center", inline: "nearest" });
    } catch (error) {
      // no-op
    }
  }

  const endAt = Date.now() + timeoutMs;
  while (Date.now() < endAt) {
    let pending = 0;
    for (const img of images) {
      const candidate = pickCandidate(img);
      const current = normalize(img.currentSrc || img.getAttribute("src") || "");
      if (!isRealUrl(current) && candidate) {
        img.setAttribute("src", candidate);
      }
      const finalSrc = normalize(img.currentSrc || img.getAttribute("src") || "");
      if (!isRealUrl(finalSrc)) {
        pending += 1;
      }
    }

    if (pending === 0) {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}
""",
            {"timeoutMs": image_wait_timeout_ms},
        )
    except Exception:  # noqa: BLE001
        pass


async def load_active_matches_once(
    page: AsyncPage,
    cfg: TargetConfig,
    parse_url: str,
    *,
    navigate: bool,
) -> list[ParsedMatch]:
    parse_url_with_limit = upsert_query_param(
        parse_url,
        "ForecastSearch[perPage]",
        "40",
    )

    if navigate:
        await page.goto(parse_url_with_limit, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(1500)
    else:
        await page.wait_for_timeout(200)

    await ensure_async_authorized(page, cfg)
    await click_active_tab(page)
    await ensure_show_by_40(page, force_click=navigate)
    await ensure_scan_page_prefers_second(page, parse_url_with_limit)
    await ensure_async_authorized(page, cfg)
    await click_active_tab(page)
    await ensure_show_by_40(page, force_click=True)
    await wait_for_active_match_images(page, navigate=navigate)

    matches = await parse_active_matches(page, cfg, parse_url)
    if not matches:
        await ensure_async_authorized(page, cfg)
    return matches


async def parse_completed_matches(
    page: AsyncPage,
    cfg: TargetConfig,
    source_url: str,
) -> list[SettledMatchSnapshot]:
    await click_completed_tab(page)

    row_selectors = [
        "#tab-forecast-zip .js-tab-forecast-zip-list .rTableLine",
        "#tab-forecast-zip .rTableBody .rTableLine",
        ".js-tab-forecast-zip-list .rTableLine",
        ".rTable_forecast #tab-forecast-zip .rTableLine",
        ".rTable_forecast .js-tab-forecast-zip-list .rTableLine",
    ]
    panel_selector = normalize_text(cfg.panel_container_selector)

    candidate_selectors: list[str] = []
    for row_selector in row_selectors:
        if not row_selector:
            continue
        if panel_selector:
            candidate_selectors.extend(
                [
                    f"{panel_selector}.active-tab {row_selector}",
                    f"{panel_selector}.active {row_selector}",
                    f"{panel_selector} {row_selector}",
                ]
            )
            continue
        candidate_selectors.append(row_selector)

    candidate_selectors = list(dict.fromkeys(candidate_selectors))
    seen_in_batch: set[str] = set()

    script = """
(rows, panelSelector) => {
  const isVisible = (element) => {
    if (!element) {
      return false;
    }
    const style = window.getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
  };
  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();

  return rows
    .map((row) => {
      if (!isVisible(row)) {
        return null;
      }
      if (panelSelector) {
        const panel = row.closest(panelSelector);
        if (panel && !isVisible(panel)) {
          return null;
        }
      }

      const teams = Array.from(row.querySelectorAll(".cell-team-command"))
        .map((item) => normalize(item.textContent))
        .filter(Boolean);
      if (teams.length < 2) {
        return null;
      }

      const tournament = normalize(row.querySelector(".cell-team-tnm")?.textContent);
      if (!tournament) {
        return null;
      }

      const rate = normalize(
        row.querySelector(".cell-coefficient__total")?.textContent ||
        row.querySelector(".rate")?.textContent
      );
      const rateDescription = normalize(
        row.querySelector(".rate-description")?.textContent ||
        row.querySelector(".cell-type .type-live")?.textContent ||
        row.querySelector(".cell-type")?.textContent
      );
      const href = row.querySelector(".cell-oboroty a")?.getAttribute("href") || row.querySelector("a")?.getAttribute("href") || "";
      const score = normalize(row.querySelector(".cell-count")?.textContent);

      const resultNode = row.querySelector(
        ".cell-subscribers-forcast span, .cell-subscribers.cell-subscribers-forcast span, .cell-subscribers span"
      );
      const resultText = normalize(resultNode?.textContent);
      const resultClass = normalize(resultNode?.className || "");

      return {
        home_team: teams[0],
        away_team: teams[1],
        tournament,
        rate,
        rate_description: rateDescription,
        href,
        score,
        result_text: resultText,
        result_class: resultClass,
      };
    })
    .filter(Boolean);
}
"""

    for selector in candidate_selectors:
        if not selector:
            continue

        try:
            raw_rows = await page.eval_on_selector_all(selector, script, panel_selector or None)
        except Exception:  # noqa: BLE001
            continue

        parsed: list[SettledMatchSnapshot] = []
        for row in raw_rows:
            if not isinstance(row, dict):
                continue

            home_team = normalize_text(str(row.get("home_team", "")))
            away_team = normalize_text(str(row.get("away_team", "")))
            tournament = normalize_text(str(row.get("tournament", "")))
            rate = normalize_text(str(row.get("rate", "")))
            rate_description = normalize_text(str(row.get("rate_description", "")))
            href = normalize_text(str(row.get("href", "")))
            score = normalize_text(str(row.get("score", "")))
            result_text = normalize_text(str(row.get("result_text", "")))
            result_class = normalize_text(str(row.get("result_class", "")))

            if not all([home_team, away_team, tournament]):
                continue

            full_href = urljoin(source_url, href) if href else source_url
            unique_key = "|".join(
                [
                    home_team,
                    away_team,
                    tournament,
                    rate,
                    rate_description,
                    full_href,
                ]
            )
            if unique_key in seen_in_batch:
                continue
            seen_in_batch.add(unique_key)

            outcome = extract_settlement_outcome(result_text, result_class)
            if outcome is None:
                # В "Прошедших" исход может появляться с задержкой.
                # Пока нет явного результата — ждем следующий цикл.
                continue
            net_profit_units, settlement_status = outcome
            parsed.append(
                SettledMatchSnapshot(
                    home_team=home_team,
                    away_team=away_team,
                    tournament=tournament,
                    rate=rate,
                    rate_description=rate_description,
                    href=full_href,
                    unique_key=unique_key,
                    match_signature=compose_match_signature(
                        home_team,
                        away_team,
                        tournament,
                        rate,
                        rate_description,
                    ),
                    score=score,
                    net_profit_units=net_profit_units,
                    settlement_status=settlement_status,
                )
            )

        if parsed:
            return parsed

    return []


async def fetch_completed_matches(
    page: AsyncPage,
    cfg: TargetConfig,
    parse_url: str,
    *,
    navigate: bool = False,
) -> list[SettledMatchSnapshot]:
    if navigate:
        await page.goto(parse_url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(1200)
    else:
        await page.wait_for_timeout(200)

    await ensure_async_authorized(page, cfg)
    return await parse_completed_matches(page, cfg, parse_url)


async def fetch_active_matches(
    page: AsyncPage,
    cfg: TargetConfig,
    parse_url: str,
    *,
    navigate: bool = True,
) -> list[ParsedMatch]:
    retry_plan = [
        ("initial", navigate, 0),
        ("wait", False, ACTIVE_MATCH_IMAGE_RETRY_WAIT_MS),
        ("reload", True, 0),
        ("reload-final", True, 0),
    ]
    best_matches: list[ParsedMatch] = []
    last_missing_matches: list[ParsedMatch] = []

    for attempt_number, (action, should_navigate, pre_wait_ms) in enumerate(retry_plan, start=1):
        if pre_wait_ms > 0:
            try:
                await page.wait_for_timeout(pre_wait_ms)
            except Exception:  # noqa: BLE001
                pass

        matches = await load_active_matches_once(
            page,
            cfg,
            parse_url,
            navigate=should_navigate,
        )
        if not matches:
            if attempt_number < len(retry_plan):
                continue
            return best_matches

        matches_with_images, matches_without_images = split_parsed_matches_by_image(matches)
        if matches_with_images:
            best_matches = matches_with_images

        if not matches_without_images:
            if attempt_number > 1:
                logger.info(
                    "Картинки активных матчей успешно подтянулись. source=%s attempt=%s/%s action=%s",
                    parse_url,
                    attempt_number,
                    len(retry_plan),
                    action,
                )
            return matches

        last_missing_matches = matches_without_images

    if last_missing_matches:
        logger.error(
            "Матчи без картинки пропущены до следующего цикла. source=%s missing=%s matches=%s",
            parse_url,
            len(last_missing_matches),
            summarize_match_titles_for_log(last_missing_matches),
        )
    return best_matches


def humanize_parser_error(exc: Exception) -> str:
    raw = normalize_text(str(exc))
    lowered = raw.lower()

    if isinstance(exc, DailyStatsNotFoundError):
        return raw or "Нет данных суточной статистики за выбранную дату."
    if isinstance(exc, WeeklyStatsNotFoundError):
        return raw or "Нет данных недельной статистики за выбранный период."
    if isinstance(exc, MonthlyStatsNotFoundError):
        return raw or "Нет данных месячной статистики за выбранный период."
    if isinstance(exc, LoginRequiredError):
        return "Сайт снова показывает форму входа. Выполни вход заново."
    if "cannot switch to a different thread" in lowered:
        return "Ошибка потоков браузера. Перезапусти парсер."
    if "target page, context or browser has been closed" in lowered or "has been closed" in lowered:
        return "Сессия браузера закрыта. Выполни вход заново."
    if "timeout" in lowered:
        return "Таймаут при загрузке страницы или получении данных."
    if not raw:
        return "Неизвестная ошибка парсера."
    return f"Техническая ошибка парсера: {raw}"


@dataclass(frozen=True)
class SourceFetchResult:
    source: ParserSource
    matches: tuple[ParsedMatch, ...]
    navigate_mode: str
    error: Optional[Exception] = None


async def fetch_matches_for_source(
    parser_context: Any,
    source_pages: dict[str, SourcePageRuntime],
    cfg: TargetConfig,
    source: ParserSource,
    parser_page_max_age_seconds: int,
) -> SourceFetchResult:
    source_runtime = source_pages.get(source.source_id)
    previous_match_count = 0
    page_age_seconds = 0.0
    needs_navigation = True
    mode = "navigate"

    try:
        if source_runtime is not None:
            previous_match_count = source_runtime.last_match_count
            if source_runtime.page.is_closed():
                mode = "reopen"
                needs_navigation = True
            else:
                page_age_seconds = (
                    time.monotonic() - source_runtime.created_at_monotonic
                )
                if page_age_seconds >= parser_page_max_age_seconds:
                    mode = "rotate"
                    needs_navigation = True
                else:
                    mode = "live"
                    needs_navigation = False

        if needs_navigation:
            reopen_required = (
                source_runtime is None
                or source_runtime.page.is_closed()
            )
            if reopen_required:
                source_runtime = SourcePageRuntime(
                    page=await parser_context.new_page(),
                    created_at_monotonic=time.monotonic(),
                    last_match_count=previous_match_count,
                )
                source_pages[source.source_id] = source_runtime
            elif mode == "rotate":
                logger.info(
                    "Перезагружаю страницу источника по TTL. source=%s age=%.1fs ttl=%ss",
                    source.url,
                    page_age_seconds,
                    parser_page_max_age_seconds,
                )

        if source_runtime is None:
            raise RuntimeError("Не удалось открыть страницу источника")

        source_matches = await fetch_active_matches(
            source_runtime.page,
            cfg,
            source.url,
            navigate=needs_navigation,
        )
        if needs_navigation:
            source_runtime.created_at_monotonic = time.monotonic()

        if (
            mode == "live"
            and previous_match_count > 0
            and not source_matches
        ):
            logger.warning(
                "Источник внезапно вернул 0 матчей в live-режиме, запускаю recovery. source=%s previous=%s",
                source.url,
                previous_match_count,
            )
            source_matches = await fetch_active_matches(
                source_runtime.page,
                cfg,
                source.url,
                navigate=True,
            )
            source_runtime.created_at_monotonic = time.monotonic()
            mode = "recovery"
            if not source_matches:
                logger.warning(
                    "Recovery через reload не вернул матчи, пересоздаю вкладку источника. source=%s",
                    source.url,
                )
                try:
                    await source_runtime.page.close()
                except Exception:  # noqa: BLE001
                    pass

                source_runtime = SourcePageRuntime(
                    page=await parser_context.new_page(),
                    created_at_monotonic=time.monotonic(),
                    last_match_count=previous_match_count,
                )
                source_pages[source.source_id] = source_runtime
                source_matches = await fetch_active_matches(
                    source_runtime.page,
                    cfg,
                    source.url,
                    navigate=True,
                )
                source_runtime.created_at_monotonic = time.monotonic()
                mode = "recovery_reopen"

        source_runtime.last_match_count = len(source_matches)
        logger.info(
            "Цикл парсера: источник=%s, tg_chat_id=%s, vk_chat_ids=%s, найдено матчей=%s, mode=%s",
            source.url,
            source.chat_id,
            len(source.vk_chat_ids),
            len(source_matches),
            mode,
        )
        return SourceFetchResult(
            source=source,
            matches=tuple(source_matches),
            navigate_mode=mode,
            error=None,
        )
    except Exception as source_exc:  # noqa: BLE001
        logger.exception("Ошибка парсинга источника. url=%s", source.url)
        bad_runtime = source_pages.pop(source.source_id, None)
        if bad_runtime is not None:
            try:
                await bad_runtime.page.close()
            except Exception:  # noqa: BLE001
                pass
        return SourceFetchResult(
            source=source,
            matches=(),
            navigate_mode=mode,
            error=source_exc,
        )


async def deliver_match_notification(
    tg_session: aiohttp.ClientSession,
    tg_cfg: TelegramConfig,
    vk_session: Optional[aiohttp.ClientSession],
    vk_cfg: Optional[VkConfig],
    blogabet_publisher: Optional[BlogabetPublisher],
    blogabet_cfg: Optional[BlogabetConfig],
    ocr_client: Optional[OcrSpaceClient],
    match_store: MatchTrackingStore,
    match: ParsedMatch,
    source: ParserSource,
    target_kind: str,
    target_chat_id: str,
    delivery_key: str,
) -> None:
    message = build_active_match_message(match, source.url)
    telegram_message_html = build_active_match_message_html(match, source.url)
    platform_label = stats_target_label(target_kind)
    blogabet_bet_raw = match.rate_description
    stored_message_text = message

    try:
        if target_kind == "telegram":
            if not normalize_text(match.image_url):
                raise RuntimeError("У матча отсутствует обязательное изображение")
            message_id = await send_telegram_match_message(
                tg_session,
                tg_cfg,
                target_chat_id,
                telegram_message_html,
                image_url=match.image_url,
                parse_mode="HTML",
                require_photo=True,
            )
            stored_message_text = message
        elif target_kind == "vk":
            if vk_session is None or vk_cfg is None:
                raise RuntimeError("VK конфигурация не загружена")
            message_id = await send_vk_match_message(
                vk_session,
                vk_cfg,
                target_chat_id,
                message,
                image_url=match.image_url,
            )
            stored_message_text = message
        elif target_kind == "blogabet":
            if blogabet_cfg is None or blogabet_publisher is None:
                raise RuntimeError("Blogabet конфигурация не загружена")
            if ocr_client is None:
                raise RuntimeError("OCR клиент не инициализирован")
            if not normalize_text(match.image_url):
                raise RuntimeError("У матча отсутствует изображение для OCR")

            image_bytes, content_type = await download_image_bytes(tg_session, match.image_url)
            ocr_text = await ocr_client.recognize_text_from_image_bytes(
                image_bytes,
                content_type,
                cache_key=normalize_text(match.image_url) or None,
            )
            bet_intent = parse_bet_intent(ocr_text)
            blogabet_bet_raw = bet_intent.raw_text
            analysis_text = build_blogabet_analysis_text(match, bet_intent, ocr_text)
            publish_result = await blogabet_publisher.publish_pick(
                match,
                bet_intent,
                blogabet_cfg.default_stake,
                analysis_text,
                diagnostics_context={
                    "ocr_text": ocr_text,
                    "match_href": match.href,
                    "source_url": source.url,
                },
            )

            if not publish_result.success:
                raise RuntimeError("Blogabet вернул неуспешный результат публикации")

            pick_id = publish_result.pick_id or 0
            pick_url = normalize_text(publish_result.pick_url or "")
            message_id = pick_id
            stored_message_text = pick_url or (
                f"Blogabet pick for {match.home_team} - {match.away_team}"
            )
        else:
            raise RuntimeError(f"Неизвестный тип канала доставки: {target_kind}")

        logger.info(
            "Сообщение отправлено. platform=%s chat_id=%s message_id=%s source=%s match=%s - %s",
            platform_label,
            target_chat_id,
            message_id,
            source.url,
            match.home_team,
            match.away_team,
        )
        match_store.upsert_sent_delivery(
            source,
            match,
            target_kind,
            target_chat_id,
            delivery_key,
            message_id,
            stored_message_text,
            now_storage_label_msk(),
        )
        with state.lock:
            state.preview = stored_message_text + (
                f"\nВложение: {match.image_url}" if match.image_url else ""
            ) + f"\nПлатформа: {platform_label}\nЧат: {target_chat_id}"
            state.last_message_id = message_id if message_id > 0 else state.last_message_id
            state.parser_last_sent_at = now_label()
            state.parser_last_match_title = f"{match.home_team} - {match.away_team}"
    except Exception as exc:  # noqa: BLE001
        error_details = humanize_parser_error(exc)
        if isinstance(exc, BlogabetPublishError):
            error_details = f"{exc.step_name}: {exc.reason}"

        if target_kind == "blogabet":
            log_blogabet_exception(
                "Ошибка отправки уведомления Blogabet. platform=%s match=%s source=%s chat_id=%s error=%s",
                platform_label,
                match.unique_key,
                source.url,
                target_chat_id,
                error_details,
            )
        else:
            logger.exception(
                "Ошибка отправки уведомления. platform=%s match=%s source=%s chat_id=%s",
                platform_label,
                match.unique_key,
                source.url,
                target_chat_id,
            )

        if target_kind == "blogabet":
            try:
                match_store.upsert_failed_delivery(
                    source,
                    match,
                    target_kind,
                    target_chat_id,
                    delivery_key,
                    error_details,
                    now_storage_label_msk(),
                )
            except Exception:  # noqa: BLE001
                log_blogabet_exception("Не удалось сохранить failed-доставку Blogabet в БД")

            raw_admin_chat_ids = blogabet_cfg.admin_tg_chat_id if blogabet_cfg else ""
            admin_chat_ids: tuple[str, ...] = ()
            try:
                admin_chat_ids = parse_telegram_chat_ids(raw_admin_chat_ids)
            except ValueError as admin_exc:
                log_blogabet_error(
                    "BLOGABET_ADMIN_TG_CHAT_ID содержит невалидный chat_id: %s",
                    admin_exc,
                )

            if admin_chat_ids:
                alert_message = build_blogabet_admin_alert_message(
                    match,
                    blogabet_bet_raw,
                    error_details,
                )
                for admin_chat_id in admin_chat_ids:
                    try:
                        await send_telegram_match_message(
                            tg_session,
                            tg_cfg,
                            admin_chat_id,
                            alert_message,
                        )
                    except Exception:  # noqa: BLE001
                        log_blogabet_exception(
                            "Не удалось отправить алерт админу о сбое Blogabet. chat_id=%s",
                            admin_chat_id,
                        )

        with state.lock:
            state.parser_error = (
                f"{now_label()} | Ошибка отправки в {platform_label} ({target_chat_id}): "
                f"{error_details}"
            )
    finally:
        with state.lock:
            state.pending_match_keys.discard(delivery_key)


def reserve_settlement_candidates(
    match_store: MatchTrackingStore,
    source_id: str,
) -> list[SentMatchRecord]:
    candidates: list[SentMatchRecord] = []
    candidate_rows = match_store.list_pending_settlement_candidates(source_id)
    if not candidate_rows:
        return candidates

    with state.lock:
        for row in candidate_rows:
            delivery_key = normalize_text(str(row.get("delivery_key", "")))
            if not delivery_key:
                continue
            if delivery_key in state.pending_settlement_keys:
                continue
            try:
                message_id = int(row.get("message_id", 0))
            except (TypeError, ValueError):
                continue
            if message_id <= 0:
                continue
            tracked_status = normalize_text(str(row.get("tracked_status", ""))).lower()
            tracked_settlement_status = normalize_text(
                str(row.get("tracked_settlement_status", ""))
            ).lower()
            try:
                tracked_settlement_profit_units = int(
                    row.get("tracked_settlement_profit_units", 0)
                )
            except (TypeError, ValueError):
                tracked_settlement_profit_units = 0
            tracked_settlement_score = normalize_text(
                str(row.get("tracked_settlement_score", ""))
            )

            state.pending_settlement_keys.add(delivery_key)
            candidates.append(
                SentMatchRecord(
                    delivery_key=delivery_key,
                    source_id=normalize_text(str(row.get("source_id", ""))),
                    source_url=normalize_text(str(row.get("source_url", ""))),
                    chat_id=normalize_text(str(row.get("chat_id", ""))),
                    message_id=message_id,
                    message_text=str(row.get("message_text", "")),
                    match_unique_key=normalize_text(str(row.get("match_unique_key", ""))),
                    match_signature=normalize_text(str(row.get("match_signature", ""))),
                    match_href=normalize_text(str(row.get("match_href", ""))),
                    match_lookup_key=normalize_text(str(row.get("match_lookup_key", ""))),
                    home_team=normalize_text(str(row.get("home_team", ""))),
                    away_team=normalize_text(str(row.get("away_team", ""))),
                    tracked_status=tracked_status,
                    tracked_settlement_status=tracked_settlement_status,
                    tracked_settlement_profit_units=tracked_settlement_profit_units,
                    tracked_settlement_score=tracked_settlement_score,
                )
            )
    return candidates


def release_pending_settlement_keys(delivery_keys: list[str]) -> None:
    if not delivery_keys:
        return
    with state.lock:
        for delivery_key in delivery_keys:
            state.pending_settlement_keys.discard(delivery_key)


def build_settlement_lookup_indexes(
    settled_matches: list[SettledMatchSnapshot],
) -> tuple[
    dict[str, SettledMatchSnapshot],
    dict[str, SettledMatchSnapshot],
    dict[str, SettledMatchSnapshot],
]:
    by_lookup_key: dict[str, SettledMatchSnapshot] = {}
    by_unique_key: dict[str, SettledMatchSnapshot] = {}
    by_signature: dict[str, SettledMatchSnapshot] = {}
    for settled_match in settled_matches:
        lookup_key = build_match_lookup_key(settled_match.href)
        if lookup_key:
            by_lookup_key[lookup_key] = settled_match
        by_unique_key[settled_match.unique_key] = settled_match
        if settled_match.match_signature:
            by_signature[settled_match.match_signature] = settled_match
    return by_lookup_key, by_unique_key, by_signature


async def deliver_settlement_update(
    tg_session: aiohttp.ClientSession,
    tg_cfg: TelegramConfig,
    match_store: MatchTrackingStore,
    record: SentMatchRecord,
    settled_match: SettledMatchSnapshot,
) -> None:
    footer_line = build_settlement_footer_line(
        settled_match.settlement_status,
        settled_match.net_profit_units,
        settled_match.score,
    )
    updated_message = append_settlement_footer(record.message_text, footer_line)

    try:
        log_message_edit_info(
            "Пробую обновить исход ставки. delivery_key=%s message_id=%s source=%s chat_id=%s match=%s - %s status=%s",
            record.delivery_key,
            record.message_id,
            record.source_url,
            record.chat_id,
            record.home_team,
            record.away_team,
            settled_match.settlement_status,
        )
        edit_result = await edit_telegram_message(
            tg_session,
            tg_cfg,
            record.chat_id,
            record.message_id,
            updated_message,
            match_url=record.match_href,
        )
        match_store.mark_settlement_success(
            record,
            settled_match,
            updated_message,
            now_storage_label_msk(),
        )
        log_message_edit_info(
            "Обновлен исход ставки. delivery_key=%s message_id=%s source=%s match=%s - %s status=%s edit_result=%s",
            record.delivery_key,
            record.message_id,
            record.source_url,
            record.home_team,
            record.away_team,
            settled_match.settlement_status,
            edit_result,
        )
        with state.lock:
            state.preview = updated_message + f"\nКанал: {record.chat_id}"
            state.last_message_id = record.message_id
            state.parser_last_settled_at = now_label()
            state.parser_last_settled_title = f"{record.home_team} - {record.away_team}"
    except Exception as exc:  # noqa: BLE001
        log_message_edit_exception(
            "Ошибка обновления исхода ставки. delivery_key=%s message_id=%s source=%s chat_id=%s",
            record.delivery_key,
            record.message_id,
            record.source_url,
            record.chat_id,
        )
        error_message = humanize_parser_error(exc)
        try:
            match_store.mark_settlement_error(
                record,
                error_message,
                now_storage_label_msk(),
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "Не удалось сохранить ошибку обновления исхода в БД. delivery_key=%s",
                record.delivery_key,
            )
        with state.lock:
            state.parser_error = (
                f"{now_label()} | Ошибка обновления исхода ({record.chat_id}): "
                f"{error_message}"
            )
    finally:
        with state.lock:
            state.pending_settlement_keys.discard(record.delivery_key)


async def schedule_settlement_updates_for_source(
    parser_context: Any,
    tg_session: aiohttp.ClientSession,
    tg_cfg: TelegramConfig,
    match_store: MatchTrackingStore,
    cfg: TargetConfig,
    source: ParserSource,
    active_matches: tuple[ParsedMatch, ...],
    settlement_tasks: set[asyncio.Task[None]],
    completed_fetch_last_at: dict[str, float],
    completed_fetch_interval_seconds: int,
) -> int:
    active_match_keys = {
        match.unique_key
        for match in active_matches
    }
    disappeared_count = match_store.mark_disappeared_matches(
        source.source_id,
        active_match_keys,
        now_storage_label_msk(),
    )
    candidates = reserve_settlement_candidates(match_store, source.source_id)
    has_disappeared = match_store.has_disappeared_matches(source.source_id)
    should_check_completed_now = disappeared_count > 0
    now_monotonic = time.monotonic()
    interval_seconds = max(completed_fetch_interval_seconds, 10)
    last_completed_fetch_at = completed_fetch_last_at.get(source.source_id, 0.0)
    completed_fetch_due = (now_monotonic - last_completed_fetch_at) >= interval_seconds

    if should_check_completed_now:
        logger.info(
            "Найдены новые disappeared-матчи, запускаю немедленную проверку 'Прошедшие'. source=%s new_disappeared=%s",
            source.url,
            disappeared_count,
        )

    if not candidates and not has_disappeared and not should_check_completed_now:
        return 0

    if not should_check_completed_now and not completed_fetch_due:
        release_pending_settlement_keys([record.delivery_key for record in candidates])
        return 0

    completed_page: Optional[AsyncPage] = None
    try:
        completed_page = await parser_context.new_page()
        settled_matches = await fetch_completed_matches(
            completed_page,
            cfg,
            source.url,
            navigate=True,
        )
        completed_fetch_last_at[source.source_id] = time.monotonic()
    except Exception:
        completed_fetch_last_at[source.source_id] = time.monotonic()
        release_pending_settlement_keys([record.delivery_key for record in candidates])
        raise
    finally:
        if completed_page is not None:
            try:
                await completed_page.close()
            except Exception:  # noqa: BLE001
                pass

    if settled_matches:
        reconciled_count = match_store.reconcile_disappeared_matches(
            source.source_id,
            settled_matches,
            now_storage_label_msk(),
        )
        if reconciled_count > 0:
            logger.info(
                "Сверка disappeared->settled выполнена. source=%s updated=%s",
                source.url,
                reconciled_count,
            )

    if not candidates:
        return 0

    by_lookup_key, by_unique_key, by_signature = build_settlement_lookup_indexes(
        settled_matches
    )
    unmatched_keys: list[str] = []
    unmatched_records: list[str] = []
    scheduled_count = 0

    for record in candidates:
        settled_match: Optional[SettledMatchSnapshot] = None
        if record.match_lookup_key:
            settled_match = by_lookup_key.get(record.match_lookup_key)
        if settled_match is None:
            settled_match = by_unique_key.get(record.match_unique_key)
        if settled_match is None and record.match_signature:
            settled_match = by_signature.get(record.match_signature)
        if settled_match is None and record.tracked_status == "settled":
            if record.tracked_settlement_status in {"win", "lose", "return"}:
                settled_match = SettledMatchSnapshot(
                    home_team=record.home_team,
                    away_team=record.away_team,
                    tournament="",
                    rate="",
                    rate_description="",
                    href=record.match_href or record.source_url,
                    unique_key=record.match_unique_key,
                    match_signature=record.match_signature,
                    score=record.tracked_settlement_score,
                    net_profit_units=record.tracked_settlement_profit_units,
                    settlement_status=record.tracked_settlement_status,
                )
                logger.info(
                    "Кандидат исхода взят из БД. delivery_key=%s message_id=%s source=%s status=%s profit_units=%s",
                    record.delivery_key,
                    record.message_id,
                    source.url,
                    settled_match.settlement_status,
                    settled_match.net_profit_units,
                )

        if settled_match is None:
            unmatched_keys.append(record.delivery_key)
            unmatched_records.append(f"{record.delivery_key}|message_id={record.message_id}")
            continue

        logger.info(
            "Кандидат исхода сопоставлен. delivery_key=%s message_id=%s source=%s status=%s profit_units=%s",
            record.delivery_key,
            record.message_id,
            source.url,
            settled_match.settlement_status,
            settled_match.net_profit_units,
        )
        settlement_task = asyncio.create_task(
            deliver_settlement_update(
                tg_session,
                tg_cfg,
                match_store,
                record,
                settled_match,
            )
        )
        settlement_tasks.add(settlement_task)
        scheduled_count += 1

    release_pending_settlement_keys(unmatched_keys)
    if unmatched_records:
        preview = "; ".join(unmatched_records[:5])
        if len(unmatched_records) > 5:
            preview += "; ..."
        logger.info(
            "Кандидаты исхода пока не найдены в разделе 'Прошедшие'. source=%s count=%s candidates=%s",
            source.url,
            len(unmatched_records),
            preview,
        )
    return scheduled_count


async def deliver_daily_stats_notification(
    tg_session: Optional[aiohttp.ClientSession],
    tg_cfg: Optional[TelegramConfig],
    vk_session: Optional[aiohttp.ClientSession],
    vk_cfg: Optional[VkConfig],
    cfg: TargetConfig,
    page: AsyncPage,
    source: ParserSource,
    stats_date: str,
    delivery_targets: list[tuple[str, str, str]],
) -> None:
    try:
        try:
            daily_snapshot = await fetch_daily_profit_snapshot(
                page,
                cfg,
                source.url,
                stats_date,
                navigate=False,
            )
            message = build_daily_stats_message(daily_snapshot)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Ошибка подготовки суточной статистики. source=%s date=%s",
                source.url,
                stats_date,
            )
            with state.lock:
                state.parser_error = (
                    f"{now_label()} | Ошибка суточной статистики "
                    f"({source.url}): {humanize_parser_error(exc)}"
                )
            return

        for target_kind, target_chat_id, target_key in delivery_targets:
            platform_label = stats_target_label(target_kind)
            try:
                message_id = await send_stats_message_to_target(
                    tg_session,
                    tg_cfg,
                    vk_session,
                    vk_cfg,
                    target_kind,
                    target_chat_id,
                    message,
                )
                with state.lock:
                    state.daily_stats_sent_by_source[target_key] = stats_date
                    state.preview = (
                        message
                        + f"\nПлатформа: {platform_label}"
                        + f"\nЧат: {target_chat_id}"
                    )
                    state.last_message_id = message_id
                    state.parser_last_daily_sent_at = now_label_msk()
                    state.parser_last_daily_date = stats_date
                    state.parser_last_daily_title = daily_snapshot.dispatch_title
                logger.info(
                    "Суточная статистика отправлена. source=%s platform=%s chat_id=%s date=%s",
                    source.url,
                    platform_label,
                    target_chat_id,
                    stats_date,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Ошибка отправки суточной статистики. source=%s platform=%s chat_id=%s date=%s",
                    source.url,
                    platform_label,
                    target_chat_id,
                    stats_date,
                )
                with state.lock:
                    state.parser_error = (
                        f"{now_label()} | Ошибка суточной статистики "
                        f"({platform_label} {target_chat_id}): {humanize_parser_error(exc)}"
                    )
    finally:
        with state.lock:
            for _, _, target_key in delivery_targets:
                state.daily_stats_inflight_sources.discard(target_key)


async def deliver_weekly_stats_notification(
    tg_session: Optional[aiohttp.ClientSession],
    tg_cfg: Optional[TelegramConfig],
    vk_session: Optional[aiohttp.ClientSession],
    vk_cfg: Optional[VkConfig],
    cfg: TargetConfig,
    page: AsyncPage,
    source: ParserSource,
    week_start: date,
    week_end: date,
    period_key: str,
    delivery_targets: list[tuple[str, str, str]],
) -> None:
    try:
        try:
            weekly_snapshot = await fetch_weekly_profit_snapshot(
                page,
                cfg,
                source.url,
                week_start,
                week_end,
                navigate=False,
            )
            message = build_weekly_stats_message(weekly_snapshot)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Ошибка подготовки недельной статистики. source=%s period=%s",
                source.url,
                period_key,
            )
            with state.lock:
                state.parser_error = (
                    f"{now_label()} | Ошибка недельной статистики "
                    f"({source.url}): {humanize_parser_error(exc)}"
                )
            return

        for target_kind, target_chat_id, target_key in delivery_targets:
            platform_label = stats_target_label(target_kind)
            try:
                message_id = await send_stats_message_to_target(
                    tg_session,
                    tg_cfg,
                    vk_session,
                    vk_cfg,
                    target_kind,
                    target_chat_id,
                    message,
                )
                with state.lock:
                    state.weekly_stats_sent_by_source[target_key] = period_key
                    state.preview = (
                        message
                        + f"\nПлатформа: {platform_label}"
                        + f"\nЧат: {target_chat_id}"
                    )
                    state.last_message_id = message_id
                    state.parser_last_weekly_sent_at = now_label_msk()
                    state.parser_last_weekly_period = weekly_snapshot.period_label
                    state.parser_last_weekly_title = weekly_snapshot.dispatch_title
                logger.info(
                    "Недельная статистика отправлена. source=%s platform=%s chat_id=%s period=%s",
                    source.url,
                    platform_label,
                    target_chat_id,
                    period_key,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Ошибка отправки недельной статистики. source=%s platform=%s chat_id=%s period=%s",
                    source.url,
                    platform_label,
                    target_chat_id,
                    period_key,
                )
                with state.lock:
                    state.parser_error = (
                        f"{now_label()} | Ошибка недельной статистики "
                        f"({platform_label} {target_chat_id}): {humanize_parser_error(exc)}"
                    )
    finally:
        with state.lock:
            for _, _, target_key in delivery_targets:
                state.weekly_stats_inflight_sources.discard(target_key)


async def deliver_monthly_stats_notification(
    tg_session: Optional[aiohttp.ClientSession],
    tg_cfg: Optional[TelegramConfig],
    vk_session: Optional[aiohttp.ClientSession],
    vk_cfg: Optional[VkConfig],
    cfg: TargetConfig,
    page: AsyncPage,
    source: ParserSource,
    target_year: int,
    target_month: int,
    period_key: str,
    delivery_targets: list[tuple[str, str, str]],
) -> None:
    try:
        try:
            monthly_snapshot = await fetch_monthly_profit_snapshot(
                page,
                cfg,
                source.url,
                target_year,
                target_month,
                navigate=False,
            )
            message = build_monthly_stats_message(monthly_snapshot)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Ошибка подготовки месячной статистики. source=%s period=%s",
                source.url,
                period_key,
            )
            with state.lock:
                state.parser_error = (
                    f"{now_label()} | Ошибка месячной статистики "
                    f"({source.url}): {humanize_parser_error(exc)}"
                )
            return

        for target_kind, target_chat_id, target_key in delivery_targets:
            platform_label = stats_target_label(target_kind)
            try:
                message_id = await send_stats_message_to_target(
                    tg_session,
                    tg_cfg,
                    vk_session,
                    vk_cfg,
                    target_kind,
                    target_chat_id,
                    message,
                )
                with state.lock:
                    state.monthly_stats_sent_by_source[target_key] = period_key
                    state.preview = (
                        message
                        + f"\nПлатформа: {platform_label}"
                        + f"\nЧат: {target_chat_id}"
                    )
                    state.last_message_id = message_id
                    state.parser_last_monthly_sent_at = now_label_msk()
                    state.parser_last_monthly_period = monthly_snapshot.month_label
                    state.parser_last_monthly_title = monthly_snapshot.dispatch_title
                logger.info(
                    "Месячная статистика отправлена. source=%s platform=%s chat_id=%s period=%s",
                    source.url,
                    platform_label,
                    target_chat_id,
                    period_key,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Ошибка отправки месячной статистики. source=%s platform=%s chat_id=%s period=%s",
                    source.url,
                    platform_label,
                    target_chat_id,
                    period_key,
                )
                with state.lock:
                    state.parser_error = (
                        f"{now_label()} | Ошибка месячной статистики "
                        f"({platform_label} {target_chat_id}): {humanize_parser_error(exc)}"
                    )
    finally:
        with state.lock:
            for _, _, target_key in delivery_targets:
                state.monthly_stats_inflight_sources.discard(target_key)


async def send_daily_stats_to_sources(
    cfg: TargetConfig,
    tg_cfg: Optional[TelegramConfig],
    vk_cfg: Optional[VkConfig],
    storage_state: dict[str, Any],
    sources: list[ParserSource],
    stats_date: str,
) -> tuple[list[str], list[str], Optional[int], str]:
    sent_targets: list[str] = []
    source_errors: list[str] = []
    last_message_id: Optional[int] = None
    last_preview = ""

    parser_playwright: Optional[AsyncPlaywright] = None
    parser_browser: Optional[Any] = None
    parser_context: Optional[Any] = None
    tg_session: Optional[aiohttp.ClientSession] = None
    vk_session: Optional[aiohttp.ClientSession] = None

    try:
        parser_playwright = await async_playwright().start()
        parser_browser = await parser_playwright.chromium.launch(headless=cfg.headless)
        parser_context = await parser_browser.new_context(storage_state=storage_state)

        if tg_cfg is not None:
            tg_timeout = aiohttp.ClientTimeout(total=tg_cfg.request_timeout_seconds)
            tg_session = aiohttp.ClientSession(
                timeout=tg_timeout,
                trust_env=tg_cfg.use_system_proxy,
            )
        if vk_cfg is not None:
            vk_timeout = aiohttp.ClientTimeout(total=vk_cfg.request_timeout_seconds)
            vk_session = aiohttp.ClientSession(
                timeout=vk_timeout,
                trust_env=vk_cfg.use_system_proxy,
            )

        for source in sources:
            page: Optional[AsyncPage] = None
            try:
                page = await parser_context.new_page()
                daily_snapshot = await fetch_daily_profit_snapshot(
                    page,
                    cfg,
                    source.url,
                    stats_date,
                    navigate=True,
                )
                message = build_daily_stats_message(daily_snapshot)
                targets = iter_source_delivery_targets(source)
                if not targets:
                    source_errors.append(f"{source.url}: нет Telegram/VK chat_id для отправки")
                    continue

                for target_kind, target_chat_id in targets:
                    platform_label = stats_target_label(target_kind)
                    try:
                        last_message_id = await send_stats_message_to_target(
                            tg_session,
                            tg_cfg,
                            vk_session,
                            vk_cfg,
                            target_kind,
                            target_chat_id,
                            message,
                        )
                        sent_targets.append(
                            f"{source.url} -> {platform_label} {target_chat_id}"
                        )
                        last_preview = (
                            message
                            + f"\nПлатформа: {platform_label}"
                            + f"\nЧат: {target_chat_id}"
                        )
                    except Exception as target_exc:  # noqa: BLE001
                        source_errors.append(
                            f"{source.url} ({platform_label} {target_chat_id}): "
                            f"{humanize_parser_error(target_exc)}"
                        )
            except Exception as source_exc:  # noqa: BLE001
                source_errors.append(
                    f"{source.url}: {humanize_parser_error(source_exc)}"
                )
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:  # noqa: BLE001
                        pass
    finally:
        if tg_session is not None:
            await tg_session.close()
        if vk_session is not None:
            await vk_session.close()
        if parser_context is not None:
            try:
                await parser_context.close()
            except Exception:  # noqa: BLE001
                pass
        if parser_browser is not None:
            try:
                await parser_browser.close()
            except Exception:  # noqa: BLE001
                pass
        if parser_playwright is not None:
            try:
                await parser_playwright.stop()
            except Exception:  # noqa: BLE001
                pass

    return sent_targets, source_errors, last_message_id, last_preview


async def send_weekly_stats_to_sources(
    cfg: TargetConfig,
    tg_cfg: Optional[TelegramConfig],
    vk_cfg: Optional[VkConfig],
    storage_state: dict[str, Any],
    sources: list[ParserSource],
    week_start: date,
    week_end: date,
) -> tuple[list[str], list[str], Optional[int], str]:
    sent_targets: list[str] = []
    source_errors: list[str] = []
    last_message_id: Optional[int] = None
    last_preview = ""

    parser_playwright: Optional[AsyncPlaywright] = None
    parser_browser: Optional[Any] = None
    parser_context: Optional[Any] = None
    tg_session: Optional[aiohttp.ClientSession] = None
    vk_session: Optional[aiohttp.ClientSession] = None

    try:
        parser_playwright = await async_playwright().start()
        parser_browser = await parser_playwright.chromium.launch(headless=cfg.headless)
        parser_context = await parser_browser.new_context(storage_state=storage_state)

        if tg_cfg is not None:
            tg_timeout = aiohttp.ClientTimeout(total=tg_cfg.request_timeout_seconds)
            tg_session = aiohttp.ClientSession(
                timeout=tg_timeout,
                trust_env=tg_cfg.use_system_proxy,
            )
        if vk_cfg is not None:
            vk_timeout = aiohttp.ClientTimeout(total=vk_cfg.request_timeout_seconds)
            vk_session = aiohttp.ClientSession(
                timeout=vk_timeout,
                trust_env=vk_cfg.use_system_proxy,
            )

        for source in sources:
            page: Optional[AsyncPage] = None
            try:
                page = await parser_context.new_page()
                weekly_snapshot = await fetch_weekly_profit_snapshot(
                    page,
                    cfg,
                    source.url,
                    week_start,
                    week_end,
                    navigate=True,
                )
                message = build_weekly_stats_message(weekly_snapshot)
                targets = iter_source_delivery_targets(source)
                if not targets:
                    source_errors.append(f"{source.url}: нет Telegram/VK chat_id для отправки")
                    continue

                for target_kind, target_chat_id in targets:
                    platform_label = stats_target_label(target_kind)
                    try:
                        last_message_id = await send_stats_message_to_target(
                            tg_session,
                            tg_cfg,
                            vk_session,
                            vk_cfg,
                            target_kind,
                            target_chat_id,
                            message,
                        )
                        sent_targets.append(
                            f"{source.url} -> {platform_label} {target_chat_id}"
                        )
                        last_preview = (
                            message
                            + f"\nПлатформа: {platform_label}"
                            + f"\nЧат: {target_chat_id}"
                        )
                    except Exception as target_exc:  # noqa: BLE001
                        source_errors.append(
                            f"{source.url} ({platform_label} {target_chat_id}): "
                            f"{humanize_parser_error(target_exc)}"
                        )
            except Exception as source_exc:  # noqa: BLE001
                source_errors.append(
                    f"{source.url}: {humanize_parser_error(source_exc)}"
                )
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:  # noqa: BLE001
                        pass
    finally:
        if tg_session is not None:
            await tg_session.close()
        if vk_session is not None:
            await vk_session.close()
        if parser_context is not None:
            try:
                await parser_context.close()
            except Exception:  # noqa: BLE001
                pass
        if parser_browser is not None:
            try:
                await parser_browser.close()
            except Exception:  # noqa: BLE001
                pass
        if parser_playwright is not None:
            try:
                await parser_playwright.stop()
            except Exception:  # noqa: BLE001
                pass

    return sent_targets, source_errors, last_message_id, last_preview


async def send_monthly_stats_to_sources(
    cfg: TargetConfig,
    tg_cfg: Optional[TelegramConfig],
    vk_cfg: Optional[VkConfig],
    storage_state: dict[str, Any],
    sources: list[ParserSource],
    target_year: int,
    target_month: int,
) -> tuple[list[str], list[str], Optional[int], str]:
    sent_targets: list[str] = []
    source_errors: list[str] = []
    last_message_id: Optional[int] = None
    last_preview = ""

    parser_playwright: Optional[AsyncPlaywright] = None
    parser_browser: Optional[Any] = None
    parser_context: Optional[Any] = None
    tg_session: Optional[aiohttp.ClientSession] = None
    vk_session: Optional[aiohttp.ClientSession] = None

    try:
        parser_playwright = await async_playwright().start()
        parser_browser = await parser_playwright.chromium.launch(headless=cfg.headless)
        parser_context = await parser_browser.new_context(storage_state=storage_state)

        if tg_cfg is not None:
            tg_timeout = aiohttp.ClientTimeout(total=tg_cfg.request_timeout_seconds)
            tg_session = aiohttp.ClientSession(
                timeout=tg_timeout,
                trust_env=tg_cfg.use_system_proxy,
            )
        if vk_cfg is not None:
            vk_timeout = aiohttp.ClientTimeout(total=vk_cfg.request_timeout_seconds)
            vk_session = aiohttp.ClientSession(
                timeout=vk_timeout,
                trust_env=vk_cfg.use_system_proxy,
            )

        for source in sources:
            page: Optional[AsyncPage] = None
            try:
                page = await parser_context.new_page()
                monthly_snapshot = await fetch_monthly_profit_snapshot(
                    page,
                    cfg,
                    source.url,
                    target_year,
                    target_month,
                    navigate=True,
                )
                message = build_monthly_stats_message(monthly_snapshot)
                targets = iter_source_delivery_targets(source)
                if not targets:
                    source_errors.append(f"{source.url}: нет Telegram/VK chat_id для отправки")
                    continue

                for target_kind, target_chat_id in targets:
                    platform_label = stats_target_label(target_kind)
                    try:
                        last_message_id = await send_stats_message_to_target(
                            tg_session,
                            tg_cfg,
                            vk_session,
                            vk_cfg,
                            target_kind,
                            target_chat_id,
                            message,
                        )
                        sent_targets.append(
                            f"{source.url} -> {platform_label} {target_chat_id}"
                        )
                        last_preview = (
                            message
                            + f"\nПлатформа: {platform_label}"
                            + f"\nЧат: {target_chat_id}"
                        )
                    except Exception as target_exc:  # noqa: BLE001
                        source_errors.append(
                            f"{source.url} ({platform_label} {target_chat_id}): "
                            f"{humanize_parser_error(target_exc)}"
                        )
            except Exception as source_exc:  # noqa: BLE001
                source_errors.append(
                    f"{source.url}: {humanize_parser_error(source_exc)}"
                )
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:  # noqa: BLE001
                        pass
    finally:
        if tg_session is not None:
            await tg_session.close()
        if vk_session is not None:
            await vk_session.close()
        if parser_context is not None:
            try:
                await parser_context.close()
            except Exception:  # noqa: BLE001
                pass
        if parser_browser is not None:
            try:
                await parser_browser.close()
            except Exception:  # noqa: BLE001
                pass
        if parser_playwright is not None:
            try:
                await parser_playwright.stop()
            except Exception:  # noqa: BLE001
                pass

    return sent_targets, source_errors, last_message_id, last_preview


async def parser_worker_async(
    cfg: TargetConfig,
    stop_event: threading.Event,
    storage_state: dict[str, Any],
) -> None:
    logger.info("Запуск фонового парсера (async)")
    try:
        tg_cfg = load_telegram_config()
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.parser_error = f"Ошибка Telegram конфигурации: {exc}"
            state.parser_running = False
            state.parser_thread = None
            state.parser_stop_event = None
        return
    try:
        vk_cfg: Optional[VkConfig] = load_vk_config()
    except Exception as exc:  # noqa: BLE001
        vk_cfg = None
        logger.warning("VK конфигурация недоступна: %s", exc)

    blogabet_requested = parse_bool_env(os.getenv("BLOGABET_ENABLED", "0"), default=False)
    blogabet_cfg: Optional[BlogabetConfig] = None
    ocr_client: Optional[OcrSpaceClient] = None
    try:
        candidate_blogabet_cfg = load_blogabet_config()
        if candidate_blogabet_cfg.enabled:
            blogabet_cfg = candidate_blogabet_cfg
            ocr_client = load_ocr_client()
            logger.info(
                "Blogabet доставка включена. storage_state=%s aliases=%s headless=%s stake=%s upcoming_url=%s",
                blogabet_cfg.storage_state_path,
                blogabet_cfg.league_aliases_path,
                blogabet_cfg.headless,
                blogabet_cfg.default_stake,
                blogabet_cfg.upcoming_url,
            )
            if not normalize_text(blogabet_cfg.admin_tg_chat_id):
                logger.warning(
                    "BLOGABET_ENABLED=1, но BLOGABET_ADMIN_TG_CHAT_ID пуст: admin-уведомления о сбоях Blogabet не будут отправляться"
                )
            else:
                try:
                    parsed_admin_chat_ids = parse_telegram_chat_ids(
                        blogabet_cfg.admin_tg_chat_id,
                        require_non_empty=True,
                    )
                    logger.info(
                        "Blogabet admin-уведомления включены. recipients=%s",
                        len(parsed_admin_chat_ids),
                    )
                except ValueError as admin_exc:
                    logger.warning(
                        "BLOGABET_ADMIN_TG_CHAT_ID содержит невалидный формат: %s",
                        admin_exc,
                    )
            if not Path(blogabet_cfg.storage_state_path).exists():
                logger.warning(
                    "BLOGABET_ENABLED=1, но storage_state не найден: %s",
                    blogabet_cfg.storage_state_path,
                )
    except Exception as exc:  # noqa: BLE001
        blogabet_cfg = None
        ocr_client = None
        if blogabet_requested:
            log_blogabet_error("BLOGABET_ENABLED=1, но Blogabet/OCR конфигурация недоступна: %s", exc)
            with state.lock:
                state.parser_error = (
                    f"{now_label()} | Blogabet/OCR конфигурация недоступна: {humanize_parser_error(exc)}"
                )
        else:
            logger.warning("Blogabet/OCR конфигурация недоступна: %s", exc)

    try:
        match_store = get_match_store()
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.parser_error = f"Ошибка БД матчей: {exc}"
            state.parser_running = False
            state.parser_thread = None
            state.parser_stop_event = None
        return

    parser_playwright: Optional[AsyncPlaywright] = None
    parser_browser: Optional[Any] = None
    parser_context: Optional[Any] = None
    source_pages: dict[str, SourcePageRuntime] = {}
    settlement_completed_fetch_last_at: dict[str, float] = {}
    source_bootstrapped: set[str] = set()
    delivery_tasks: set[asyncio.Task[None]] = set()
    settlement_tasks: set[asyncio.Task[None]] = set()
    blogabet_publisher: Optional[BlogabetPublisher] = None

    try:
        parser_playwright = await async_playwright().start()
        parser_browser = await parser_playwright.chromium.launch(headless=cfg.headless)
        parser_context = await parser_browser.new_context(storage_state=storage_state)
        logger.info("Браузер парсера успешно инициализирован")
        if blogabet_cfg is not None and ocr_client is not None:
            blogabet_publisher = BlogabetPublisher(blogabet_cfg, logger=logger)

        tg_timeout = aiohttp.ClientTimeout(total=tg_cfg.request_timeout_seconds)
        async with aiohttp.ClientSession(
            timeout=tg_timeout,
            trust_env=tg_cfg.use_system_proxy,
        ) as tg_session:
            vk_session: Optional[aiohttp.ClientSession] = None
            try:
                if vk_cfg is not None:
                    vk_timeout = aiohttp.ClientTimeout(total=vk_cfg.request_timeout_seconds)
                    vk_session = aiohttp.ClientSession(
                        timeout=vk_timeout,
                        trust_env=vk_cfg.use_system_proxy,
                    )

                while not stop_event.is_set():
                    cycle_started_at = time.monotonic()
                    interval_seconds = max(cfg.parser_interval_seconds, 10)
                    parser_page_max_age_seconds = max(cfg.parser_page_max_age_seconds, 10)

                    # Подчищаем завершившиеся delivery-задачи без блокировки цикла.
                    delivery_tasks = {task for task in delivery_tasks if not task.done()}
                    settlement_tasks = {task for task in settlement_tasks if not task.done()}

                    try:
                        if parser_context is None:
                            raise RuntimeError("Сессия парсера не инициализирована")

                        with state.lock:
                            enabled_sources = [
                                ParserSource(
                                    source_id=source.source_id,
                                    url=source.url,
                                    chat_id=source.chat_id,
                                    vk_chat_ids=tuple(source.vk_chat_ids),
                                    enabled=True,
                                )
                                for source in state.parser_sources
                                if source.enabled
                            ]
                            interval_seconds = max(state.parser_interval_seconds, 10)
                            parser_page_max_age_seconds = max(
                                state.parser_page_max_age_seconds,
                                10,
                            )

                        enabled_source_ids = {source.source_id for source in enabled_sources}
                        for source_id in list(source_pages.keys()):
                            if source_id in enabled_source_ids:
                                continue
                            source_runtime = source_pages.pop(source_id)
                            try:
                                await source_runtime.page.close()
                            except Exception:  # noqa: BLE001
                                pass
                            source_bootstrapped.discard(source_id)
                            settlement_completed_fetch_last_at.pop(source_id, None)

                        if not enabled_sources:
                            with state.lock:
                                state.parser_last_check_at = now_label_msk()
                                state.parser_error = "Нет включенных ссылок для парсинга"
                        else:
                            source_errors: list[str] = []
                            now_msk = datetime.now(MSK_TIMEZONE)
                            should_send_daily_stats = (
                                now_msk.hour == cfg.daily_stats_send_hour_msk
                            )
                            daily_stats_date = (
                                now_msk.date() - timedelta(days=1)
                            ).strftime("%d.%m.%Y")
                            should_send_weekly_stats = (
                                now_msk.weekday() == 0
                                and now_msk.hour == cfg.weekly_stats_send_hour_msk
                            )
                            weekly_start, weekly_end = previous_week_period(now_msk)
                            weekly_period_key = weekly_stats_period_key(weekly_start, weekly_end)
                            should_send_monthly_stats = (
                                now_msk.day == 1
                                and now_msk.hour == cfg.monthly_stats_send_hour_msk
                            )
                            monthly_year, monthly_month = previous_month_period(now_msk)
                            monthly_period_key = monthly_stats_period_key(
                                monthly_year,
                                monthly_month,
                            )
                            fetch_tasks = [
                                asyncio.create_task(
                                    fetch_matches_for_source(
                                        parser_context,
                                        source_pages,
                                        cfg,
                                        source,
                                        parser_page_max_age_seconds,
                                    )
                                )
                                for source in enabled_sources
                            ]

                            for fetch_task in asyncio.as_completed(fetch_tasks):
                                result = await fetch_task
                                source = result.source

                                if result.error is not None:
                                    source_errors.append(
                                        f"{source.url}: {humanize_parser_error(result.error)}"
                                    )
                                    continue

                                event_at = now_storage_label_msk()
                                try:
                                    match_store.register_active_matches(
                                        source,
                                        result.matches,
                                        event_at,
                                    )
                                except Exception as storage_exc:  # noqa: BLE001
                                    source_errors.append(
                                        f"{source.url}: ошибка сохранения активных матчей в БД ({humanize_parser_error(storage_exc)})"
                                    )
                                    continue

                                skip_live_notifications = False
                                if source.source_id not in source_bootstrapped:
                                    source_bootstrapped.add(source.source_id)
                                    if not cfg.parser_send_existing_on_start:
                                        logger.info(
                                            "Инициализация источника: существующие матчи помечены как отправленные. source=%s",
                                            source.url,
                                        )
                                        try:
                                            for match in result.matches:
                                                for target_kind, target_chat_id in iter_source_match_delivery_targets(
                                                    source,
                                                    include_blogabet=blogabet_publisher is not None,
                                                ):
                                                    if target_kind == "telegram":
                                                        delivery_key = compose_delivery_key(
                                                            source.source_id,
                                                            match.unique_key,
                                                        )
                                                    else:
                                                        delivery_key = compose_platform_delivery_key(
                                                            source.source_id,
                                                            match.unique_key,
                                                            target_kind,
                                                            target_chat_id,
                                                        )
                                                    match_store.upsert_ignored_delivery(
                                                        source,
                                                        match,
                                                        target_kind,
                                                        target_chat_id,
                                                        delivery_key,
                                                        event_at,
                                                    )
                                        except Exception as storage_exc:  # noqa: BLE001
                                            source_errors.append(
                                                f"{source.url}: ошибка фиксации стартового skip-режима в БД ({humanize_parser_error(storage_exc)})"
                                            )
                                            continue
                                        skip_live_notifications = True
                                    else:
                                        logger.info(
                                            "Инициализация источника: существующие матчи будут отправлены. source=%s",
                                            source.url,
                                        )

                                if not skip_live_notifications:
                                    for match in result.matches:
                                        for target_kind, target_chat_id in iter_source_match_delivery_targets(
                                            source,
                                            include_blogabet=blogabet_publisher is not None,
                                        ):
                                            if target_kind == "telegram":
                                                delivery_key = compose_delivery_key(
                                                    source.source_id,
                                                    match.unique_key,
                                                )
                                            else:
                                                delivery_key = compose_platform_delivery_key(
                                                    source.source_id,
                                                    match.unique_key,
                                                    target_kind,
                                                    target_chat_id,
                                                )
                                            with state.lock:
                                                if delivery_key in state.pending_match_keys:
                                                    continue

                                            try:
                                                already_delivered = match_store.delivery_exists(delivery_key)
                                            except Exception as storage_exc:  # noqa: BLE001
                                                source_errors.append(
                                                    f"{source.url}: ошибка чтения БД матчей ({humanize_parser_error(storage_exc)})"
                                                )
                                                continue

                                            if already_delivered:
                                                continue

                                            with state.lock:
                                                if delivery_key in state.pending_match_keys:
                                                    continue
                                                state.pending_match_keys.add(delivery_key)

                                            delivery_task = asyncio.create_task(
                                                deliver_match_notification(
                                                    tg_session,
                                                    tg_cfg,
                                                    vk_session,
                                                    vk_cfg,
                                                    blogabet_publisher,
                                                    blogabet_cfg,
                                                    ocr_client,
                                                    match_store,
                                                    match,
                                                    source,
                                                    target_kind,
                                                    target_chat_id,
                                                    delivery_key,
                                                )
                                            )
                                            delivery_tasks.add(delivery_task)

                                source_runtime = source_pages.get(source.source_id)
                                if source_runtime is not None:
                                    try:
                                        settled_updates_scheduled = await schedule_settlement_updates_for_source(
                                            parser_context,
                                            tg_session,
                                            tg_cfg,
                                            match_store,
                                            cfg,
                                            source,
                                            result.matches,
                                            settlement_tasks,
                                            settlement_completed_fetch_last_at,
                                            parser_page_max_age_seconds,
                                        )
                                        if settled_updates_scheduled > 0:
                                            logger.info(
                                                "Запланированы обновления исходов. source=%s count=%s",
                                                source.url,
                                                settled_updates_scheduled,
                                            )
                                    except Exception as settlement_exc:  # noqa: BLE001
                                        source_errors.append(
                                            f"{source.url}: {humanize_parser_error(settlement_exc)}"
                                        )

                                if should_send_daily_stats:
                                    if source_runtime is not None:
                                        daily_delivery_targets: list[tuple[str, str, str]] = []
                                        with state.lock:
                                            for target_kind, target_chat_id in iter_source_delivery_targets(source):
                                                target_key = build_stats_target_key(
                                                    source.source_id,
                                                    "daily",
                                                    target_kind,
                                                    target_chat_id,
                                                )
                                                already_sent_for_date = (
                                                    state.daily_stats_sent_by_source.get(target_key)
                                                    == daily_stats_date
                                                )
                                                if (
                                                    already_sent_for_date
                                                    or target_key in state.daily_stats_inflight_sources
                                                ):
                                                    continue
                                                state.daily_stats_inflight_sources.add(target_key)
                                                daily_delivery_targets.append(
                                                    (target_kind, target_chat_id, target_key)
                                                )
                                        if daily_delivery_targets:
                                            await deliver_daily_stats_notification(
                                                tg_session,
                                                tg_cfg,
                                                vk_session,
                                                vk_cfg,
                                                cfg,
                                                source_runtime.page,
                                                source,
                                                daily_stats_date,
                                                daily_delivery_targets,
                                            )

                                if should_send_weekly_stats:
                                    if source_runtime is not None:
                                        weekly_delivery_targets: list[tuple[str, str, str]] = []
                                        with state.lock:
                                            for target_kind, target_chat_id in iter_source_delivery_targets(source):
                                                target_key = build_stats_target_key(
                                                    source.source_id,
                                                    "weekly",
                                                    target_kind,
                                                    target_chat_id,
                                                )
                                                already_sent_for_period = (
                                                    state.weekly_stats_sent_by_source.get(target_key)
                                                    == weekly_period_key
                                                )
                                                if (
                                                    already_sent_for_period
                                                    or target_key in state.weekly_stats_inflight_sources
                                                ):
                                                    continue
                                                state.weekly_stats_inflight_sources.add(target_key)
                                                weekly_delivery_targets.append(
                                                    (target_kind, target_chat_id, target_key)
                                                )
                                        if weekly_delivery_targets:
                                            await deliver_weekly_stats_notification(
                                                tg_session,
                                                tg_cfg,
                                                vk_session,
                                                vk_cfg,
                                                cfg,
                                                source_runtime.page,
                                                source,
                                                weekly_start,
                                                weekly_end,
                                                weekly_period_key,
                                                weekly_delivery_targets,
                                            )

                                if should_send_monthly_stats:
                                    if source_runtime is not None:
                                        monthly_delivery_targets: list[tuple[str, str, str]] = []
                                        with state.lock:
                                            for target_kind, target_chat_id in iter_source_delivery_targets(source):
                                                target_key = build_stats_target_key(
                                                    source.source_id,
                                                    "monthly",
                                                    target_kind,
                                                    target_chat_id,
                                                )
                                                already_sent_for_period = (
                                                    state.monthly_stats_sent_by_source.get(target_key)
                                                    == monthly_period_key
                                                )
                                                if (
                                                    already_sent_for_period
                                                    or target_key in state.monthly_stats_inflight_sources
                                                ):
                                                    continue
                                                state.monthly_stats_inflight_sources.add(target_key)
                                                monthly_delivery_targets.append(
                                                    (target_kind, target_chat_id, target_key)
                                                )
                                        if monthly_delivery_targets:
                                            await deliver_monthly_stats_notification(
                                                tg_session,
                                                tg_cfg,
                                                vk_session,
                                                vk_cfg,
                                                cfg,
                                                source_runtime.page,
                                                source,
                                                monthly_year,
                                                monthly_month,
                                                monthly_period_key,
                                                monthly_delivery_targets,
                                            )

                            with state.lock:
                                state.parser_last_check_at = now_label_msk()
                                if source_errors:
                                    state.parser_error = f"{now_label()} | {' | '.join(source_errors)}"
                                else:
                                    state.parser_error = ""
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("Ошибка в цикле парсера")
                        with state.lock:
                            state.parser_error = f"{now_label()} | {humanize_parser_error(exc)}"

                    elapsed = time.monotonic() - cycle_started_at
                    sleep_seconds = max(0.0, interval_seconds - elapsed)
                    if sleep_seconds <= 0:
                        continue
                    if await asyncio.to_thread(stop_event.wait, sleep_seconds):
                        break
            finally:
                if vk_session is not None:
                    await vk_session.close()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Критическая ошибка фонового парсера")
        with state.lock:
            state.parser_error = f"{now_label()} | {humanize_parser_error(exc)}"
    finally:
        background_tasks = delivery_tasks | settlement_tasks
        if background_tasks:
            done, pending = await asyncio.wait(background_tasks, timeout=8)
            for pending_task in pending:
                pending_task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        for source_runtime in source_pages.values():
            try:
                await source_runtime.page.close()
            except Exception:  # noqa: BLE001
                pass

        if parser_context is not None:
            try:
                await parser_context.close()
            except Exception:  # noqa: BLE001
                pass

        if parser_browser is not None:
            try:
                await parser_browser.close()
            except Exception:  # noqa: BLE001
                pass

        if parser_playwright is not None:
            try:
                await parser_playwright.stop()
            except Exception:  # noqa: BLE001
                pass

        if blogabet_publisher is not None:
            try:
                await blogabet_publisher.close()
            except Exception:  # noqa: BLE001
                pass

        with state.lock:
            if state.parser_stop_event is stop_event:
                state.parser_stop_event = None
            if state.parser_thread is threading.current_thread():
                state.parser_thread = None
            state.parser_running = False

        logger.info("Фоновый парсер остановлен")


def parser_worker(
    cfg: TargetConfig,
    stop_event: threading.Event,
    storage_state: dict[str, Any],
) -> None:
    asyncio.run(parser_worker_async(cfg, stop_event, storage_state))


def start_parser_thread(cfg: TargetConfig, storage_state: dict[str, Any]) -> None:
    state.stop_parser()
    match_store = get_match_store()
    match_store.clear_runtime_data()
    logger.info("История матчей очищена перед запуском парсера")

    stop_event = threading.Event()
    worker = threading.Thread(
        target=parser_worker,
        args=(cfg, stop_event, storage_state),
        name="alpinbet-parser",
        daemon=True,
    )

    with state.lock:
        if state.parser_interval_seconds < 10:
            state.parser_interval_seconds = max(cfg.parser_interval_seconds, 10)
        state.parser_interval_initialized = True
        if state.parser_page_max_age_seconds < 10:
            state.parser_page_max_age_seconds = max(cfg.parser_page_max_age_seconds, 10)
        state.parser_page_max_age_initialized = True
        state.pending_match_keys = set()
        state.pending_settlement_keys = set()
        state.parser_last_check_at = ""
        state.parser_last_sent_at = ""
        state.parser_last_match_title = ""
        state.parser_last_settled_at = ""
        state.parser_last_settled_title = ""
        state.parser_error = ""
        state.daily_stats_inflight_sources = set()
        state.weekly_stats_inflight_sources = set()
        state.monthly_stats_inflight_sources = set()

        state.parser_stop_event = stop_event
        state.parser_thread = worker
        state.parser_running = True

    worker.start()


def describe_login_status(step: str) -> str:
    if step == "await_code":
        return "Ожидается код подтверждения из почты"
    if step == "ready":
        return "Вход в аккаунт выполнен успешно. Бот готов к парсингу данных"
    return "Вход не выполнен"


def describe_parser_status(
    step: str,
    running: bool,
    interval_seconds: int,
    parser_page_max_age_seconds: int,
    enabled_sources: int,
    total_sources: int,
) -> str:
    if running:
        return (
            f"Включен (интервал проверки: {interval_seconds} сек, "
            f"TTL страницы: {parser_page_max_age_seconds} сек, "
            f"ссылок: {enabled_sources}/{total_sources})"
        )
    if step != "ready":
        return "Недоступен до успешного входа"
    return f"Выключен (ссылок: {enabled_sources}/{total_sources})"


TEMPLATE = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Alpine Control Panel</title>
  <style>
    :root {
      --bg: #fefaf4;
      --bg-soft: #fdf4e7;
      --surface: #ffffff;
      --surface-soft: #f7fbfa;
      --text: #122430;
      --muted: #4c6670;
      --accent: #0f766e;
      --accent-2: #ea580c;
      --accent-soft: #d9f3ef;
      --danger: #b91c1c;
      --danger-soft: #fee2e2;
      --line: #d6e4e6;
      --radius: 18px;
      --shadow: 0 14px 36px rgba(22, 42, 55, 0.1);
    }

    * { box-sizing: border-box; }

    html {
      scroll-behavior: smooth;
    }

    body {
      margin: 0;
      background:
        radial-gradient(circle at 8% 8%, rgba(15, 118, 110, 0.12) 0%, transparent 26%),
        radial-gradient(circle at 92% 12%, rgba(234, 88, 12, 0.12) 0%, transparent 24%),
        linear-gradient(180deg, var(--bg-soft) 0%, var(--bg) 64%),
        var(--bg);
      color: var(--text);
      font-family: "Space Grotesk", "IBM Plex Sans", "Segoe UI", "Trebuchet MS", sans-serif;
      line-height: 1.45;
    }

    .app-shell {
      max-width: 1260px;
      margin: 0 auto;
      padding: 20px 16px 30px;
      display: grid;
      grid-template-columns: 280px 1fr;
      gap: 16px;
      min-height: 100vh;
    }

    .sidebar {
      position: sticky;
      top: 16px;
      align-self: start;
      border: 1px solid var(--line);
      border-radius: calc(var(--radius) + 2px);
      background: linear-gradient(170deg, #f8fffd 0%, #fff7ef 100%);
      box-shadow: var(--shadow);
      padding: 16px 14px;
      z-index: 25;
    }

    .side-head {
      margin-bottom: 16px;
      padding-bottom: 12px;
      border-bottom: 1px dashed #b9d5d9;
    }

    .logo {
      margin: 0;
      font-size: 20px;
      letter-spacing: 0.3px;
      line-height: 1.1;
      font-weight: 800;
      color: #0f5d56;
    }

    .side-sub {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 13px;
    }

    .side-nav {
      display: grid;
      gap: 6px;
    }

    .side-nav a {
      display: block;
      text-decoration: none;
      color: #194656;
      border-radius: 12px;
      border: 1px solid transparent;
      padding: 10px 11px;
      font-size: 14px;
      font-weight: 600;
      transition: background 0.16s ease, transform 0.16s ease, border-color 0.16s ease;
    }

    .side-nav a:hover {
      background: #edf8f6;
      border-color: #b7dbd5;
      transform: translateX(2px);
    }

    .side-meta {
      margin-top: 14px;
      padding-top: 12px;
      border-top: 1px dashed #b9d5d9;
      display: grid;
      gap: 6px;
    }

    .side-meta-item {
      font-size: 12px;
      color: var(--muted);
    }

    .content {
      min-width: 0;
      display: grid;
      gap: 12px;
      align-content: start;
    }

    .panel {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 16px;
      box-shadow: var(--shadow);
      opacity: 0;
      transform: translateY(8px);
      animation: reveal 0.45s ease forwards;
    }

    .content .panel:nth-child(2) { animation-delay: 0.04s; }
    .content .panel:nth-child(3) { animation-delay: 0.08s; }
    .content .panel:nth-child(4) { animation-delay: 0.12s; }
    .content .panel:nth-child(5) { animation-delay: 0.16s; }
    .content .panel:nth-child(6) { animation-delay: 0.2s; }
    .content .panel:nth-child(7) { animation-delay: 0.24s; }
    .content .panel:nth-child(8) { animation-delay: 0.28s; }
    .content .panel:nth-child(9) { animation-delay: 0.32s; }

    @keyframes reveal {
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }

    .topbar {
      background: linear-gradient(140deg, #0e3f4d, #0f766e 56%, #ea580c 120%);
      color: #ecfffb;
      border: none;
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      position: relative;
      overflow: hidden;
    }

    .topbar::after {
      content: "";
      position: absolute;
      inset: auto -16% -62% auto;
      width: 220px;
      aspect-ratio: 1 / 1;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(255, 255, 255, 0.28), rgba(255, 255, 255, 0));
      pointer-events: none;
    }

    .topbar h1 {
      margin: 0;
      font-size: 26px;
      font-weight: 800;
      line-height: 1.1;
      letter-spacing: 0.2px;
    }

    .topbar p {
      margin: 8px 0 0;
      max-width: 640px;
      color: #d7f8f2;
      font-size: 14px;
    }

    .menu-btn {
      width: 42px;
      height: 42px;
      min-width: 42px;
      border-radius: 12px;
      border: 1px solid rgba(255, 255, 255, 0.36);
      background: rgba(255, 255, 255, 0.14);
      display: none;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      padding: 0;
      margin: 0;
    }

    .menu-btn span {
      display: block;
      width: 18px;
      height: 2px;
      background: #f8ffff;
      border-radius: 999px;
      position: relative;
    }

    .menu-btn span::before,
    .menu-btn span::after {
      content: "";
      position: absolute;
      left: 0;
      width: 18px;
      height: 2px;
      background: #f8ffff;
      border-radius: 999px;
    }

    .menu-btn span::before { top: -6px; }
    .menu-btn span::after { top: 6px; }

    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }

    .panel h2 {
      margin: 0;
      font-size: 18px;
      letter-spacing: 0.2px;
    }

    .panel h3 {
      margin: 0 0 10px;
      font-size: 15px;
      letter-spacing: 0.2px;
    }

    .chip-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 8px;
    }

    .chip {
      display: inline-flex;
      align-items: center;
      padding: 7px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      background: #eaf8f6;
      color: #0d5f58;
      border: 1px solid #b8ddd7;
    }

    .chip.meta {
      background: #fff2e8;
      color: #b45309;
      border-color: #ffd1ad;
    }

    .hint {
      color: var(--muted);
      font-size: 13px;
      margin: 3px 0;
    }

    .state-lines {
      display: grid;
      gap: 4px;
      margin-bottom: 8px;
    }

    .ok,
    .error {
      white-space: pre-wrap;
      border-radius: 12px;
      padding: 9px 10px;
      margin-top: 8px;
      font-size: 14px;
    }

    .ok {
      color: #14532d;
      background: #dcfce7;
      border: 1px solid #bbf7d0;
    }

    .error {
      color: #7f1d1d;
      background: var(--danger-soft);
      border: 1px solid #fecaca;
    }

    .section-grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .tile {
      border: 1px dashed #c7dbdf;
      border-radius: 14px;
      padding: 12px;
      background: var(--surface-soft);
    }

    .tile.full {
      grid-column: 1 / -1;
    }

    form {
      margin: 0;
    }

    input:not([type='hidden']),
    select,
    textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 11px 12px;
      font-size: 14px;
      color: var(--text);
      background: #fff;
      margin-bottom: 10px;
      font-family: inherit;
    }

    input:not([type='hidden']):focus,
    select:focus,
    textarea:focus {
      outline: none;
      border-color: #15a090;
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.16);
    }

    button {
      width: 100%;
      border: 0;
      border-radius: 12px;
      padding: 11px 14px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      color: #fff;
      background: linear-gradient(120deg, var(--accent), #14b8a6);
      transition: transform 0.16s ease, filter 0.16s ease;
      font-family: inherit;
    }

    button:hover { filter: brightness(1.04); transform: translateY(-1px); }
    button:active { transform: translateY(0); }
    button[disabled] { opacity: 0.58; cursor: not-allowed; transform: none; }

    .secondary {
      background: linear-gradient(120deg, #1f3340, #36566a);
    }

    .danger {
      background: linear-gradient(120deg, #b91c1c, #ef4444);
    }

    .mini {
      width: auto;
      min-width: 130px;
      padding: 8px 12px;
      font-size: 13px;
    }

    .source-list {
      display: grid;
      gap: 10px;
      margin-top: 10px;
    }

    .source-row {
      border: 1px solid #d6e9eb;
      border-radius: 14px;
      background: #ffffff;
      padding: 11px;
    }

    .source-url {
      font-size: 13px;
      word-break: break-all;
      color: #163040;
      margin-bottom: 8px;
      font-weight: 600;
    }

    .source-chat-form {
      display: flex;
      gap: 8px;
      align-items: stretch;
      margin-bottom: 8px;
    }

    .source-chat-form input,
    .source-chat-form textarea {
      margin-bottom: 0;
    }

    .source-controls {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }

    .source-state {
      font-size: 12px;
      font-weight: 700;
      padding: 5px 9px;
      border-radius: 999px;
      border: 1px solid #a8dbd4;
      background: #dcf7f2;
      color: #0f766e;
    }

    .source-state.off {
      border-color: #d6dde3;
      background: #f2f5f7;
      color: #4f6170;
    }

    .source-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 7px;
    }

    .action-grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    pre {
      margin: 0;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #fff;
      padding: 12px;
      white-space: pre-wrap;
      font-size: 13px;
      font-family: "IBM Plex Mono", "Consolas", "Courier New", monospace;
    }

    .overlay {
      position: fixed;
      inset: 0;
      background: rgba(13, 28, 36, 0.38);
      opacity: 0;
      pointer-events: none;
      transition: opacity 0.2s ease;
      z-index: 20;
    }

    @media (max-width: 1180px) {
      .source-chat-form {
        flex-direction: column;
      }
      .source-chat-form .mini {
        width: 100%;
      }
    }

    @media (max-width: 1080px) {
      .app-shell {
        grid-template-columns: 1fr;
        padding: 14px 10px 22px;
      }

      .menu-btn {
        display: inline-flex;
      }

      .sidebar {
        position: fixed;
        top: 10px;
        left: 10px;
        bottom: 10px;
        width: min(290px, calc(100vw - 20px));
        overflow-y: auto;
        transform: translateX(-112%);
        transition: transform 0.24s ease;
      }

      body.menu-open .sidebar {
        transform: translateX(0);
      }

      body.menu-open .overlay {
        opacity: 1;
        pointer-events: auto;
      }
    }

    @media (max-width: 860px) {
      .section-grid,
      .action-grid {
        grid-template-columns: 1fr;
      }

      .topbar {
        padding: 14px;
      }

      .topbar h1 {
        font-size: 22px;
      }

      .panel {
        padding: 13px;
      }
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <aside class="sidebar" id="sidebar">
      <div class="side-head">
        <h2 class="logo">Alpine Control</h2>
        <p class="side-sub">Управление парсером и рассылками из одного меню.</p>
      </div>
      <nav class="side-nav">
        <a href="#overview">Обзор</a>
        <a href="#auth">Авторизация</a>
        <a href="#parser">Парсер</a>
        <a href="#sources">Источники</a>
        <a href="#stats">Статистика</a>
        <a href="#tests">Тесты и сервис</a>
        <a href="#blogabet">Blogabet</a>
        <a href="#tokens">Токены</a>
        <a href="#vk-peer-ids">VK peer_id</a>
        {% if preview %}<a href="#preview">Последнее сообщение</a>{% endif %}
      </nav>
      <div class="side-meta">
        <div class="side-meta-item">Вход: {{ login_status }}</div>
        <div class="side-meta-item">Парсер: {{ parser_status }}</div>
      </div>
    </aside>

    <div class="overlay" id="navOverlay"></div>

    <main class="content">
      <section class="panel topbar">
        <button class="menu-btn" type="button" id="menuToggle" aria-label="Открыть меню">
          <span></span>
        </button>
        <div>
          <h1>Панель управления ботом</h1>
          <p>Alpinbet -> Telegram + VK.</p>
        </div>
      </section>

      <section id="overview" class="panel">
        <div class="panel-head">
          <h2>Обзор состояния</h2>
          {% if message_id %}<span class="chip meta">Последний message_id={{ message_id }}</span>{% endif %}
        </div>
        <div class="chip-row">
          <span class="chip">Вход: {{ login_status }}</span>
          <span class="chip meta">Парсер: {{ parser_status }}</span>
        </div>
        <div class="state-lines">
          {% if parser_last_check_at %}<div class="hint">Последняя проверка: {{ parser_last_check_at }}</div>{% endif %}
          {% if match_db_status %}<div class="hint">{{ match_db_status }}</div>{% endif %}
          {% if parser_last_sent_at %}<div class="hint">Последняя отправка: {{ parser_last_sent_at }}{% if parser_last_match_title %} ({{ parser_last_match_title }}){% endif %}</div>{% endif %}
          {% if parser_last_settled_at %}<div class="hint">Последнее обновление исхода: {{ parser_last_settled_at }}{% if parser_last_settled_title %} ({{ parser_last_settled_title }}){% endif %}</div>{% endif %}
          {% if parser_last_daily_sent_at %}<div class="hint">Последняя суточная статистика: {{ parser_last_daily_sent_at }} (за {{ parser_last_daily_date }}){% if parser_last_daily_title %} — {{ parser_last_daily_title }}{% endif %}</div>{% endif %}
          {% if parser_last_weekly_sent_at %}<div class="hint">Последняя недельная статистика: {{ parser_last_weekly_sent_at }} ({{ parser_last_weekly_period }}){% if parser_last_weekly_title %} — {{ parser_last_weekly_title }}{% endif %}</div>{% endif %}
          {% if parser_last_monthly_sent_at %}<div class="hint">Последняя месячная статистика: {{ parser_last_monthly_sent_at }} ({{ parser_last_monthly_period }}){% if parser_last_monthly_title %} — {{ parser_last_monthly_title }}{% endif %}</div>{% endif %}
        </div>
        {% if info %}<div class="ok">{{ info }}</div>{% endif %}
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
        {% if parser_error %}<div class="error">Ошибка парсера: {{ parser_error }}</div>{% endif %}
      </section>

      <section id="auth" class="panel">
        <div class="panel-head">
          <h2>Авторизация</h2>
        </div>
        <div class="section-grid">
          <article class="tile">
            <h3>Вход в аккаунт</h3>
            <form method="post" action="{{ url_for('start_login') }}">
              <input name="password" type="password" placeholder="Пароль" required />
              <button type="submit" {% if not can_start_login %}disabled{% endif %}>Начать вход</button>
            </form>
            <form method="post" action="{{ url_for('reset') }}">
              <button class="danger" type="submit" {% if not can_logout %}disabled{% endif %}>Выйти из аккаунта</button>
            </form>
          </article>
          {% if step == "await_code" %}
          <article class="tile">
            <h3>Подтверждение кода</h3>
            <form method="post" action="{{ url_for('submit_code') }}">
              <input name="code" type="text" placeholder="Код из почты" required />
              <button class="secondary" type="submit">Подтвердить код</button>
            </form>
          </article>
          {% else %}
          <article class="tile">
            <h3>Статус 2FA</h3>
            <div class="hint">Если сайт запросит код, здесь появится отдельная форма подтверждения.</div>
          </article>
          {% endif %}
        </div>
      </section>

      <section id="parser" class="panel">
        <div class="panel-head">
          <h2>Парсер</h2>
        </div>
        <div class="section-grid">
          <article class="tile">
            <h3>Управление парсером</h3>
            <div class="source-actions">
              <form method="post" action="{{ url_for('start_parser') }}">
                <button type="submit" {% if not can_start_parser %}disabled{% endif %}>Включить парсинг</button>
              </form>
              <form method="post" action="{{ url_for('stop_parser') }}">
                <button class="secondary" type="submit" {% if not can_stop_parser %}disabled{% endif %}>Выключить парсинг</button>
              </form>
            </div>
            <div class="hint">Включаются все активные ссылки из раздела «Источники».</div>
          </article>

          <article class="tile">
            <h3>Интервал проверки</h3>
            <form method="post" action="{{ url_for('update_parser_interval') }}">
              <input name="parser_interval_seconds" type="number" min="10" step="1" value="{{ parser_interval_seconds }}" required />
              <button class="secondary" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Сохранить интервал</button>
            </form>
            <div class="hint">Минимум 10 сек. Применяется в следующем цикле.</div>
          </article>

          <article class="tile">
            <h3>TTL страницы источника</h3>
            <form method="post" action="{{ url_for('update_parser_page_max_age') }}">
              <input name="parser_page_max_age_seconds" type="number" min="10" step="1" value="{{ parser_page_max_age_seconds }}" required />
              <button class="secondary" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Сохранить TTL</button>
            </form>
            <div class="hint">После TTL страница источника пересоздаётся.</div>
          </article>

          <article class="tile">
            <h3>Поведение при старте</h3>
            <form method="post" action="{{ url_for('update_parser_send_existing_mode') }}">
              <select name="parser_send_existing_on_start" required>
                <option value="1" {% if parser_send_existing_on_start %}selected{% endif %}>Отправлять уже существующие матчи</option>
                <option value="0" {% if not parser_send_existing_on_start %}selected{% endif %}>Пропускать уже существующие матчи</option>
              </select>
              <button class="secondary" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Сохранить режим</button>
            </form>
            <div class="hint">Настройка применяется при следующем запуске парсера.</div>
          </article>

          <article class="tile full">
            <h3>Авторассылки</h3>
            <div class="hint">Суточная: каждый день в {{ daily_stats_send_hour_msk }}:00 МСК (за предыдущие сутки).</div>
            <div class="hint">Недельная: каждый понедельник в {{ weekly_stats_send_hour_msk }}:00 МСК (за прошлую неделю).</div>
            <div class="hint">Месячная: 1-го числа в {{ monthly_stats_send_hour_msk }}:00 МСК (за прошлый месяц).</div>
          </article>
        </div>
      </section>

      <section id="sources" class="panel">
        <div class="panel-head">
          <h2>Источники</h2>
        </div>
        <article class="tile full">
          <h3>Добавить ссылку</h3>
          <form method="post" action="{{ url_for('add_parser_source_route') }}">
            <input name="source_url" type="url" placeholder="https://..." required />
            <input name="source_chat_id" type="text" placeholder="Telegram chat_id: -100... или @channel" required />
            <textarea name="source_vk_chat_ids" placeholder="VK chat_id (peer_id): по одному в строке, через запятую или ;"></textarea>
            <button class="secondary" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Добавить ссылку</button>
          </form>
          <div class="hint">VK-поля можно оставить пустыми. Поддерживается ввод через новую строку, запятую или ;</div>
        </article>

        <div class="source-list">
          {% for source in parser_sources %}
          <div class="source-row">
            <div class="source-url">{{ source.url }}</div>
            <form method="post" action="{{ url_for('update_parser_source_chat_route') }}" class="source-chat-form">
              <input type="hidden" name="source_id" value="{{ source.source_id }}" />
              <input name="source_chat_id" type="text" value="{{ source.chat_id }}" placeholder="-100... или @channel" required />
              <button class="secondary mini" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Сохранить Telegram chat_id</button>
            </form>
            <form method="post" action="{{ url_for('update_parser_source_vk_chat_route') }}" class="source-chat-form">
              <input type="hidden" name="source_id" value="{{ source.source_id }}" />
              <textarea name="source_vk_chat_ids" placeholder="VK chat_id (peer_id)">{{ source.vk_chat_ids|join('\n') }}</textarea>
              <button class="secondary mini" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Сохранить VK chat_id</button>
            </form>
            <div class="source-controls">
              <span class="source-state {% if not source.enabled %}off{% endif %}">
                {% if source.enabled %}Включена{% else %}Выключена{% endif %}
              </span>
              <div class="source-actions">
                <form method="post" action="{{ url_for('toggle_parser_source_route') }}">
                  <input type="hidden" name="source_id" value="{{ source.source_id }}" />
                  <button class="secondary mini" type="submit" {% if not can_manage_parser %}disabled{% endif %}>
                    {% if source.enabled %}Выключить{% else %}Включить{% endif %}
                  </button>
                </form>
                <form method="post" action="{{ url_for('delete_parser_source_route') }}">
                  <input type="hidden" name="source_id" value="{{ source.source_id }}" />
                  <button class="danger mini" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Удалить</button>
                </form>
              </div>
            </div>
          </div>
          {% else %}
          <article class="tile full">
            <div class="hint">Ссылки пока не добавлены.</div>
          </article>
          {% endfor %}
        </div>
      </section>

      <section id="stats" class="panel">
        <div class="panel-head">
          <h2>Тестовая статистика</h2>
        </div>
        <div class="section-grid">
          <article class="tile">
            <h3>Суточная</h3>
            <form method="post" action="{{ url_for('send_daily_stats_test_route') }}">
              <input name="daily_stats_days_ago" type="number" min="0" step="1" value="1" required />
              <button class="secondary" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Отправить суточную</button>
            </form>
            <div class="hint">0 — сегодня, 1 — вчера, 2 — позавчера.</div>
          </article>
          <article class="tile">
            <h3>Недельная</h3>
            <form method="post" action="{{ url_for('send_weekly_stats_test_route') }}">
              <input name="weekly_stats_week" type="week" value="{{ weekly_stats_week_value }}" required />
              <button class="secondary" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Отправить недельную</button>
            </form>
            <div class="hint">Выбери ISO-неделю (пн-вс).</div>
          </article>
          <article class="tile full">
            <h3>Месячная</h3>
            <form method="post" action="{{ url_for('send_monthly_stats_test_route') }}">
              <input name="monthly_stats_month" type="month" value="{{ monthly_stats_month_value }}" required />
              <button class="secondary" type="submit" {% if not can_manage_parser %}disabled{% endif %}>Отправить месячную</button>
            </form>
            <div class="hint">Выбери месяц, за который нужна статистика.</div>
          </article>
        </div>
      </section>

      <section id="tests" class="panel">
        <div class="panel-head">
          <h2>Тесты и сервисные действия</h2>
        </div>
        <div class="action-grid">
          <form method="post" action="{{ url_for('send_test_message') }}">
            <button class="secondary" type="submit">Тест отправки в Telegram + VK</button>
          </form>
          <form method="post" action="{{ url_for('send_settlement_test') }}">
            <select name="settlement_status" required>
              <option value="win">Тест исхода: Зашла</option>
              <option value="lose">Тест исхода: Не зашла</option>
              <option value="return">Тест исхода: Возврат</option>
            </select>
            <button class="secondary" type="submit">Тест обновления исхода</button>
          </form>
        </div>
      </section>

      <section id="blogabet" class="panel">
        <div class="panel-head">
          <h2>Blogabet</h2>
        </div>
        <div class="section-grid">
          <article class="tile">
            <h3>Статус и сессия</h3>
            <div class="hint">Включено: {{ 'Да' if blogabet_enabled else 'Нет' }}</div>
            <div class="hint">Headless: {{ 'Да' if blogabet_headless else 'Нет' }}</div>
            <div class="hint">Stake по умолчанию: {{ blogabet_default_stake }}</div>
            <div class="hint">Storage state: <code>{{ blogabet_storage_state_path }}</code></div>
            <div class="hint">Файл state: {{ 'найден' if blogabet_storage_state_exists else 'не найден' }}</div>
            <form method="post" action="{{ url_for('blogabet_login_route') }}">
              <button class="secondary" type="submit">Login to Blogabet</button>
            </form>
            <div class="hint">Если при входе есть CAPTCHA/reCAPTCHA, пройди её вручную в открытом браузере.</div>
            <div class="hint">Автологин использует BLOGABET_LOGIN_EMAIL и BLOGABET_LOGIN_PASSWORD (если заданы).</div>
          </article>

          <article class="tile full">
            <h3>Test publish</h3>
            <form method="post" action="{{ url_for('blogabet_test_publish_route') }}" enctype="multipart/form-data">
              <input name="tournament" type="text" placeholder="Tournament" required />
              <input name="home_team" type="text" placeholder="Home team" required />
              <input name="away_team" type="text" placeholder="Away team" required />
              <input name="manual_score" type="text" placeholder="Current score (опционально, для live), например 3:0" />
              <input name="image_url" type="url" placeholder="Image URL (если не загружаешь файл)" />
              <input name="image_file" type="file" accept="image/*" />
              <textarea name="bet_text" placeholder="Bet text (опционально, без OCR), например: 1-я половина&#10;П1"></textarea>
              <input name="stake" type="number" min="1" max="10" step="1" value="{{ blogabet_default_stake }}" required />
              <textarea name="analysis_text" placeholder="Analysis (опционально)"></textarea>
              <label class="checkbox">
                <input type="checkbox" name="dry_run" value="1" checked /><i></i> Dry run (без Create pick)
              </label>
              <div class="source-actions">
                <button class="secondary mini" type="submit" name="blogabet_action" value="find">Найти матч</button>
                <button class="mini" type="submit" name="blogabet_action" value="publish">Опубликовать</button>
              </div>
            </form>
            <div class="hint">Поле Analysis (опционально): текст комментария к pick в Blogabet. Если пусто, подставится авто‑analysis из OCR/ставки.</div>
            <div class="hint">Поле Bet text (опционально): если заполнено, OCR не используется.</div>
            <div class="hint">Поле Current score: ручной счёт live-матча для теста (формат 3:0 или 3-0), если OCR его не распознал.</div>
            <div class="hint">Чекбокс Dry run: включает тест без нажатия Create pick (реально не публикует).</div>
            <div class="hint">Тестовые действия Find/Publish запускаются в видимом браузере (headful).</div>
            <div class="hint">Используются OCR -> parse_bet_intent -> поиск лиги/матча/рынка в Blogabet Pinnacle Live.</div>
          </article>

          <article class="tile full">
            <h3>OCR by URL</h3>
            <form method="post" action="{{ url_for('blogabet_test_ocr_route') }}" enctype="multipart/form-data">
              <input name="image_url" type="url" placeholder="Image URL (если не загружаешь файл)" />
              <input name="image_file" type="file" accept="image/*" />
              <div class="source-actions">
                <button class="secondary mini" type="submit">Распознать параметры</button>
              </div>
            </form>
            <div class="hint">Инструмент только для распознавания OCR и разбора Bet intent, без поиска матча и публикации.</div>
          </article>

          {% if blogabet_test_log %}
          <article class="tile full">
            <h3>Результат Test publish</h3>
            {% if blogabet_test_pick_url %}<div class="hint">Pick URL: {{ blogabet_test_pick_url }}</div>{% endif %}
            {% if blogabet_test_screenshot_path %}<div class="hint">Screenshot: {{ blogabet_test_screenshot_path }}</div>{% endif %}
            {% if blogabet_test_html_dump_path %}<div class="hint">HTML dump: {{ blogabet_test_html_dump_path }}</div>{% endif %}
            <pre>{{ blogabet_test_log }}</pre>
            {% if blogabet_test_diagnostics %}
            <div class="hint">Diagnostics:</div>
            <pre>{{ blogabet_test_diagnostics }}</pre>
            {% endif %}
          </article>
          {% endif %}

          {% if blogabet_ocr_log %}
          <article class="tile full">
            <h3>Результат OCR by URL</h3>
            <pre>{{ blogabet_ocr_log }}</pre>
          </article>
          {% endif %}
        </div>
      </section>

      <section id="tokens" class="panel">
        <div class="panel-head">
          <h2>Токены и интеграции</h2>
        </div>
        <div class="section-grid">
          <article class="tile">
            <h3>Telegram</h3>
            <form method="post" action="{{ url_for('update_telegram_token') }}">
              <input name="telegram_bot_token" type="password" placeholder="Новый TELEGRAM_BOT_TOKEN" required />
              <button class="secondary" type="submit">Сохранить токен</button>
            </form>
            <div class="hint">Текущий токен: {{ telegram_token_masked }}</div>
          </article>
          <article class="tile">
            <h3>VK</h3>
            <form method="post" action="{{ url_for('update_vk_token') }}">
              <input name="vk_user_token" type="password" placeholder="Новый VK_USER_TOKEN" required />
              <button class="secondary" type="submit">Сохранить VK токен</button>
            </form>
            <div class="hint">Текущий VK токен: {{ vk_token_masked }}</div>
          </article>
        </div>
      </section>

      <section id="vk-peer-ids" class="panel">
        <div class="panel-head">
          <h2>VK peer_id чатов</h2>
          {% if vk_chat_lookup_last_at %}<span class="chip meta">{{ vk_chat_lookup_last_at }}</span>{% endif %}
        </div>
        <article class="tile full">
          <h3>Получение chat_id / peer_id</h3>
          <form method="post" action="{{ url_for('fetch_vk_chat_ids_route') }}">
            <input
              name="vk_chat_lookup_limit"
              type="number"
              min="1"
              max="200"
              step="1"
              value="{{ vk_chat_lookup_limit }}"
              required
            />
            <button class="secondary" type="submit">Получить список чатов VK</button>
          </form>
          <div class="hint">Используется VK_USER_TOKEN из .env и метод messages.getConversations.</div>
          <div class="hint">В VK обычно указывается именно <code>peer_id</code>.</div>
        </article>

        {% if vk_chat_lookup_loaded %}
          {% if vk_chat_lookup_results %}
          <div class="source-list">
            {% for chat in vk_chat_lookup_results %}
            <div class="source-row">
              <div class="source-url">{{ chat.title }}</div>
              <div class="chip-row">
                <span class="chip">peer_id: {{ chat.peer_id }}</span>
                <span class="chip meta">chat_id: {{ chat.chat_id }}</span>
              </div>
            </div>
            {% endfor %}
          </div>
          {% else %}
          <article class="tile full">
            <div class="hint">Чаты с типом <code>chat</code> не найдены в первых {{ vk_chat_lookup_limit }} диалогах.</div>
          </article>
          {% endif %}
        {% else %}
        <article class="tile full">
          <div class="hint">Нажми кнопку выше, чтобы загрузить список групповых чатов и их peer_id.</div>
        </article>
        {% endif %}
      </section>

      {% if preview %}
      <section id="preview" class="panel">
        <div class="panel-head">
          <h2>Последнее отправленное сообщение</h2>
        </div>
        <pre>{{ preview }}</pre>
      </section>
      {% endif %}
    </main>
  </div>
  <script>
    (() => {
      const body = document.body;
      const menuToggle = document.getElementById("menuToggle");
      const navOverlay = document.getElementById("navOverlay");
      const navLinks = document.querySelectorAll(".side-nav a");
      const mobileQuery = window.matchMedia("(max-width: 1080px)");

      const closeMenu = () => body.classList.remove("menu-open");
      const toggleMenu = () => body.classList.toggle("menu-open");

      if (menuToggle) {
        menuToggle.addEventListener("click", toggleMenu);
      }
      if (navOverlay) {
        navOverlay.addEventListener("click", closeMenu);
      }
      navLinks.forEach((link) => {
        link.addEventListener("click", () => {
          if (mobileQuery.matches) {
            closeMenu();
          }
        });
      });

      mobileQuery.addEventListener("change", () => {
        if (!mobileQuery.matches) {
          closeMenu();
        }
      });
    })();
  </script>
</body>
</html>
"""


@app.get("/")
def index():
    config_error = ""
    match_db_status = ""
    blogabet_enabled = False
    blogabet_headless = True
    blogabet_default_stake = DEFAULT_BLOGABET_STAKE
    blogabet_storage_state_path = resolve_local_path(
        DEFAULT_BLOGABET_STORAGE_STATE_PATH,
        DEFAULT_BLOGABET_STORAGE_STATE_PATH,
    )
    blogabet_storage_state_exists = Path(blogabet_storage_state_path).exists()
    default_interval = DEFAULT_PARSER_INTERVAL_SECONDS
    default_parser_page_max_age_seconds = DEFAULT_PARSER_PAGE_MAX_AGE_SECONDS
    default_parser_send_existing_on_start = True
    default_daily_stats_send_hour_msk = DEFAULT_DAILY_STATS_SEND_HOUR_MSK
    default_weekly_stats_send_hour_msk = DEFAULT_WEEKLY_STATS_SEND_HOUR_MSK
    default_monthly_stats_send_hour_msk = DEFAULT_MONTHLY_STATS_SEND_HOUR_MSK
    default_week_start, _ = previous_week_period()
    default_weekly_stats_week_value = iso_week_input_value(default_week_start)
    default_month_year, default_month = previous_month_period()
    default_monthly_stats_month_value = month_input_value(
        default_month_year,
        default_month,
    )

    try:
        cfg = load_target_config()
        default_interval = cfg.parser_interval_seconds
        default_parser_page_max_age_seconds = cfg.parser_page_max_age_seconds
        default_parser_send_existing_on_start = cfg.parser_send_existing_on_start
        default_daily_stats_send_hour_msk = cfg.daily_stats_send_hour_msk
        default_weekly_stats_send_hour_msk = cfg.weekly_stats_send_hour_msk
        default_monthly_stats_send_hour_msk = cfg.monthly_stats_send_hour_msk
        ensure_parser_runtime_defaults(cfg)
    except Exception as exc:  # noqa: BLE001
        config_error = str(exc)

    try:
        blogabet_cfg = load_blogabet_config()
        blogabet_enabled = blogabet_cfg.enabled
        blogabet_headless = blogabet_cfg.headless
        blogabet_default_stake = blogabet_cfg.default_stake
        blogabet_storage_state_path = blogabet_cfg.storage_state_path
        blogabet_storage_state_exists = Path(blogabet_storage_state_path).exists()
    except Exception as exc:  # noqa: BLE001
        if config_error:
            config_error = f"{config_error} | Blogabet: {exc}"
        else:
            config_error = f"Blogabet: {exc}"

    try:
        counters = get_match_store().fetch_status_counters()
        match_db_status = (
            "БД матчей: "
            f"активных {counters['active']}, "
            f"исчезнувших {counters['disappeared']}, "
            f"ожидают исхода {counters['pending_settlement']}, "
            f"завершено {counters['settled']}, "
            f"всего {counters['total_matches']}"
        )
    except Exception as exc:  # noqa: BLE001
        match_db_status = f"БД матчей недоступна: {humanize_parser_error(exc)}"

    with state.lock:
        parser_interval_seconds = max(
            state.parser_interval_seconds,
            10,
        ) if state.parser_interval_initialized else max(default_interval, 10)
        parser_page_max_age_seconds = max(
            state.parser_page_max_age_seconds,
            10,
        ) if state.parser_page_max_age_initialized else max(default_parser_page_max_age_seconds, 10)
        parser_sources = list(state.parser_sources)
        enabled_sources = sum(1 for source in parser_sources if source.enabled)
        total_sources = len(parser_sources)
        step = state.step
        parser_running = state.parser_running
        has_auth_storage = state.auth_storage_state is not None
        can_manage_parser = step == "ready" and has_auth_storage
        can_start_parser = can_manage_parser and not parser_running
        can_stop_parser = can_manage_parser and parser_running
        can_start_login = not (step == "ready" and has_auth_storage)
        can_logout = step == "ready" and has_auth_storage

        current_error = state.error or config_error
        telegram_token_masked = mask_token(os.getenv("TELEGRAM_BOT_TOKEN", ""))
        vk_token_masked = mask_token(os.getenv("VK_USER_TOKEN", ""))
        vk_chat_lookup_results = list(state.vk_chat_lookup_results)
        vk_chat_lookup_loaded = state.vk_chat_lookup_loaded
        vk_chat_lookup_limit = state.vk_chat_lookup_limit
        vk_chat_lookup_last_at = state.vk_chat_lookup_last_at
        blogabet_test_log = state.blogabet_test_log
        blogabet_test_pick_url = state.blogabet_test_pick_url
        blogabet_test_screenshot_path = state.blogabet_test_screenshot_path
        blogabet_test_html_dump_path = state.blogabet_test_html_dump_path
        blogabet_test_diagnostics = state.blogabet_test_diagnostics
        blogabet_ocr_log = state.blogabet_ocr_log

        return render_template_string(
            TEMPLATE,
            login_status=describe_login_status(step),
            parser_status=describe_parser_status(
                step,
                parser_running,
                parser_interval_seconds,
                parser_page_max_age_seconds,
                enabled_sources,
                total_sources,
            ),
            parser_last_check_at=state.parser_last_check_at,
            parser_last_sent_at=state.parser_last_sent_at,
            parser_last_match_title=state.parser_last_match_title,
            parser_last_settled_at=state.parser_last_settled_at,
            parser_last_settled_title=state.parser_last_settled_title,
            parser_last_daily_sent_at=state.parser_last_daily_sent_at,
            parser_last_daily_date=state.parser_last_daily_date,
            parser_last_daily_title=state.parser_last_daily_title,
            parser_last_weekly_sent_at=state.parser_last_weekly_sent_at,
            parser_last_weekly_period=state.parser_last_weekly_period,
            parser_last_weekly_title=state.parser_last_weekly_title,
            parser_last_monthly_sent_at=state.parser_last_monthly_sent_at,
            parser_last_monthly_period=state.parser_last_monthly_period,
            parser_last_monthly_title=state.parser_last_monthly_title,
            parser_error=state.parser_error,
            match_db_status=match_db_status,
            info=state.info,
            error=current_error,
            preview=state.preview,
            message_id=state.last_message_id,
            parser_sources=parser_sources,
            telegram_token_masked=telegram_token_masked,
            vk_token_masked=vk_token_masked,
            vk_chat_lookup_results=vk_chat_lookup_results,
            vk_chat_lookup_loaded=vk_chat_lookup_loaded,
            vk_chat_lookup_limit=vk_chat_lookup_limit,
            vk_chat_lookup_last_at=vk_chat_lookup_last_at,
            blogabet_enabled=blogabet_enabled,
            blogabet_headless=blogabet_headless,
            blogabet_default_stake=blogabet_default_stake,
            blogabet_storage_state_path=blogabet_storage_state_path,
            blogabet_storage_state_exists=blogabet_storage_state_exists,
            blogabet_test_log=blogabet_test_log,
            blogabet_test_pick_url=blogabet_test_pick_url,
            blogabet_test_screenshot_path=blogabet_test_screenshot_path,
            blogabet_test_html_dump_path=blogabet_test_html_dump_path,
            blogabet_test_diagnostics=blogabet_test_diagnostics,
            blogabet_ocr_log=blogabet_ocr_log,
            parser_interval_seconds=parser_interval_seconds,
            parser_page_max_age_seconds=parser_page_max_age_seconds,
            parser_send_existing_on_start=default_parser_send_existing_on_start,
            daily_stats_send_hour_msk=default_daily_stats_send_hour_msk,
            weekly_stats_send_hour_msk=default_weekly_stats_send_hour_msk,
            monthly_stats_send_hour_msk=default_monthly_stats_send_hour_msk,
            weekly_stats_week_value=default_weekly_stats_week_value,
            monthly_stats_month_value=default_monthly_stats_month_value,
            step=step,
            can_manage_parser=can_manage_parser,
            can_start_parser=can_start_parser,
            can_stop_parser=can_stop_parser,
            can_start_login=can_start_login,
            can_logout=can_logout,
        )


@app.post("/start-login")
def start_login():
    password = request.form.get("password", "").strip()

    with state.lock:
        state.error = ""
        state.info = ""

    if not password:
        with state.lock:
            state.error = "Нужно передать пароль"
        return redirect(url_for("index"))

    try:
        cfg = load_target_config()

        state.stop_parser()
        state.clear_runtime()

        with state.lock:
            state.preview = ""
            state.last_message_id = None
            state.step = "idle"
            state.auth_storage_state = None

        state.playwright = sync_playwright().start()
        browser = state.playwright.chromium.launch(headless=cfg.headless)
        context = browser.new_context()
        state.page = context.new_page()

        state.page.goto(
            cfg.login_url, wait_until="domcontentloaded", timeout=30000)

        if cfg.open_login_selector:
            try:
                state.page.click(cfg.open_login_selector, timeout=10000)
            except Exception:  # noqa: BLE001
                pass

        username_input_visible = try_wait_visible(
            state.page, cfg.email_selector, timeout_ms=7000)
        if username_input_visible:
            if not cfg.login_username:
                raise ValueError(
                    "Не задан TARGET_LOGIN_USERNAME в .env (логин/почта для входа)")
            state.page.fill(cfg.email_selector,
                            cfg.login_username, timeout=10000)

        state.page.fill(cfg.password_selector, password, timeout=10000)
        state.page.click(cfg.submit_selector, timeout=10000)
        state.page.wait_for_timeout(1500)

        if try_wait_visible(state.page, cfg.code_selector, timeout_ms=3500):
            with state.lock:
                state.step = "await_code"
            return redirect(url_for("index"))

        login_errors = get_visible_texts(state.page, cfg.login_error_selector)
        joined_errors = " | ".join(login_errors).lower()
        has_invalid_password = any(
            marker in joined_errors
            for marker in ("неверный пароль", "неверный логин", "invalid", "error")
        )

        if has_invalid_password:
            raise ValueError(f"Ошибка входа: {'; '.join(login_errors)}")

        if is_login_form_visible(state.page, cfg.login_form_selector):
            raise ValueError(
                "Вход не подтвержден: форма логина все еще активна")

        auth_storage_state = state.page.context.storage_state()
        with state.lock:
            state.step = "ready"
            state.auth_storage_state = auth_storage_state
        state.clear_runtime()
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Ошибка на шаге логина: {exc}"
            state.step = "idle"
            state.auth_storage_state = None
        state.clear_runtime()

    return redirect(url_for("index"))


@app.post("/submit-code")
def submit_code():
    code = request.form.get("code", "").strip()

    with state.lock:
        state.error = ""
        state.info = ""

    if not code:
        with state.lock:
            state.error = "Нужно передать код"
        return redirect(url_for("index"))

    with state.lock:
        if state.step != "await_code" or state.page is None:
            state.error = "Сначала выполни вход"
            return redirect(url_for("index"))

    try:
        cfg = load_target_config()

        with state.lock:
            page = state.page

        if page is None:
            raise RuntimeError("Сессия браузера недоступна")

        page.fill(cfg.code_selector, code, timeout=10000)
        page.click(cfg.code_submit_selector, timeout=10000)
        page.wait_for_timeout(2000)

        login_errors = get_visible_texts(page, cfg.login_error_selector)
        if login_errors:
            joined = "; ".join(login_errors)
            raise ValueError(f"Код не принят: {joined}")

        if is_login_form_visible(page, cfg.login_form_selector):
            raise ValueError("Код не подтвержден: форма логина снова активна")

        auth_storage_state = page.context.storage_state()
        with state.lock:
            state.step = "ready"
            state.auth_storage_state = auth_storage_state
        state.clear_runtime()
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Ошибка на шаге кода: {exc}"

    return redirect(url_for("index"))


@app.post("/start-parser")
def start_parser():
    with state.lock:
        state.error = ""
        state.info = ""

    try:
        cfg = load_target_config()
        ensure_parser_runtime_defaults(cfg)
        load_telegram_config()

        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        with state.lock:
            enabled_sources = [source for source in state.parser_sources if source.enabled]
        if not enabled_sources:
            raise RuntimeError("Нет включенных ссылок для парсинга")
        sources_with_invalid_chat: list[str] = []
        sources_with_invalid_vk_chat: list[str] = []
        requires_vk_delivery = False
        for source in enabled_sources:
            try:
                validate_chat_id(source.chat_id)
            except Exception:  # noqa: BLE001
                sources_with_invalid_chat.append(source.url)
            if source.vk_chat_ids:
                requires_vk_delivery = True
            for chat_id in source.vk_chat_ids:
                try:
                    validate_vk_chat_id(chat_id)
                except Exception:  # noqa: BLE001
                    sources_with_invalid_vk_chat.append(source.url)
                    break
        if sources_with_invalid_chat:
            raise RuntimeError(
                "Для некоторых ссылок некорректный chat_id Telegram: "
                + " | ".join(sources_with_invalid_chat)
            )
        if sources_with_invalid_vk_chat:
            raise RuntimeError(
                "Для некоторых ссылок некорректный chat_id VK: "
                + " | ".join(sources_with_invalid_vk_chat)
            )
        if requires_vk_delivery:
            load_vk_config()

        with state.lock:
            storage_state = state.auth_storage_state
        if storage_state is None:
            raise RuntimeError("Сессия авторизации недоступна. Выполни вход заново.")

        start_parser_thread(cfg, storage_state)
        with state.lock:
            state.info = (
                f"Парсер запущен. История матчей очищена. Активных ссылок: {len(enabled_sources)}. "
                f"Суточная: {cfg.daily_stats_send_hour_msk:02d}:00 МСК, "
                f"недельная: Пн {cfg.weekly_stats_send_hour_msk:02d}:00 МСК, "
                f"месячная: 1-го числа {cfg.monthly_stats_send_hour_msk:02d}:00 МСК."
            )
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось запустить парсер: {exc}"

    return redirect(url_for("index"))


@app.post("/add-parser-source")
def add_parser_source_route():
    with state.lock:
        state.error = ""
        state.info = ""

    source_url = request.form.get("source_url", "").strip()
    source_chat_id = request.form.get("source_chat_id", "").strip()
    source_vk_chat_ids = request.form.get(
        "source_vk_chat_ids",
        request.form.get("source_vk_chat_id", ""),
    ).strip()

    try:
        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        is_added, source = add_parser_source(
            source_url,
            source_chat_id,
            source_vk_chat_ids,
        )

        with state.lock:
            if is_added:
                state.info = (
                    f"Ссылка добавлена: {source.url} -> TG {source.chat_id}, "
                    f"VK {len(source.vk_chat_ids)} чат(ов)"
                )
            else:
                state.info = (
                    f"Ссылка обновлена: {source.url} -> TG {source.chat_id}, "
                    f"VK {len(source.vk_chat_ids)} чат(ов)"
                )
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось добавить ссылку: {exc}"

    return redirect(url_for("index"))


@app.post("/update-parser-source-chat")
def update_parser_source_chat_route():
    with state.lock:
        state.error = ""
        state.info = ""

    source_id = request.form.get("source_id", "").strip()
    source_chat_id = request.form.get("source_chat_id", "").strip()

    if not source_id:
        with state.lock:
            state.error = "Не передан идентификатор ссылки"
        return redirect(url_for("index"))

    try:
        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        source = update_parser_source_chat_id(source_id, source_chat_id)
        with state.lock:
            state.info = f"Telegram chat_id обновлён: {source.url} -> {source.chat_id}"
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось обновить Telegram chat_id: {exc}"

    return redirect(url_for("index"))


@app.post("/update-parser-source-vk-chat")
def update_parser_source_vk_chat_route():
    with state.lock:
        state.error = ""
        state.info = ""

    source_id = request.form.get("source_id", "").strip()
    source_vk_chat_ids = request.form.get(
        "source_vk_chat_ids",
        request.form.get("source_vk_chat_id", ""),
    ).strip()

    if not source_id:
        with state.lock:
            state.error = "Не передан идентификатор ссылки"
        return redirect(url_for("index"))

    try:
        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        source = update_parser_source_vk_chat_ids(source_id, source_vk_chat_ids)
        with state.lock:
            state.info = (
                f"VK chat_id обновлён: {source.url} -> "
                f"{len(source.vk_chat_ids)} чат(ов)"
            )
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось обновить VK chat_id: {exc}"

    return redirect(url_for("index"))


@app.post("/toggle-parser-source")
def toggle_parser_source_route():
    with state.lock:
        state.error = ""
        state.info = ""

    source_id = request.form.get("source_id", "").strip()
    if not source_id:
        with state.lock:
            state.error = "Не передан идентификатор ссылки"
        return redirect(url_for("index"))

    try:
        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        source = toggle_parser_source(source_id)
        status_label = "включена" if source.enabled else "выключена"
        with state.lock:
            state.info = f"Ссылка {status_label}: {source.url}"
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось изменить статус ссылки: {exc}"

    return redirect(url_for("index"))


@app.post("/delete-parser-source")
def delete_parser_source_route():
    with state.lock:
        state.error = ""
        state.info = ""

    source_id = request.form.get("source_id", "").strip()
    if not source_id:
        with state.lock:
            state.error = "Не передан идентификатор ссылки"
        return redirect(url_for("index"))

    try:
        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        removed_source = remove_parser_source(source_id)
        with state.lock:
            state.info = f"Ссылка удалена: {removed_source.url}"
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось удалить ссылку: {exc}"

    return redirect(url_for("index"))


@app.post("/update-parser-interval")
def update_parser_interval():
    with state.lock:
        state.error = ""
        state.info = ""

    interval_raw = request.form.get("parser_interval_seconds", "").strip()

    try:
        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        interval_seconds = parse_interval_seconds(interval_raw)

        upsert_env_value("PARSER_INTERVAL_SECONDS", str(interval_seconds))
        os.environ["PARSER_INTERVAL_SECONDS"] = str(interval_seconds)

        with state.lock:
            state.parser_interval_seconds = interval_seconds
            state.parser_interval_initialized = True
            if state.parser_running:
                state.info = (
                    f"Интервал обновлен: {interval_seconds} сек. "
                    "Применится в следующем цикле."
                )
            else:
                state.info = f"Интервал обновлен: {interval_seconds} сек"
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось обновить интервал: {exc}"

    return redirect(url_for("index"))


@app.post("/update-parser-page-max-age")
def update_parser_page_max_age():
    with state.lock:
        state.error = ""
        state.info = ""

    page_max_age_raw = request.form.get("parser_page_max_age_seconds", "").strip()

    try:
        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        page_max_age_seconds = parse_min_seconds_value(
            page_max_age_raw,
            field_label="Интервал пересоздания страниц парсера",
            minimum_seconds=10,
        )

        upsert_env_value("PARSER_PAGE_MAX_AGE_SECONDS", str(page_max_age_seconds))
        os.environ["PARSER_PAGE_MAX_AGE_SECONDS"] = str(page_max_age_seconds)

        with state.lock:
            state.parser_page_max_age_seconds = page_max_age_seconds
            state.parser_page_max_age_initialized = True
            if state.parser_running:
                state.info = (
                    f"Интервал перезапуска страниц обновлен: {page_max_age_seconds} сек. "
                    "Применится в следующем цикле."
                )
            else:
                state.info = f"Интервал перезапуска страниц обновлен: {page_max_age_seconds} сек"
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось обновить интервал перезапуска страниц: {exc}"

    return redirect(url_for("index"))


@app.post("/update-parser-send-existing")
def update_parser_send_existing_mode():
    with state.lock:
        state.error = ""
        state.info = ""

    raw_value = normalize_text(request.form.get("parser_send_existing_on_start", ""))

    try:
        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None

        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        if raw_value not in {"0", "1"}:
            raise ValueError("Нужно выбрать режим 0 или 1")

        send_existing_on_start = raw_value == "1"
        value_to_store = "1" if send_existing_on_start else "0"

        upsert_env_value("PARSER_SEND_EXISTING_ON_START", value_to_store)
        os.environ["PARSER_SEND_EXISTING_ON_START"] = value_to_store

        with state.lock:
            if send_existing_on_start:
                state.info = (
                    "Режим стартовой отправки включен: существующие матчи будут отправляться "
                    "при запуске парсера."
                )
            else:
                state.info = (
                    "Режим стартовой отправки выключен: существующие матчи будут пропускаться "
                    "при запуске парсера."
                )
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось обновить режим стартовой отправки: {exc}"

    return redirect(url_for("index"))


@app.post("/stop-parser")
def stop_parser():
    state.stop_parser()
    with state.lock:
        state.error = ""
        state.info = ""
    return redirect(url_for("index"))


def analyze_stats_delivery_sources(sources: list[ParserSource]) -> tuple[bool, bool]:
    invalid_tg_sources: list[str] = []
    invalid_vk_sources: list[str] = []
    sources_without_targets: list[str] = []
    has_telegram_targets = False
    has_vk_targets = False

    for source in sources:
        source_has_target = False

        normalized_tg_chat_id = normalize_chat_id(source.chat_id)
        if normalized_tg_chat_id:
            try:
                validate_chat_id(normalized_tg_chat_id)
                has_telegram_targets = True
                source_has_target = True
            except Exception:  # noqa: BLE001
                invalid_tg_sources.append(source.url)

        for raw_vk_chat_id in source.vk_chat_ids:
            try:
                validate_vk_chat_id(raw_vk_chat_id)
                has_vk_targets = True
                source_has_target = True
            except Exception:  # noqa: BLE001
                invalid_vk_sources.append(source.url)
                break

        if not source_has_target:
            sources_without_targets.append(source.url)

    if invalid_tg_sources:
        unique_sources = list(dict.fromkeys(invalid_tg_sources))
        raise RuntimeError(
            "Для некоторых ссылок некорректный chat_id Telegram: "
            + " | ".join(unique_sources)
        )
    if invalid_vk_sources:
        unique_sources = list(dict.fromkeys(invalid_vk_sources))
        raise RuntimeError(
            "Для некоторых ссылок некорректный chat_id VK: "
            + " | ".join(unique_sources)
        )
    if sources_without_targets:
        unique_sources = list(dict.fromkeys(sources_without_targets))
        raise RuntimeError(
            "Для некоторых ссылок не указан ни Telegram, ни VK chat_id: "
            + " | ".join(unique_sources)
        )

    return has_telegram_targets, has_vk_targets


@app.post("/send-daily-stats-test")
def send_daily_stats_test_route():
    with state.lock:
        state.error = ""
        state.info = ""

    days_ago_raw = request.form.get("daily_stats_days_ago", "").strip()

    try:
        cfg = load_target_config()
        tg_cfg: Optional[TelegramConfig] = None
        vk_cfg: Optional[VkConfig] = None
        days_ago = parse_days_ago(days_ago_raw)
        stats_date = daily_stats_date_label(days_ago)

        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None
        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        with state.lock:
            enabled_sources = [
                ParserSource(
                    source_id=source.source_id,
                    url=source.url,
                    chat_id=source.chat_id,
                    vk_chat_ids=tuple(source.vk_chat_ids),
                    enabled=True,
                )
                for source in state.parser_sources
                if source.enabled
            ]
            storage_state = state.auth_storage_state

        if storage_state is None:
            raise RuntimeError("Сессия авторизации недоступна. Выполни вход заново.")
        if not enabled_sources:
            raise RuntimeError("Нет включенных ссылок для отправки")

        has_telegram_targets, has_vk_targets = analyze_stats_delivery_sources(enabled_sources)
        if has_telegram_targets:
            tg_cfg = load_telegram_config()
        if has_vk_targets:
            vk_cfg = load_vk_config()

        sent_targets, source_errors, message_id, preview = asyncio.run(
            send_daily_stats_to_sources(
                cfg,
                tg_cfg,
                vk_cfg,
                storage_state,
                enabled_sources,
                stats_date,
            )
        )

        if not sent_targets:
            joined = " | ".join(source_errors)
            raise RuntimeError(f"Не удалось отправить статистику ни по одной ссылке: {joined}")

        with state.lock:
            state.last_message_id = message_id
            if preview:
                state.preview = preview
            state.info = (
                f"Суточная статистика за {stats_date} отправлена в {len(sent_targets)} канал(ов)."
            )
            if source_errors:
                state.error = "Часть каналов не отправлена: " + " | ".join(source_errors)
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось отправить суточную статистику: {exc}"

    return redirect(url_for("index"))


@app.post("/send-weekly-stats-test")
def send_weekly_stats_test_route():
    with state.lock:
        state.error = ""
        state.info = ""

    week_raw = request.form.get("weekly_stats_week", "").strip()

    try:
        cfg = load_target_config()
        tg_cfg: Optional[TelegramConfig] = None
        vk_cfg: Optional[VkConfig] = None
        week_start, week_end = parse_week_input(week_raw)
        period_label = weekly_stats_period_label(week_start, week_end)

        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None
        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        with state.lock:
            enabled_sources = [
                ParserSource(
                    source_id=source.source_id,
                    url=source.url,
                    chat_id=source.chat_id,
                    vk_chat_ids=tuple(source.vk_chat_ids),
                    enabled=True,
                )
                for source in state.parser_sources
                if source.enabled
            ]
            storage_state = state.auth_storage_state

        if storage_state is None:
            raise RuntimeError("Сессия авторизации недоступна. Выполни вход заново.")
        if not enabled_sources:
            raise RuntimeError("Нет включенных ссылок для отправки")

        has_telegram_targets, has_vk_targets = analyze_stats_delivery_sources(enabled_sources)
        if has_telegram_targets:
            tg_cfg = load_telegram_config()
        if has_vk_targets:
            vk_cfg = load_vk_config()

        sent_targets, source_errors, message_id, preview = asyncio.run(
            send_weekly_stats_to_sources(
                cfg,
                tg_cfg,
                vk_cfg,
                storage_state,
                enabled_sources,
                week_start,
                week_end,
            )
        )

        if not sent_targets:
            joined = " | ".join(source_errors)
            raise RuntimeError(f"Не удалось отправить статистику ни по одной ссылке: {joined}")

        with state.lock:
            state.last_message_id = message_id
            if preview:
                state.preview = preview
            state.info = (
                f"Недельная статистика за период {period_label} отправлена по "
                f"{len(sent_targets)} канал(ам)."
            )
            if source_errors:
                state.error = "Часть каналов не отправлена: " + " | ".join(source_errors)
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось отправить недельную статистику: {exc}"

    return redirect(url_for("index"))


@app.post("/send-monthly-stats-test")
def send_monthly_stats_test_route():
    with state.lock:
        state.error = ""
        state.info = ""

    month_raw = request.form.get("monthly_stats_month", "").strip()

    try:
        cfg = load_target_config()
        tg_cfg: Optional[TelegramConfig] = None
        vk_cfg: Optional[VkConfig] = None
        target_year, target_month = parse_month_input(month_raw)
        period_label = month_short_label(target_year, target_month)

        with state.lock:
            is_ready = state.step == "ready" and state.auth_storage_state is not None
        if not is_ready:
            raise RuntimeError("Сначала выполни вход и подтверди код")

        with state.lock:
            enabled_sources = [
                ParserSource(
                    source_id=source.source_id,
                    url=source.url,
                    chat_id=source.chat_id,
                    vk_chat_ids=tuple(source.vk_chat_ids),
                    enabled=True,
                )
                for source in state.parser_sources
                if source.enabled
            ]
            storage_state = state.auth_storage_state

        if storage_state is None:
            raise RuntimeError("Сессия авторизации недоступна. Выполни вход заново.")
        if not enabled_sources:
            raise RuntimeError("Нет включенных ссылок для отправки")

        has_telegram_targets, has_vk_targets = analyze_stats_delivery_sources(enabled_sources)
        if has_telegram_targets:
            tg_cfg = load_telegram_config()
        if has_vk_targets:
            vk_cfg = load_vk_config()

        sent_targets, source_errors, message_id, preview = asyncio.run(
            send_monthly_stats_to_sources(
                cfg,
                tg_cfg,
                vk_cfg,
                storage_state,
                enabled_sources,
                target_year,
                target_month,
            )
        )

        if not sent_targets:
            joined = " | ".join(source_errors)
            raise RuntimeError(f"Не удалось отправить статистику ни по одной ссылке: {joined}")

        with state.lock:
            state.last_message_id = message_id
            if preview:
                state.preview = preview
            state.info = (
                f"Месячная статистика за {period_label} отправлена по "
                f"{len(sent_targets)} канал(ам)."
            )
            if source_errors:
                state.error = "Часть каналов не отправлена: " + " | ".join(source_errors)
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось отправить месячную статистику: {exc}"

    return redirect(url_for("index"))


def collect_unique_enabled_chat_ids() -> list[str]:
    with state.lock:
        raw_sources_with_chat = [
            ParserSource(
                source_id=source.source_id,
                url=source.url,
                chat_id=source.chat_id,
                vk_chat_ids=source.vk_chat_ids,
                enabled=source.enabled,
            )
            for source in state.parser_sources
            if source.enabled and normalize_chat_id(source.chat_id)
        ]

    if not raw_sources_with_chat:
        return []

    unique_chat_ids: list[str] = []
    seen_chat_ids: set[str] = set()
    for source in raw_sources_with_chat:
        target_chat_id = validate_chat_id(source.chat_id)
        if target_chat_id in seen_chat_ids:
            continue
        unique_chat_ids.append(target_chat_id)
        seen_chat_ids.add(target_chat_id)

    return unique_chat_ids


def collect_unique_enabled_vk_chat_ids() -> list[str]:
    with state.lock:
        raw_sources = [
            ParserSource(
                source_id=source.source_id,
                url=source.url,
                chat_id=source.chat_id,
                vk_chat_ids=source.vk_chat_ids,
                enabled=source.enabled,
            )
            for source in state.parser_sources
            if source.enabled and source.vk_chat_ids
        ]

    if not raw_sources:
        return []

    unique_chat_ids: list[str] = []
    seen_chat_ids: set[str] = set()
    for source in raw_sources:
        for chat_id in source.vk_chat_ids:
            target_chat_id = validate_vk_chat_id(chat_id)
            if target_chat_id in seen_chat_ids:
                continue
            unique_chat_ids.append(target_chat_id)
            seen_chat_ids.add(target_chat_id)

    return unique_chat_ids


@app.post("/send-test-message")
def send_test_message():
    with state.lock:
        state.error = ""
        state.info = ""

    try:
        unique_tg_chat_ids = collect_unique_enabled_chat_ids()
        unique_vk_chat_ids = collect_unique_enabled_vk_chat_ids()

        if not unique_tg_chat_ids and not unique_vk_chat_ids:
            raise RuntimeError(
                "Нужна хотя бы одна включенная ссылка с Telegram/VK chat_id для тестовой отправки"
            )

        tg_cfg: Optional[TelegramConfig] = None
        vk_cfg: Optional[VkConfig] = None
        if unique_tg_chat_ids:
            tg_cfg = load_telegram_config()
        if unique_vk_chat_ids:
            vk_cfg = load_vk_config()

        message = (
            "Тестовое сообщение Telegram + VK\n"
            "------------------------------\n"
            "Запущен AlpineBOT-v2.0.\n"
            f"Время: {now_label()}"
        )

        async def _send_test() -> tuple[list[str], list[str], list[str], Optional[int]]:
            tg_session: Optional[aiohttp.ClientSession] = None
            vk_session: Optional[aiohttp.ClientSession] = None
            try:
                if tg_cfg is not None:
                    tg_timeout = aiohttp.ClientTimeout(total=tg_cfg.request_timeout_seconds)
                    tg_session = aiohttp.ClientSession(
                        timeout=tg_timeout,
                        trust_env=tg_cfg.use_system_proxy,
                    )
                if vk_cfg is not None:
                    vk_timeout = aiohttp.ClientTimeout(total=vk_cfg.request_timeout_seconds)
                    vk_session = aiohttp.ClientSession(
                        timeout=vk_timeout,
                        trust_env=vk_cfg.use_system_proxy,
                    )

                sent_tg_chat_ids: list[str] = []
                sent_vk_chat_ids: list[str] = []
                source_errors: list[str] = []
                last_message_id: Optional[int] = None
                if tg_session is not None and tg_cfg is not None:
                    for chat_id in unique_tg_chat_ids:
                        try:
                            last_message_id = await send_telegram_match_message(
                                tg_session,
                                tg_cfg,
                                chat_id,
                                message,
                            )
                            sent_tg_chat_ids.append(chat_id)
                        except Exception as source_exc:  # noqa: BLE001
                            source_errors.append(
                                f"Telegram {chat_id}: {format_exception_details(source_exc)}"
                            )
                if vk_session is not None and vk_cfg is not None:
                    for chat_id in unique_vk_chat_ids:
                        try:
                            last_message_id = await send_vk_match_message(
                                vk_session,
                                vk_cfg,
                                chat_id,
                                message,
                            )
                            sent_vk_chat_ids.append(chat_id)
                        except Exception as source_exc:  # noqa: BLE001
                            source_errors.append(
                                f"VK {chat_id}: {format_exception_details(source_exc)}"
                            )
                return sent_tg_chat_ids, sent_vk_chat_ids, source_errors, last_message_id
            finally:
                if tg_session is not None:
                    await tg_session.close()
                if vk_session is not None:
                    await vk_session.close()

        sent_tg_chat_ids, sent_vk_chat_ids, source_errors, message_id = asyncio.run(_send_test())

        if not sent_tg_chat_ids and not sent_vk_chat_ids:
            joined = " | ".join(source_errors)
            raise RuntimeError(f"Тест не отправлен ни в один чат: {joined}")

        with state.lock:
            state.preview = (
                message
                + f"\nTelegram: {', '.join(sent_tg_chat_ids) if sent_tg_chat_ids else '-'}"
                + f"\nVK: {', '.join(sent_vk_chat_ids) if sent_vk_chat_ids else '-'}"
            )
            state.last_message_id = message_id
            state.info = (
                f"Тест отправлен: Telegram {len(sent_tg_chat_ids)} чат(ов), "
                f"VK {len(sent_vk_chat_ids)} чат(ов)"
            )
            if source_errors:
                state.error = (
                    "Часть чатов недоступна: " + " | ".join(source_errors)
                )
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Тестовая отправка в Telegram/VK не удалась: {exc}"

    return redirect(url_for("index"))


@app.post("/send-settlement-test")
def send_settlement_test():
    with state.lock:
        state.error = ""
        state.info = ""

    settlement_status = normalize_text(request.form.get("settlement_status", "win")).lower()
    if settlement_status not in {"win", "lose", "return"}:
        settlement_status = "win"

    test_profit_by_status = {
        "win": 850,
        "lose": -1000,
        "return": 0,
    }
    net_profit_units = test_profit_by_status[settlement_status]
    score_label = "2:1" if settlement_status == "win" else "1:2" if settlement_status == "lose" else "0:0"

    base_message = (
        "Тестовое сообщение исхода ставки\n"
        "------------------------------\n"
        "Проверка редактирования выполнена успешно.\n"
        f"Время отправки: {now_label()}"
    )
    footer_line = build_settlement_footer_line(
        settlement_status,
        net_profit_units,
        score_label,
    )
    updated_message = append_settlement_footer(base_message, footer_line)

    try:
        tg_cfg = load_telegram_config()
        unique_chat_ids = collect_unique_enabled_chat_ids()
        if not unique_chat_ids:
            raise RuntimeError("Нужна хотя бы одна включенная ссылка с chat_id Telegram")

        async def _send_test() -> tuple[list[str], list[str], Optional[int]]:
            timeout = aiohttp.ClientTimeout(total=tg_cfg.request_timeout_seconds)
            async with aiohttp.ClientSession(
                timeout=timeout,
                trust_env=tg_cfg.use_system_proxy,
            ) as tg_session:
                sent_chat_ids: list[str] = []
                source_errors: list[str] = []
                last_message_id: Optional[int] = None
                for chat_id in unique_chat_ids:
                    try:
                        message_id = await send_telegram_match_message(
                            tg_session,
                            tg_cfg,
                            chat_id,
                            base_message,
                        )
                        await edit_telegram_message(
                            tg_session,
                            tg_cfg,
                            chat_id,
                            message_id,
                            updated_message,
                        )
                        last_message_id = message_id
                        sent_chat_ids.append(chat_id)
                    except Exception as source_exc:  # noqa: BLE001
                        source_errors.append(
                            f"{chat_id}: {format_exception_details(source_exc)}"
                        )
                return sent_chat_ids, source_errors, last_message_id

        sent_chat_ids, source_errors, message_id = asyncio.run(_send_test())

        if not sent_chat_ids:
            joined = " | ".join(source_errors)
            raise RuntimeError(f"Тест обновления исхода не отправлен: {joined}")

        with state.lock:
            state.preview = updated_message + f"\nКаналы: {', '.join(sent_chat_ids)}"
            state.last_message_id = message_id
            state.info = (
                "Тест обновления исхода отправлен в "
                f"{len(sent_chat_ids)} канал(ов): "
                + ", ".join(sent_chat_ids)
            )
            if source_errors:
                state.error = "Часть каналов недоступна: " + " | ".join(source_errors)
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Тест обновления исхода не удался: {exc}"

    return redirect(url_for("index"))


@app.post("/blogabet-login")
def blogabet_login_route():
    with state.lock:
        state.error = ""
        state.info = ""

    try:
        blogabet_cfg = load_blogabet_config()
        publisher = BlogabetPublisher(blogabet_cfg, logger=logger)

        async def _login() -> str:
            try:
                return await publisher.interactive_login_and_save_state()
            finally:
                await publisher.close()

        storage_state_path = asyncio.run(_login())
        with state.lock:
            state.info = f"Blogabet login завершен. Storage state сохранен: {storage_state_path}"
    except Exception as exc:  # noqa: BLE001
        log_blogabet_exception("Blogabet login не выполнен: %s", humanize_parser_error(exc))
        with state.lock:
            state.error = f"Blogabet login не выполнен: {humanize_parser_error(exc)}"

    return redirect(url_for("index"))


@app.post("/blogabet-test-publish")
def blogabet_test_publish_route():
    with state.lock:
        state.error = ""
        state.info = ""
        state.blogabet_test_log = ""
        state.blogabet_test_pick_url = ""
        state.blogabet_test_screenshot_path = ""
        state.blogabet_test_html_dump_path = ""
        state.blogabet_test_diagnostics = ""

    tournament = normalize_text(request.form.get("tournament", ""))
    home_team = normalize_text(request.form.get("home_team", ""))
    away_team = normalize_text(request.form.get("away_team", ""))
    manual_score_raw = normalize_text(request.form.get("manual_score", ""))
    image_url = normalize_text(request.form.get("image_url", ""))
    manual_bet_text = (request.form.get("bet_text", "") or "").strip()
    analysis_text = normalize_text(request.form.get("analysis_text", ""))
    action = normalize_text(request.form.get("blogabet_action", "find")).lower()
    dry_run_requested = normalize_text(request.form.get("dry_run", "")) in {
        "1",
        "on",
        "true",
        "yes",
    }
    dry_run = action != "publish" or dry_run_requested

    stake_raw = normalize_text(request.form.get("stake", ""))
    uploaded_image = request.files.get("image_file")

    if not tournament or not home_team or not away_team:
        with state.lock:
            state.error = "Нужно заполнить tournament, home_team и away_team"
        return redirect(url_for("index"))

    manual_score = ""
    if manual_score_raw:
        score_match = re.search(r"(\d{1,2})\s*[:\-]\s*(\d{1,2})", manual_score_raw)
        if score_match is None:
            with state.lock:
                state.error = "Current score должен быть в формате 3:0 или 3-0"
            return redirect(url_for("index"))
        manual_score = f"{score_match.group(1)}:{score_match.group(2)}"

    try:
        stake = int(stake_raw or str(DEFAULT_BLOGABET_STAKE))
    except ValueError:
        with state.lock:
            state.error = "Stake должен быть целым числом"
        return redirect(url_for("index"))

    image_bytes: bytes = b""
    image_content_type = "image/jpeg"
    if uploaded_image is not None and normalize_text(uploaded_image.filename):
        image_bytes = uploaded_image.read()
        image_content_type = normalize_text(uploaded_image.mimetype) or "image/jpeg"

    if not manual_bet_text and not image_bytes and not image_url:
        with state.lock:
            state.error = "Нужен Bet text или image_url/upload изображения"
        return redirect(url_for("index"))

    try:
        blogabet_cfg = load_blogabet_config()
        test_blogabet_cfg = replace(blogabet_cfg, headless=False)
        ocr_client = load_ocr_client()
        publisher = BlogabetPublisher(test_blogabet_cfg, logger=logger)

        async def _run_test() -> tuple[str, str, BetIntent, PublishResult]:
            try:
                ocr_source = "manual"
                if manual_bet_text:
                    ocr_text = manual_bet_text
                else:
                    ocr_source = "ocr"
                    if image_bytes:
                        ocr_text = await ocr_client.recognize_text_from_image_bytes(
                            image_bytes,
                            image_content_type,
                        )
                    else:
                        ocr_text = await ocr_client.recognize_text_from_image_url(image_url)

                bet_intent = parse_bet_intent(ocr_text)
                if manual_score:
                    bet_intent = replace(
                        bet_intent,
                        current_score=manual_score,
                        is_live=True,
                    )
                manual_match = ParsedMatch(
                    home_team=home_team,
                    away_team=away_team,
                    tournament=tournament,
                    event_time="",
                    score=manual_score,
                    rate="",
                    rate_description=bet_intent.raw_text,
                    href=image_url or "manual://uploaded-image",
                    image_url=image_url or "manual://uploaded-image",
                    unique_key=f"manual::{tournament}::{home_team}::{away_team}",
                )
                result = await publisher.publish_pick(
                    manual_match,
                    bet_intent,
                    stake,
                    analysis_text or build_blogabet_analysis_text(manual_match, bet_intent, ocr_text),
                    dry_run=dry_run,
                    diagnostics_context={"test_action": action},
                )
                return ocr_source, ocr_text, bet_intent, result
            finally:
                await publisher.close()

        ocr_source, ocr_text, bet_intent, result = asyncio.run(_run_test())
        diagnostics_text = BlogabetPublisher.format_diagnostics(result.diagnostics)
        bet_json = json.dumps(bet_intent.__dict__, ensure_ascii=False, indent=2)

        with state.lock:
            state.blogabet_test_pick_url = normalize_text(result.pick_url or "")
            state.blogabet_test_diagnostics = diagnostics_text
            state.blogabet_test_log = (
                f"Action: {action}\n"
                f"Dry run: {'yes' if dry_run else 'no'}\n"
                f"Bet source: {ocr_source}\n"
                f"Manual score: {manual_score or '-'}\n"
                f"OCR text:\n{ocr_text}\n\n"
                f"Bet intent:\n{bet_json}\n\n"
                f"Publish success: {result.success}\n"
            )
            state.info = (
                "Blogabet test выполнен успешно (dry run)."
                if dry_run
                else "Blogabet test publish выполнен успешно."
            )
    except BlogabetPublishError as exc:
        log_blogabet_exception(
            "Blogabet test publish error: step=%s reason=%s",
            exc.step_name,
            exc.reason,
        )
        with state.lock:
            state.blogabet_test_screenshot_path = exc.screenshot_path
            state.blogabet_test_html_dump_path = exc.html_dump_path
            state.blogabet_test_diagnostics = BlogabetPublisher.format_diagnostics(exc.diagnostics)
            state.blogabet_test_log = (
                f"Step: {exc.step_name}\n"
                f"Reason: {exc.reason}\n"
            )
            state.error = f"Blogabet test не выполнен: {exc.step_name} | {exc.reason}"
    except OcrError as exc:
        log_blogabet_exception("Blogabet test OCR error: %s", humanize_parser_error(exc))
        with state.lock:
            state.error = (
                "Blogabet test не выполнен: ошибка OCR. "
                f"{humanize_parser_error(exc)}. "
                "Проверь OCR_SPACE_API_KEY или укажи Bet text для теста без OCR."
            )
    except Exception as exc:  # noqa: BLE001
        log_blogabet_exception("Blogabet test publish unexpected error: %s", humanize_parser_error(exc))
        with state.lock:
            state.error = f"Blogabet test не выполнен: {humanize_parser_error(exc)}"

    return redirect(url_for("index"))


@app.post("/blogabet-test-ocr")
def blogabet_test_ocr_route():
    with state.lock:
        state.error = ""
        state.info = ""
        state.blogabet_ocr_log = ""

    image_url = normalize_text(request.form.get("image_url", ""))
    uploaded_image = request.files.get("image_file")
    image_bytes: bytes = b""
    image_content_type = "image/jpeg"
    if uploaded_image is not None and normalize_text(uploaded_image.filename):
        image_bytes = uploaded_image.read()
        image_content_type = normalize_text(uploaded_image.mimetype) or "image/jpeg"

    if not image_url and not image_bytes:
        with state.lock:
            state.error = "Нужен Image URL или upload изображения для OCR"
        return redirect(url_for("index"))

    try:
        ocr_client = load_ocr_client()

        async def _run_ocr() -> tuple[str, BetIntent, str]:
            if image_bytes:
                ocr_text = await ocr_client.recognize_text_from_image_bytes(
                    image_bytes,
                    content_type=image_content_type,
                )
                source = "upload"
            else:
                ocr_text = await ocr_client.recognize_text_from_image_url(image_url)
                source = "url"
            bet_intent = parse_bet_intent(ocr_text)
            return ocr_text, bet_intent, source

        ocr_text, bet_intent, source = asyncio.run(_run_ocr())
        bet_json = json.dumps(bet_intent.__dict__, ensure_ascii=False, indent=2)
        with state.lock:
            state.blogabet_ocr_log = (
                f"Source: {source}\n"
                f"Image URL: {image_url or '-'}\n\n"
                f"OCR text:\n{ocr_text}\n\n"
                f"Bet intent:\n{bet_json}\n"
            )
            state.info = "OCR распознавание выполнено."
    except OcrError as exc:
        log_blogabet_exception("Blogabet OCR tool error: %s", humanize_parser_error(exc))
        with state.lock:
            state.error = f"OCR не выполнен: {humanize_parser_error(exc)}"
    except Exception as exc:  # noqa: BLE001
        log_blogabet_exception("Blogabet OCR tool unexpected error: %s", humanize_parser_error(exc))
        with state.lock:
            state.error = f"OCR не выполнен: {humanize_parser_error(exc)}"

    return redirect(url_for("index"))


@app.post("/update-telegram-token")
def update_telegram_token():
    new_token = request.form.get("telegram_bot_token", "").strip()

    with state.lock:
        state.error = ""
        state.info = ""

    if not new_token:
        with state.lock:
            state.error = "Нужно передать TELEGRAM_BOT_TOKEN"
        return redirect(url_for("index"))

    try:
        upsert_env_value("TELEGRAM_BOT_TOKEN", new_token)
        os.environ["TELEGRAM_BOT_TOKEN"] = new_token

        with state.lock:
            state.info = "TELEGRAM_BOT_TOKEN обновлён"
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось обновить TELEGRAM_BOT_TOKEN: {exc}"

    return redirect(url_for("index"))


@app.post("/update-vk-token")
def update_vk_token():
    new_token = request.form.get("vk_user_token", "").strip()

    with state.lock:
        state.error = ""
        state.info = ""

    if not new_token:
        with state.lock:
            state.error = "Нужно передать VK_USER_TOKEN"
        return redirect(url_for("index"))

    try:
        upsert_env_value("VK_USER_TOKEN", new_token)
        os.environ["VK_USER_TOKEN"] = new_token

        with state.lock:
            state.info = "VK_USER_TOKEN обновлён"
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.error = f"Не удалось обновить VK_USER_TOKEN: {exc}"

    return redirect(url_for("index"))


@app.post("/fetch-vk-chat-ids")
def fetch_vk_chat_ids_route():
    with state.lock:
        state.error = ""
        state.info = ""

    limit_raw = normalize_text(request.form.get("vk_chat_lookup_limit", "200"))

    try:
        if not limit_raw:
            limit_raw = "200"
        lookup_limit = int(limit_raw)
        if lookup_limit < 1 or lookup_limit > 200:
            raise ValueError("Лимит должен быть числом от 1 до 200")

        vk_cfg = load_vk_config()

        async def _fetch() -> list[dict[str, Any]]:
            vk_timeout = aiohttp.ClientTimeout(total=vk_cfg.request_timeout_seconds)
            async with aiohttp.ClientSession(
                timeout=vk_timeout,
                trust_env=vk_cfg.use_system_proxy,
            ) as vk_session:
                return await fetch_vk_chat_peer_ids(
                    vk_session,
                    vk_cfg,
                    limit=lookup_limit,
                )

        chats = asyncio.run(_fetch())

        with state.lock:
            state.vk_chat_lookup_results = chats
            state.vk_chat_lookup_loaded = True
            state.vk_chat_lookup_limit = lookup_limit
            state.vk_chat_lookup_last_at = now_label_msk()
            state.info = (
                f"Загружено {len(chats)} VK чат(ов) "
                f"из первых {lookup_limit} диалогов."
            )
    except Exception as exc:  # noqa: BLE001
        with state.lock:
            state.vk_chat_lookup_results = []
            state.vk_chat_lookup_loaded = False
            state.vk_chat_lookup_last_at = ""
            state.error = f"Не удалось получить VK peer_id чатов: {exc}"

    return redirect(url_for("index"))


@app.post("/reset")
def reset():
    state.reset()
    return redirect(url_for("index"))


if __name__ == "__main__":
    load_dotenv()
    app.run(host="127.0.0.1", port=int(
        os.getenv("LOCAL_WEB_PORT", "5000")), debug=False)
