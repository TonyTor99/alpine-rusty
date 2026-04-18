# Alpinbet -> Telegram Parser

Веб-приложение для входа в Alpinbet, парсинга вкладки **Активные** и отправки новых матчей в Telegram.

## Что умеет

- вход через веб-панель с 2FA-кодом;
- парсинг нескольких ссылок Alpinbet;
- привязка каждой ссылки к своему `chat_id` Telegram;
- отдельное включение/выключение каждой ссылки;
- асинхронный цикл парсинга и асинхронная отправка в Telegram;
- тестовая отправка в Telegram из панели.
- ссылки и `chat_id` сохраняются в `parser_sources.json` и поднимаются после перезапуска.
- матчи и отправки сохраняются в БД (`sqlite` по умолчанию), поэтому статусы не теряются после рестарта;
- когда матч пропадает из **Активных**, бот продолжает искать его во вкладке **Прошедшие**, фиксирует результат в БД и редактирует исходное сообщение в Telegram.

## Быстрый старт

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
python3 app.py
```

Открой: `http://127.0.0.1:5050`

По умолчанию ссылки не добавляются автоматически: добавь нужные источники вручную в панели.

## Обязательные переменные `.env`

- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота.
- `TARGET_LOGIN_URL` — URL входа в Alpinbet.
- `TARGET_DATA_URL` — базовый URL Alpinbet (в список источников не добавляется автоматически).
- `TARGET_LOGIN_USERNAME` — логин/e-mail.
- `TARGET_PASSWORD_SELECTOR`
- `TARGET_SUBMIT_SELECTOR`
- `TARGET_CODE_SELECTOR`
- `TARGET_CODE_SUBMIT_SELECTOR`

## Полезные переменные `.env`

- `TELEGRAM_REQUEST_TIMEOUT_SECONDS` — таймаут запросов в Telegram (по умолчанию `20`).
- `TELEGRAM_USE_SYSTEM_PROXY` — использовать системные прокси для Telegram API (`0/1`), по умолчанию `0`.
- `PARSER_INTERVAL_SECONDS` — интервал проверки, минимум `10`.
- `PARSER_SEND_EXISTING_ON_START` — отправлять существующие матчи при старте (`1`) или считать их уже отправленными (`0`).
- `PARSER_DELIVERY_TELEGRAM_ENABLED` — отправлять сигналы в Telegram (`1`) или только собирать без отправки (`0`).
- `PARSER_DELIVERY_VK_ENABLED` — отправлять сигналы в VK (`1`) или только собирать без отправки (`0`).
- `TARGET_HEADLESS` — `0/1`.
- `APP_LOG_LEVEL` — `DEBUG/INFO/WARNING/ERROR`.
- `BLOGABET_ADMIN_TG_CHAT_ID` — chat_id для админ-алертов по сбоям Blogabet. Можно несколько через запятую/`;`/перенос строки.
- `BLOGABET_LEAGUE_ALIASES_PATH` — путь к JSON с ручными алиасами лиг Blogabet (по умолчанию `./blogabet_league_aliases.json`).
- `MATCH_DATABASE_URL` — URL БД для трекинга матчей.
  - по умолчанию: `sqlite:///parser_matches.db`
  - для PostgreSQL: `postgresql://user:password@host:5432/dbname`
  - для PostgreSQL нужен драйвер `psycopg` (`pip install psycopg[binary]`).

При проблемах с сопоставлением лиг/матчей Blogabet пишет JSONL-диагностику в `blogabet_mismatches.jsonl`.

## Важное по Telegram

- Для каналов обычно нужен `chat_id` вида `-100...`.
- Бот должен быть добавлен в канал и иметь право отправки сообщений.
- В панели у каждой ссылки есть отдельное поле `chat_id`.
