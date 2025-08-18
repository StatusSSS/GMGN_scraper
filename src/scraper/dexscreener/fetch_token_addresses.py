# fetch_token_addresses.py  — Playwright + Selenium Grid
from __future__ import annotations

import os
import sys
import re
import json
from pathlib import Path
from typing import Dict, List, Tuple

import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.sdk.queues.redis_connect import get_redis_sync as get_redis

# ────────────────────────── Конфиг ──────────────────────────

# NB: Прокси теперь настраиваем на уровне ноды Grid (env HTTP(S)_PROXY
#     или через SELENIUM_REMOTE_CAPABILITIES), см. примечания ниже.
SEL_PROXY = os.getenv("SEL_PROXY", "196.18.2.145:8000:ZvMv3G:3ySzNZ")

# Очереди для раскладки токенов по источникам
FLAG_QUEUES: Dict[str, str] = {
    "PumpSwap": os.getenv("PUMP_QUEUE", "pump_queue"),
    "Raydium": os.getenv("RAYDIUM_QUEUE", "raydium_queue"),
    "Meteora": os.getenv("METEORA_QUEUE", "meteora_queue"),
}

# Если "1" — очистить очереди перед пушем
RESET_TOKENS_QUEUE = os.getenv("RESET_TOKENS_QUEUE", "0") == "1"

# Куда сложить HTML-дамп таблицы (для отладки)
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./dexscreener_out"))

# Сколько часов свежести (параметр maxAge на dexscreener)
MAX_AGE_HOURS = int(os.getenv("MAX_AGE_HOURS", "24"))

# Проверять внешний IP через прокси (GET https://api.ipify.org)
CHECK_IP = os.getenv("CHECK_IP", "0") == "1"

# Делать браузер headful для работы через noVNC (обычно нужно True)
HEADFUL = os.getenv("HEADFUL", "1") == "1"

# Заводить persistent-контекст (обычно не требуется)
PERSISTENT = os.getenv("PERSISTENT", "0") == "1"

# Принудительно задать прокси на уровне Playwright (как правило,
# при работе с Grid ЭТО НЕ НУЖНО и может игнорироваться нодой)
FORCE_PLAYWRIGHT_PROXY = os.getenv("FORCE_PLAYWRIGHT_PROXY", "0") == "1"

sys.stdout.reconfigure(line_buffering=True)

# ────────────────────────── Redis ───────────────────────────

def push_tokens(tokens: List[str], queue: str) -> None:
    if not tokens:
        return
    rds = get_redis()
    if RESET_TOKENS_QUEUE:
        rds.delete(queue)
        print(f"🧹  Очистили очередь {queue}")
    rds.rpush(queue, *tokens)
    print(f"🚚  Отправили {len(tokens)} токенов → {queue}")

# ────────────────────────── Парсер HTML ─────────────────────

_token_src_re = re.compile(r"/tokens/solana/([A-Za-z0-9]+)\.png", re.IGNORECASE)
FLAGS = list(FLAG_QUEUES.keys())  # ["PumpSwap", "Raydium", "Meteora"]

def extract_tokens_by_flag(html: str, flags: List[str] = FLAGS) -> Dict[str, List[str]]:
    """
    Скрейп адреса токенов по каждому интересующему флагу.
    Возвращает {flag: [addr, …]}.
    """
    soup = BeautifulSoup(html, "html.parser")
    result: Dict[str, List[str]] = {flag: [] for flag in flags}
    flag_set = {f.lower() for f in flags}

    for row in soup.select("a.ds-dex-table-row"):
        titles = [img.get("title", "").strip().lower() for img in row.select("img[title]")]
        found = next((t for t in titles if t in flag_set), None)
        if not found:
            continue
        flag_found = next(f for f in flags if f.lower() == found)
        icon = row.select_one("img.ds-dex-table-row-token-icon-img[src*='/tokens/solana/']")
        if not icon:
            continue
        m = _token_src_re.search(icon["src"])
        if m:
            result[flag_found].append(m.group(1))

    for flag, lst in result.items():
        result[flag] = list(dict.fromkeys(lst))
    return result

# ────────────────────────── Скрейпер (Playwright) ───────────

DEX_URL_TPL = (
    "https://dexscreener.com/solana"
    "?rankBy=trendingScoreH6&order=desc&minMarketCap=50000&maxAge={max_age}"
)
TABLE_SEL = "div.ds-dex-table.ds-dex-table-top"

async def _check_ip(page) -> None:
    try:
        await page.goto("https://api.ipify.org?format=text", wait_until="domcontentloaded", timeout=30000)
        txt = await page.evaluate("() => document.body.innerText || ''")
        txt = (txt or "").strip().splitlines()[0] if txt else ""
        print(f"[ip] {txt}")
    except Exception as e:
        print(f"[ip] check failed: {e}")

def _parse_proxy(raw: str) -> Tuple[str, int, str, str]:
    host, port, user, pwd = raw.split(":", 3)
    return host, int(port), user, pwd

async def run_async(max_age_hours: int, out_dir: Path) -> str:
    """
    Открывает DexScreener в браузере, поднятом через Selenium Grid.
    Ждём таблицу (после ручного решения капчи через noVNC), сохраняем её HTML и возвращаем строкой.
    """
    url = DEX_URL_TPL.format(max_age=max_age_hours)
    print("[open]", url)

    # Взаимодействовать с Grid — просто обычный launch(); Playwright сам возьмёт SELENIUM_REMOTE_URL из env.
    # См. официальную доку (experimental).  # noqa
    # HEADS UP: работает только для Chromium/Edge.  # noqa

    proxy_opt = None
    if FORCE_PLAYWRIGHT_PROXY:
        host, port, user, pwd = _parse_proxy(SEL_PROXY)
        proxy_opt = {"server": f"http://{host}:{port}", "username": user, "password": pwd}

    async with async_playwright() as p:
        # 1) Браузер
        # headless=False, чтобы видеть окно в noVNC
        browser = await p.chromium.launch(headless=not HEADFUL, proxy=proxy_opt)

        # 2) Контекст/страница
        if PERSISTENT:
            # persistent-профиль редко нужен; не включаем расширений
            context = await browser.new_context(no_viewport=True, locale="ru-RU")
        else:
            context = await browser.new_context(no_viewport=True, locale="ru-RU")
        page = await context.new_page()
        page.set_default_timeout(600_000)  # до 10 минут (капча руками)

        # 3) IP-проверка по желанию
        if CHECK_IP:
            await _check_ip(page)

        # 4) DexScreener + ожидание таблицы
        await page.goto(url, wait_until="domcontentloaded")
        print("[wait] Решите CAPTCHA в noVNC; ждём таблицу…")
        await page.wait_for_selector(TABLE_SEL, state="attached", timeout=600_000)

        # 5) Сохранить HTML
        table_el = page.locator(TABLE_SEL).first
        html = await table_el.evaluate("el => el.outerHTML")  # outerHTML таблицы

        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "last_table.html").write_text(html, encoding="utf-8")
        print(f"[save] {out_dir / 'last_table.html'}")

        await context.close()
        await browser.close()
        print("[quit] browser closed")
        return html

def run(max_age_hours: int, out_dir: Path) -> str:
    return asyncio.run(run_async(max_age_hours, out_dir))

def run_and_push(max_age_hours: int, out_dir: Path) -> Dict[str, List[str]]:
    html = run(max_age_hours, out_dir)
    tokens_by_flag = extract_tokens_by_flag(html)
    for flag, tokens in tokens_by_flag.items():
        push_tokens(tokens, FLAG_QUEUES[flag])
    return tokens_by_flag

# ────────────────────────── CLI ─────────────────────────────

if __name__ == "__main__":
    tokens_by_flag = run_and_push(MAX_AGE_HOURS, OUTPUT_DIR)
    for flag, tokens in tokens_by_flag.items():
        print(f"\n{flag} addresses ({len(tokens)}):")
        print("\n".join(tokens))
