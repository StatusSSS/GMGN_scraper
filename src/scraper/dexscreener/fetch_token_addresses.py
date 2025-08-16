# fetch_token_addresses.py
from __future__ import annotations

import os
import sys
import re
import json
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.sdk.queues.redis_connect import get_redis_sync as get_redis

# ────────────────────────── Конфиг ──────────────────────────

# Прокси формата: host:port:user:pass
SEL_PROXY = os.getenv("SEL_PROXY", "178.171.42.135:9056:t1d496:grovgA")

# URL вашего Selenium Grid/Standalone
# Если по compose из вопроса и запускаете со своего хоста: http://localhost:4445/wd/hub
SELENIUM_SERVER_URL = os.getenv("SELENIUM_SERVER_URL", "http://localhost:4445/wd/hub")

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

# Включить подробные сообщения Selenium/Chrome
QUIET = os.getenv("QUIET", "1") == "1"

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

# ────────────────────────── Proxy MV3 ───────────────────────

def _parse_proxy(raw: str) -> Tuple[str, int, str, str]:
    """host:port:user:pass → (host, port, user, pass)"""
    host, port, user, pwd = raw.split(":", 3)
    return host, int(port), user, pwd

def _build_auth_proxy_extension_zip(host: str, port: int, user: str, pwd: str) -> str:
    """
    Создаёт временный ZIP c MV3-расширением Chrome, которое:
      - задаёт fixed proxy (HTTP)
      - отдаёт credentials в onAuthRequired
    Возвращает путь к zip.
    """
    manifest = {
        "name": "auth-proxy",
        "version": "1.0",
        "manifest_version": 3,
        "permissions": [
            "proxy",
            "storage",
            "webRequest",
            "webRequestAuthProvider"
        ],
        "host_permissions": ["<all_urls>"],
        "background": {"service_worker": "background.js"},
        "minimum_chrome_version": "110"
    }

    background_js = f"""// generated
chrome.proxy.settings.set({{
  value: {{
    mode: "fixed_servers",
    rules: {{
      singleProxy: {{ scheme: "http", host: "{host}", port: {port} }},
      bypassList: ["localhost","127.0.0.1"]
    }}
  }},
  scope: "regular"
}}, () => {{}});

// MV3: asyncBlocking для ввода логина/пароля
chrome.webRequest.onAuthRequired.addListener(
  (details, callback) => {{
    callback({{authCredentials: {{username: "{user}", password: "{pwd}"}}}});
  }},
  {{ urls: ["<all_urls>"] }},
  ["asyncBlocking"]
);
"""

    tmpdir = tempfile.mkdtemp(prefix="pxyext_")
    zippath = os.path.join(tmpdir, "proxy_auth_ext.zip")
    with zipfile.ZipFile(zippath, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False))
        z.writestr("background.js", background_js)
    return zippath

def _make_driver() -> WebDriver:
    host, port, user, pwd = _parse_proxy(SEL_PROXY)
    ext_zip = _build_auth_proxy_extension_zip(host, port, user, pwd)

    opts = webdriver.ChromeOptions()

    # Базовые флаги стабильности
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-features=BlockInsecurePrivateNetworkRequests")
    opts.add_argument("--disable-quic")  # избегаем QUIC, чтобы трафик точно шёл через HTTP-прокси

    # Можно добавить явный язык/UA при необходимости:
    # opts.add_argument("--lang=ru-RU")

    # Подключаем MV3-расширение с прокси-авторизацией
    opts.add_extension(ext_zip)

    if QUIET:
        # Чуть тише логи дисплея/крошек
        opts.add_experimental_option("excludeSwitches", ["enable-logging"])
        opts.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Remote(
        command_executor=SELENIUM_SERVER_URL,
        options=opts
    )
    return driver

# ────────────────────────── Ожидания/страницы ───────────────

def _wait_table(driver: WebDriver, timeout: int = 600):
    """Ждём появления основной таблицы после капчи."""
    sel = "div.ds-dex-table.ds-dex-table-top"
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
    )

def _check_ip(driver: WebDriver) -> None:
    try:
        driver.get("https://api.ipify.org?format=text")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "pre"))
        )
        ip = driver.find_element(By.TAG_NAME, "pre").text
        print(f"[ip] {ip}")
    except Exception as e:
        print(f"[ip] check failed: {e}")

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
        # Найдём, к какому источнику относится строка (по значку с title)
        titles = [img.get("title", "").strip().lower() for img in row.select("img[title]")]
        found = next((t for t in titles if t in flag_set), None)
        if not found:
            continue
        # Нормализуем имя флага как в ключах словаря
        flag_found = next(f for f in flags if f.lower() == found)

        # Ищем иконку токена → адрес в src
        icon = row.select_one("img.ds-dex-table-row-token-icon-img[src*='/tokens/solana/']")
        if not icon:
            continue
        m = _token_src_re.search(icon["src"])
        if m:
            result[flag_found].append(m.group(1))

    # Убираем дубли, сохраняя порядок
    for flag, lst in result.items():
        result[flag] = list(dict.fromkeys(lst))
    return result

# ────────────────────────── Скрейпер ────────────────────────

def run(max_age_hours: int, out_dir: Path) -> str:
    """
    Открывает DexScreener, ждёт таблицу (после ручного решения капчи через VNC),
    возвращает HTML таблицы.
    """
    url = (
        "https://dexscreener.com/solana"
        f"?rankBy=trendingScoreH6&order=desc&minMarketCap=50000&maxAge={max_age_hours}"
    )
    print("[open]", url)

    driver = _make_driver()
    try:
        if CHECK_IP:
            _check_ip(driver)

        driver.get(url)
        print("[wait] Решите CAPTCHA в noVNC; ждём таблицу…")
        table_el = _wait_table(driver)

        out_dir.mkdir(parents=True, exist_ok=True)
        html = table_el.get_attribute("outerHTML") or ""
        (out_dir / "last_table.html").write_text(html, encoding="utf-8")
        print(f"[save] {out_dir / 'last_table.html'}")
        return html
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        print("[quit] browser closed")

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
