from __future__ import annotations
import os
import sys
import re
from pathlib import Path
from typing import Dict, List

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webdriver import WebDriver

from src.sdk.queues.redis_connect import get_redis_sync as get_redis


FLAG_QUEUES: Dict[str, str] = {
    "PumpSwap": os.getenv("PUMP_QUEUE",    "pump_queue"),
    "Raydium":  os.getenv("RAYDIUM_QUEUE", "raydium_queue"),
    "Meteora":  os.getenv("METEORA_QUEUE", "meteora_queue"),
}
RESET_TOKENS_QUEUE = os.getenv("RESET_TOKENS_QUEUE", "0") == "1"

sys.stdout.reconfigure(line_buffering=True)  # принудительно выводим логи сразу


# ────────────────────────── Redis helpers ───────────────────
def push_tokens(tokens: List[str], queue: str) -> None:
    """Кладёт адреса токенов в Redis-лист `queue` (по одному элементу)."""
    if not tokens:
        return

    rds = get_redis()
    if RESET_TOKENS_QUEUE:
        rds.delete(queue)
        print(f"🧹  Очистили очередь {queue}")

    rds.rpush(queue, *tokens)
    print(f"🚚  Отправили {len(tokens)} токенов → {queue}")



def _get_driver() -> WebDriver:
    grid_url = os.getenv("SELENIUM_SERVER_URL", "http://localhost:4444/wd/hub")
    opts = webdriver.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-infobars")
    return webdriver.Remote(command_executor=grid_url, options=opts)


def _wait_table(driver: WebDriver, timeout: int = 600):
    """Ждём появления основной таблицы после прохождения CAPTCHA."""
    sel = "div.ds-dex-table.ds-dex-table-top"
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
    )


# ────────────────────────── HTML parser ─────────────────────
_token_src_re = re.compile(r"/tokens/solana/([A-Za-z0-9]+)\.png", re.IGNORECASE)
FLAGS = list(FLAG_QUEUES.keys())  # ["PumpSwap", "Raydium", "Meteora"]


def extract_tokens_by_flag(html: str, flags: List[str] = FLAGS) -> Dict[str, List[str]]:
    """
    Выдёргивает адреса токенов по каждому интересующему флагу.
    Возвращает словарь {flag: [addr, …]}.
    """
    soup = BeautifulSoup(html, "html.parser")
    result: Dict[str, List[str]] = {flag: [] for flag in flags}

    for row in soup.select("a.ds-dex-table-row"):
        flag_found: str | None = None
        # Определяем, к какому источнику относится строка
        for flag in flags:
            if row.select_one(f'img[title="{flag}" i]'):
                flag_found = flag
                break
        if flag_found is None:
            continue  # ни один из интересующих флагов не найден

        # Ищем иконку токена, чтобы вытащить адрес из её src
        icon = row.select_one(
            "img.ds-dex-table-row-token-icon-img[src*='/tokens/solana/']"
        )
        if not icon:
            continue

        m = _token_src_re.search(icon["src"])
        if m:
            result[flag_found].append(m.group(1))

    # Убираем возможные дубликаты, сохраняя порядок
    for flag, lst in result.items():
        result[flag] = list(dict.fromkeys(lst))

    return result


# ────────────────────────── Scraper core ────────────────────
def run(max_age_hours: int, out_dir: Path) -> str:
    """
    Забирает HTML основной таблицы DexScreener.
    Возвращает HTML как строку.
    """
    url = (
        "https://dexscreener.com/solana"
        f"?rankBy=trendingScoreH6&order=desc&minMarketCap=50000&maxAge={max_age_hours}"
    )
    print("[open]", url)

    driver = _get_driver()
    try:
        driver.get(url)
        print("[wait] Solve CAPTCHA via VNC; waiting for table…")
        table_el = _wait_table(driver)

        # Сохраняем HTML (пригодится для отладки)
        out_dir.mkdir(parents=True, exist_ok=True)
        html = table_el.get_attribute("outerHTML")
        (out_dir / "last_table.html").write_text(html, encoding="utf-8")

        return html

    finally:
        driver.quit()
        print("[quit] browser closed")


def run_and_push(max_age_hours: int, out_dir: Path) -> Dict[str, List[str]]:
    """
    Парсит страницу, раскладывает токены по очередям Redis
    и возвращает словарь {flag: [addresses]}.
    """
    html = run(max_age_hours, out_dir)
    tokens_by_flag = extract_tokens_by_flag(html)

    for flag, tokens in tokens_by_flag.items():
        push_tokens(tokens, FLAG_QUEUES[flag])

    return tokens_by_flag



if __name__ == "__main__":
    hours = int(os.getenv("MAX_AGE_HOURS", "24"))
    output_dir = Path(os.getenv("OUTPUT_DIR", "./"))

    tokens_by_flag = run_and_push(hours, output_dir)

    for flag, tokens in tokens_by_flag.items():
        print(f"\n{flag} addresses ({len(tokens)}):")
        print("\n".join(tokens))
