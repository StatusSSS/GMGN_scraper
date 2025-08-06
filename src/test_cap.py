#!/usr/bin/env python
"""
Получает задачи из очереди cookie_tasks, открывает gmgn.ai через
Selenium-прокси, ждёт, пока оператор кликнет капчу, и сохраняет cookies
в Redis под ключом  cookies:<full-proxy>.

proxy в задаче теперь полный:  ip:port:user:pwd
Selenium получает только ip:port (user/password ему не нужны).
"""

import json
import os
import sys
import time
import redis
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ─── конфиг ──────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
SELENIUM_URL = os.getenv(
    "SELENIUM_SERVER_URL", "http://selenium_testcap:4444/wd/hub"
)

API_URL = "https://gmgn.ai/new-pair"          # страница, где появляется капча
WAIT_FOR_CLICK = int(os.getenv("WAIT_FOR_CLICK", 30))   # секунд ждать оператора

# ─── Redis ───────────────────────────────────────────────────────────
rds = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)

# немедленная печать логов
sys.stdout.reconfigure(line_buffering=True)

# ─── helpers ─────────────────────────────────────────────────────────
def get_task() -> dict:
    """Блокирующе берёт следующую задачу из cookie_tasks."""
    _key, raw = rds.blpop("cookie_tasks")
    return json.loads(raw)         # {"proxy": "ip:port:user:pwd", "ua": "…"}


def build_opts(proxy: str, ua: str) -> Options:
    """
    Готовит ChromeOptions.

    proxy — строка ip:port:user:pwd  (берём только ip:port для Selenium).
    """
    host, port, *_ = proxy.split(":", 3)

    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_argument(f"--proxy-server=http://{host}:{port}")
    return opts


def save_cookies(proxy: str, ua: str, driver):
    payload = {"ua": ua, "cookies": driver.get_cookies()}
    rds.set(f"cookies:{proxy}", json.dumps(payload))
    # 👇 добавьте подробный лог
    print(f"[SAVE] {proxy} ua={ua[:30]}… "
          f"{[c['name'] for c in payload['cookies']]}")


# ─── основной цикл ───────────────────────────────────────────────────
while True:
    task = get_task()
    proxy, ua = task["proxy"], task["ua"]

    print("\n" + "=" * 60, f"\nProxy: {proxy}\nUA: {ua}")

    driver = webdriver.Remote(SELENIUM_URL, options=build_opts(proxy, ua))
    try:
        driver.get(API_URL)
        print(f"⏳  wait click ({WAIT_FOR_CLICK}s)…")
        time.sleep(WAIT_FOR_CLICK)

        save_cookies(proxy, ua, driver)
    except Exception as exc:
        # печатаем в stderr, чтобы Docker-логи пометили как ERROR
        print(f"[ERR] {type(exc).__name__}: {exc}", file=sys.stderr)
    finally:
        print("[SEL_HEADERS] UA=", driver.execute_script("return navigator.userAgent"))
        print("[SEL_COOKIES] ",
              {c['name']: c['value'] for c in driver.get_cookies()})
        driver.quit()
        print("➡️  next proxy…")
