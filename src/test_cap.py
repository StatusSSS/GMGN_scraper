#!/usr/bin/env python
"""
Получает задачи из очереди cookie_tasks, открывает gmgn.ai через
Selenium-прокси, ждёт, пока оператор кликнет капчу, и сохраняет cookies
в Redis под ключом  cookies:<full-proxy>.
"""

import json, os, sys, time, uuid
import redis
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

# ─── конфиг ──────────────────────────────────────────────────────────
REDIS_HOST   = os.getenv("REDIS_HOST", "redis")
REDIS_PORT   = int(os.getenv("REDIS_PORT", 6379))
SELENIUM_URL = os.getenv("SELENIUM_SERVER_URL",
                         "http://selenium_testcap:4444/wd/hub")

POST_PAGE_URL  = (
    "https://gmgn.ai/sol/address/9NQn3TLma9sKd6pbBuXy5we6PMs6QLWAz8awhZyGNgmo"
)          # любая существующая страница
WAIT_FOR_CLICK = int(os.getenv("WAIT_FOR_CLICK", 30))   # сек на ручной клик

# ─── Redis ───────────────────────────────────────────────────────────
rds = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)
sys.stdout.reconfigure(line_buffering=True)             # мгновенный лог

# ─── helpers ─────────────────────────────────────────────────────────
def get_task() -> dict:
    """Блокирующе берёт следующую задачу из cookie_tasks."""
    _key, raw = rds.blpop("cookie_tasks")
    return json.loads(raw)          # {"proxy": "ip:port:user:pwd", "ua": "…"}


def build_opts(proxy: str, ua: str) -> Options:
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
    print(f"[SAVE] {proxy} ua={ua[:30]}… "
          f"{[c['name'] for c in payload['cookies']]}")

# ─── основной цикл ───────────────────────────────────────────────────
while True:
    task = get_task()
    proxy, ua = task["proxy"], task["ua"]

    print("\n" + "=" * 60, f"\nProxy: {proxy}\nUA: {ua}")
    driver = webdriver.Remote(SELENIUM_URL, options=build_opts(proxy, ua))

    try:
        # 1️⃣ открываем страницу, оператор решает капчу
        driver.get(POST_PAGE_URL)
        print(f"⏳  wait click ({WAIT_FOR_CLICK}s)…")
        time.sleep(WAIT_FOR_CLICK)

        # 2️⃣ статические first-party куки
        for name, val in {
            "GMGN_CHAIN":  "sol",
            "GMGN_THEME":  "dark",
            "GMGN_LOCALE": "ru",
        }.items():
            try:
                driver.add_cookie({"name": name, "value": val,
                                   "domain": ".gmgn.ai", "path": "/"})
            except Exception:
                pass

        # 3️⃣ ждём sid (до 8 c); если нет — ставим свой
        try:
            WebDriverWait(driver, 8).until(
                lambda d: any(c["name"] == "sid" for c in d.get_cookies())
            )
        except Exception:
            sid = f"gmgn|{uuid.uuid4().hex}"
            driver.add_cookie({"name": "sid", "value": sid,
                               "domain": ".gmgn.ai", "path": "/"})
            print(f"[SID] custom sid set → {sid}")

        # 4️⃣ сохраняем куки в Redis
        save_cookies(proxy, ua, driver)

    except Exception as exc:
        print(f"[ERR] {type(exc).__name__}: {exc}", file=sys.stderr)

    finally:
        try:
            print("[SEL_HEADERS] UA=",
                  driver.execute_script("return navigator.userAgent"))
            print("[SEL_COOKIES] ",
                  {c['name']: c['value'] for c in driver.get_cookies()})
        except Exception:
            pass

        driver.quit()
        print("➡️  next proxy…")
