#!/usr/bin/env python
import json, os, sys, time, redis
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
SELENIUM_URL = os.getenv("SELENIUM_SERVER_URL",
                         "http://selenium_testcap:4444/wd/hub")

API_URL        = "https://gmgn.ai/new-pair"
WAIT_FOR_CLICK = int(os.getenv("WAIT_FOR_CLICK", 30))

rds = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)
sys.stdout.reconfigure(line_buffering=True)

# ─── helpers ─────────────────────────────────────────────────────────
def get_task() -> dict:
    _key, raw = rds.blpop("cookie_tasks")
    return json.loads(raw)          # {"proxy": "ip:port", "ua": "…"}

def build_opts(proxy: str, ua: str) -> Options:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_argument(f"--proxy-server=http://{proxy}")   # напрямую!
    return opts

def save_cookies(proxy: str, ua: str, driver):
    rds.set(f"cookies:{proxy}",
            json.dumps({"ua": ua, "cookies": driver.get_cookies()}))
    print(f"[OK] cookies saved → cookies:{proxy}")

# ─── основной цикл ───────────────────────────────────────────────────
while True:
    task = get_task()
    proxy, ua = task["proxy"], task["ua"]
    print("\n" + "="*60, f"\nProxy: {proxy}\nUA: {ua}")

    driver = webdriver.Remote(SELENIUM_URL, options=build_opts(proxy, ua))
    try:
        driver.get(API_URL)
        print(f"⏳  wait click ({WAIT_FOR_CLICK}s)…")
        time.sleep(WAIT_FOR_CLICK)

        save_cookies(proxy, ua, driver)
    except Exception as exc:
        print(f"[ERR] {type(exc).__name__}: {exc}", file=sys.stderr)
    finally:
        driver.quit()
        print("➡️  next proxy…")
