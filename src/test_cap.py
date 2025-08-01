#!/usr/bin/env python
# test_cap.py
import base64, json, os, sys, time, redis
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver

REDIS_HOST  = os.getenv("REDIS_HOST", "redis")
REDIS_PORT  = int(os.getenv("REDIS_PORT", 6379))
SELENIUM_URL = os.getenv("SELENIUM_SERVER_URL", "http://localhost:4444/wd/hub")

API_URL        = "https://gmgn.ai/new-pair"
WAIT_FOR_CLICK = int(os.getenv("WAIT_FOR_CLICK", 600))

rds = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)
sys.stdout.reconfigure(line_buffering=True)

def get_task() -> dict:
    _key, raw = rds.blpop("cookie_tasks")
    return json.loads(raw)

def build_opts(proxy: str, ua: str) -> tuple[Options, tuple[str, str]]:
    host, port, user, pwd = proxy.split(":", 3)

    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_argument(f"--proxy-server=http://{host}:{port}")

    token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    return opts, ("Proxy-Authorization", f"Basic {token}")

def inject_header(driver: WebDriver, header: tuple[str, str]) -> None:
    name, value = header
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders",
                           {"headers": {name: value}})

def save_cookies(proxy: str, ua: str, driver: WebDriver) -> None:
    rds.set(f"cookies:{proxy}",
            json.dumps({"ua": ua, "cookies": driver.get_cookies()}))
    print(f"[OK] cookies сохранены в Redis → cookies:{proxy}")

def wait_manual_click(timeout: int):
    print(f"⏳  Жду ваш клик reCAPTCHA (до {timeout} с)…")
    time.sleep(timeout)

# ─── главный цикл ───────────────────────────────────────
while True:
    task = get_task()
    proxy, ua = task["proxy"], task["ua"]
    print("\n" + "=" * 60)
    print(f"Proxy: {proxy}\nUA: {ua}")

    opts, auth_header = build_opts(proxy, ua)
    driver = webdriver.Remote(command_executor=SELENIUM_URL, options=opts)

    try:
        inject_header(driver, auth_header)
        print("[DEBUG] Proxy-Authorization header set")      # ← отладочный вывод
        driver.get(API_URL)

        wait_manual_click(WAIT_FOR_CLICK)
        save_cookies(proxy, ua, driver)

    except Exception as exc:
        print(f"[ERR] {type(exc).__name__}: {exc}", file=sys.stderr)

    finally:
        driver.quit()
        print("➡️  Переход к следующему proxy…")
