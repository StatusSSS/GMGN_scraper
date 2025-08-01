# test_cap.py
import json, os, time, redis, sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import sys
# ─── конфиг ─────────────────────────────────────────────────────────
REDIS_HOST  = os.getenv("REDIS_HOST", "redis")
REDIS_PORT  = int(os.getenv("REDIS_PORT", 6379))
SELENIUM_URL = os.getenv("SELENIUM_SERVER_URL", "http://localhost:4444/wd/hub")

API_URL        = "https://gmgn.ai/new-pair"
WAIT_FOR_CLICK = int(os.getenv("WAIT_FOR_CLICK", 600))   # время на клик, сек

rds = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)


sys.stdout.reconfigure(line_buffering=True)


def get_task() -> dict:
    """Берём след. proxy+UA (блокируется, пока очередь пуста)."""
    _key, raw = rds.blpop("cookie_tasks")
    return json.loads(raw)

def build_opts(proxy: str, ua: str) -> Options:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-agent={ua}")

    host, port, user, pwd = proxy.split(":", 3)
    opts.add_argument(f"--proxy-server=http://{user}:{pwd}@{host}:{port}")
    return opts

def save_cookies(proxy: str, ua: str, driver: webdriver.Remote) -> None:
    cookies = driver.get_cookies()
    key = f"cookies:{proxy}"
    rds.set(key, json.dumps({"ua": ua, "cookies": cookies}))
    print(f"[OK] cookies сохранены в Redis → {key}")

def wait_manual_click(timeout: int) -> None:
    print(f"⏳  Жду ваш клик reCAPTCHA (до {timeout} с)…")
    time.sleep(timeout)

# ─── главный цикл ───────────────────────────────────────────────────
while True:
    task = get_task()               # {"proxy": "ip:port:user:pwd", "ua": "..."}
    proxy, ua = task["proxy"], task["ua"]
    print("\n" + "=" * 60)
    print(f"Proxy: {proxy}\nUA: {ua}")

    driver = webdriver.Remote(command_executor=SELENIUM_URL,
                              options=build_opts(proxy, ua))

    try:
        driver.get(API_URL)
        wait_manual_click(WAIT_FOR_CLICK)
        save_cookies(proxy, ua, driver)

    except Exception as exc:
        print(f"[ERR] {type(exc).__name__}: {exc}", file=sys.stderr)

    finally:
        driver.quit()
        print("➡️  Переход к следующему proxy…")
