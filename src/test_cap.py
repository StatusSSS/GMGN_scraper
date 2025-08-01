#!/usr/bin/env python
import base64, json, os, signal, subprocess, sys, time, redis
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
SELENIUM_URL = os.getenv("SELENIUM_SERVER_URL", "http://selenium_testcap:4444/wd/hub")

API_URL        = "https://gmgn.ai/new-pair"
WAIT_FOR_CLICK = int(os.getenv("WAIT_FOR_CLICK", 600))

TINY_CONF = Path("/etc/tinyproxy/tinyproxy.conf")
TINY_PID  = Path("/var/run/tinyproxy.pid")  # где tinyproxy пишет pid

rds = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)
sys.stdout.reconfigure(line_buffering=True)

# ─── helpers ─────────────────────────────────────────────────────────
def get_task() -> dict:
    _key, raw = rds.blpop("cookie_tasks")
    return json.loads(raw)           # {"proxy": "ip:port:user:pwd", "ua": …}

def reload_tinyproxy(proxy: str):
    host, port, user, pwd = proxy.split(":", 3)
    upstream_line = f"Upstream http {user}:{pwd}@{host} {port}\n"

    # переписываем конфиг (первая часть уже содержит Port/Listen/MaxClients)
    cfg = TINY_CONF.read_text().splitlines()
    cfg = [ln for ln in cfg if not ln.startswith("Upstream ")]
    cfg.append(upstream_line)
    TINY_CONF.write_text("\n".join(cfg) + "\n")

    if TINY_PID.exists():
        pid = int(TINY_PID.read_text().strip())
        os.kill(pid, signal.SIGHUP)          # мягкий reload
        print(f"[tinyproxy] reloaded for {host}:{port}")
    else:
        # fallback – старта не было? (не должно случиться)
        subprocess.Popen(["tinyproxy", "-c", str(TINY_CONF)])

def build_opts(ua: str) -> Options:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_argument("--proxy-server=http://localhost:8888")
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

    reload_tinyproxy(proxy)              # <= сменили Upstream

    driver = webdriver.Remote(SELENIUM_URL, options=build_opts(ua))
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
