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
BASE_CONF = "Port 8888\nListen 0.0.0.0\nMaxClients 50\n"
rds = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)
sys.stdout.reconfigure(line_buffering=True)

# ─── helpers ─────────────────────────────────────────────────────────
def get_task() -> dict:
    _key, raw = rds.blpop("cookie_tasks")
    return json.loads(raw)           # {"proxy": "ip:port:user:pwd", "ua": …}

def reload_tinyproxy(proxy: str):
    host, port, user, pwd = proxy.split(":", 3)

    # правильный синтаксис: host:port в одном токене
    upstream = f"Upstream http {user}:{pwd}@{host}:{port}"

    # ── остальное без изменений ─────────────────────────────
    if not TINY_CONF.exists():
        TINY_CONF.parent.mkdir(parents=True, exist_ok=True)
        TINY_CONF.write_text(BASE_CONF)

    lines = [ln for ln in TINY_CONF.read_text().splitlines()
             if not ln.startswith("Upstream ")]
    lines.append(upstream)
    TINY_CONF.write_text("\n".join(lines) + "\n")
    print("[tinyproxy] new config ↓\n" + TINY_CONF.read_text())

    if TINY_PID.exists():
        os.kill(int(TINY_PID.read_text().strip()), signal.SIGHUP)
    else:
        subprocess.Popen(["tinyproxy", "-d", "-c", str(TINY_CONF)])
    print(f"[tinyproxy] reloaded for {host}:{port}")

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
