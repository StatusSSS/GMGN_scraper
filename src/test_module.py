#!/usr/bin/env python3
"""
Тестовый запуск без Redis: один запрос к gmgn.ai
с динамически сгенерированными заголовками и параметрами.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import os
import random
import time
import uuid
from typing import Final

from curl_cffi import requests as curl
from dotenv import load_dotenv

# ─────────── ENV ───────────
load_dotenv()
API_TIMEOUT: Final[int] = int(os.getenv("GMGN_TIMEOUT", "30"))
MAX_RETRIES: Final[int] = int(os.getenv("GMGN_RETRIES", "3"))

WALLETS: Final[list[str]] = [
    "2rmJhgCfqWsh8MqUFchUnsv43EDg55mTh9bkYMT4oPHk",
]

# ─────────── Справочники ───────────
BROWSERS = [
    ("Google Chrome",
     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"),
    ("Opera GX",
     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36 OPR/{major}.0.0.0"),
    ("Chromium",
     "Mozilla/5.0 (X11; Linux x86_64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"),
]
PLATFORMS = [
    ("Windows", "15.0.0", "x86", "64"),
    ("macOS",   "14.5.0", "x86", "64"),
    ("Linux",   "6.9",    "x86", "64"),
]
LANGS = ["ru", "en", "de", "es"]
TZS   = [("Europe/Moscow", 10800),
         ("Europe/Berlin",  7200),
         ("Asia/Tokyo",    32400)]

# ─────────── Вспомогательные ───────────
def _rand_major() -> int:
    return random.randint(133, 140)

def _uid() -> str:
    return uuid.uuid4().hex

def _build_stamp() -> str:
    today = dt.datetime.utcnow().strftime("%Y%m%d")
    return f"{today}-{random.randint(50, 1500)}-{_uid()[:7]}"

# ─────────── Генераторы ───────────
def generate_headers(addr: str) -> dict[str, str]:
    brand, ua_tpl = random.choice(BROWSERS)
    major         = _rand_major()
    ua            = ua_tpl.format(major=major)
    platform, platform_ver, arch, bits = random.choice(PLATFORMS)

    full_ver = f"{major}.0.{random.randint(4000, 8000)}.{random.randint(50, 200)}"
    full_list = (f"\"{brand}\";v=\"{full_ver}\", "
                 f"\"Chromium\";v=\"{full_ver}\", "
                 f"\"Not/A)Brand\";v=\"24.0.0.0\"")
    sec_ch_ua = (f"\"{brand}\";v=\"{major}\", "
                 f"\"Chromium\";v=\"{major}\", "
                 f"\"Not/A)Brand\";v=\"24\"")

    return {
        "Accept": "application/json, text/plain, */*",
        "accept-language": (
            f"ru-RU,ru;q=0.9,{random.choice(LANGS)}-US;q=0.8,en;q=0.7"
        ),
        "User-Agent": ua,

        # UA-CH
        "sec-ch-ua-full-version-list": full_list,
        "sec-ch-ua-full-version": f"\"{full_ver}\"",
        "sec-ch-ua": sec_ch_ua,
        "sec-ch-ua-platform": f"\"{platform}\"",
        "sec-ch-ua-platform-version": f"\"{platform_ver}\"",
        "sec-ch-ua-arch": f"\"{arch}\"",
        "sec-ch-ua-bitness": f"\"{bits}\"",
        "sec-ch-ua-model": "\"\"",
        "sec-ch-ua-mobile": "?0",

        # Sentry / baggage
        "baggage": (
            "sentry-environment=production,"
            f"sentry-release={_build_stamp()},"
            "sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,"
            f"sentry-trace_id={_uid()},"
            "sentry-sample_rate=0.005,sentry-sampled=false"
        ),
        "sentry-trace": f"{_uid()}-{_uid()[:16]}-0",

        # Referer
        "Referer": f"https://gmgn.ai/sol/address/{addr}",
    }

def generate_params() -> dict[str, str]:
    tz_name, tz_offset = random.choice(TZS)
    stamp = _build_stamp()
    return {
        "device_id": _uid(),
        "client_id": f"gmgn_web_{stamp}",
        "from_app":  "gmgn",
        "app_ver":   stamp,
        "tz_name":   tz_name,
        "tz_offset": str(tz_offset),
        "app_lang":  random.choice(LANGS),
        "fp_did":    hashlib.md5(_uid().encode()).hexdigest(),
        "os":        "web",

        # фильтры, как в оригинале
        "limit":      "50",
        "orderby":    "last_active_timestamp",
        "direction":  "desc",
        "showsmall":  "true",
        "sellout":    "true",
        "tx30d":      "true",
    }

# ─────────── HTTP ───────────
def fetch_wallet_stat(wallet: str,
                      hdrs: dict[str, str],
                      prms: dict[str, str]) -> dict | None:
    url = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/7d"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = curl.get(
                url=url,
                params=prms,
                headers=hdrs,
                impersonate="chrome120",
                timeout=API_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            # тут можно логировать тип ошибки
            print(f"[{wallet}] error, retry {attempt}")
            time.sleep(1.5)
    return None

# ─────────── Async wrapper ───────────
async def one_wallet(wallet: str) -> None:
    hdrs = generate_headers(wallet)
    prms = generate_params()
    loop = asyncio.get_running_loop()
    print(hdrs)
    print(prms)
    data = await loop.run_in_executor(None, fetch_wallet_stat, wallet, hdrs, prms)
    if data:
        print(f"{wallet[:6]}… ✔ pnl={data.get('pnl')}")
    else:
        print(f"{wallet[:6]}… ❌")

async def main(wallets: list[str]) -> None:
    await asyncio.gather(*(one_wallet(w) for w in wallets))

if __name__ == "__main__":
    asyncio.run(main(WALLETS))
