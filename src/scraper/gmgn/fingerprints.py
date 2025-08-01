# src/scraper/gmgn/fingerprints.py
"""
Генерирует и кеш-ирует статичные browser-fingerprints
для gmgn.ai. На каждый proxy-IP создаём ровно одну
«личность» (index 0), которую используем всегда.
"""

from __future__ import annotations
import random, uuid, hashlib, datetime as dt
from typing import Dict, Tuple, List

# ────────────────── источники реальных значений ────────────────────
_BROWSERS: List[tuple[str, str]] = [
    (
        "Google Chrome",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36",
    ),
    (
        "Opera GX",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36 OPR/{major}.0.0.0",
    ),
    (
        "Chromium",
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36",
    ),
]

_PLATFORMS = [
    ("Windows", "15.0.0", "x86", "64"),
    ("macOS",   "14.5.0", "x86", "64"),
    ("Linux",   "6.9",    "x86", "64"),
]

_LANGS = ["ru", "en", "de", "es"]
_TZS   = [("Europe/Moscow", 10800),
          ("Europe/Berlin",  7200),
          ("Asia/Tokyo",   32400)]

# ────────────────── helpers ─────────────────────────────────────────
_uid  = lambda: uuid.uuid4().hex

def _build_stamp() -> str:
    today = dt.datetime.utcnow().strftime("%Y%m%d")
    return f"{today}-{random.randint(50,1500)}-{_uid()[:7]}"

def _build_baggage() -> str:
    return (
        "sentry-environment=production,"
        f"sentry-release={_build_stamp()},"
        "sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,"
        f"sentry-trace_id={_uid()},"
        "sentry-sample_rate=0.005,sentry-sampled=false"
    )

def _build_trace() -> str:
    return f"{_uid()}-{_uid()[:16]}-0"

# ────────────────── конструкторы fingerprint ───────────────────────
def _build_static_headers(rnd: random.Random) -> Dict[str, str]:
    brand, ua_tpl = rnd.choice(_BROWSERS)
    major = rnd.randint(133, 140)
    ua    = ua_tpl.format(major=major)

    platform, platform_ver, arch, bits = rnd.choice(_PLATFORMS)
    full_ver = f"{major}.0.{rnd.randint(4000,8000)}.{rnd.randint(50,200)}"

    full_list = ", ".join([
        f'"{brand}";v="{full_ver}"',
        f'"Chromium";v="{full_ver}"',
        '"Not/A)Brand";v="24.0.0.0"',
    ])
    sec_ch = ", ".join([
        f'"{brand}";v="{major}"',
        f'"Chromium";v="{major}"',
        '"Not/A)Brand";v="24"',
    ])

    return {
        "Accept": "application/json, text/plain, */*",
        "accept-language": f"ru-RU,ru;q=0.9,{rnd.choice(_LANGS)}-US;q=0.8,en;q=0.7",
        "User-Agent": ua,

        # UA-Client-Hints (фиксированные)
        "sec-ch-ua-full-version-list": full_list,
        "sec-ch-ua-full-version":      f'"{full_ver}"',
        "sec-ch-ua":                   sec_ch,
        "sec-ch-ua-platform":          f'"{platform}"',
        "sec-ch-ua-platform-version":  f'"{platform_ver}"',
        "sec-ch-ua-arch":              f'"{arch}"',
        "sec-ch-ua-bitness":           f'"{bits}"',
        "sec-ch-ua-model":             '""',
        "sec-ch-ua-mobile":            "?0",

        # будут обновлены в .fresh()
        "baggage": "", "sentry-trace": "",
    }

def _build_static_params(rnd: random.Random) -> Dict[str, str]:
    tz_name, tz_offset = rnd.choice(_TZS)
    stamp = _build_stamp()
    return {
        "device_id": _uid(),
        "client_id": f"gmgn_web_{stamp}",
        "from_app":  "gmgn",
        "app_ver":   stamp,
        "tz_name":   tz_name,
        "tz_offset": str(tz_offset),
        "app_lang":  rnd.choice(_LANGS),
        "fp_did":    hashlib.md5(_uid().encode()).hexdigest(),
        "os":        "web",

        "limit": "50",
        "orderby": "last_active_timestamp",
        "direction": "desc",
        "showsmall": "true",
        "sellout":   "true",
        "tx30d":     "true",
    }

# ────────────────── объект Identity ────────────────────────────────
class Identity:
    """Статичный fingerprint + метод fresh() для volatile-полей."""
    def __init__(self, rnd: random.Random | None = None):
        rnd = rnd or random.Random(uuid.uuid4().int)
        self.static_headers = _build_static_headers(rnd)
        self.static_params  = _build_static_params(rnd)

    def fresh(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        h, p = self.static_headers.copy(), self.static_params.copy()
        h["baggage"]      = _build_baggage()
        h["sentry-trace"] = _build_trace()
        p["fp_did"]       = hashlib.md5(_uid().encode()).hexdigest()
        return h, p

# ────────────────── кеш «личностей» по proxy-IP ────────────────────
PROXY_IDENTITIES: Dict[str, Identity] = {}

def init_proxies(proxy_list: List[str] | None = None) -> None:
    """
    На каждый proxy-IP создаём одну Identity с детерминированным сидом
    f\"{proxy}-0\" — чтобы UA совпадал с push_cookie_tasks / test_cap.
    """
    if not proxy_list:
        proxy_list = ["local"]

    global PROXY_IDENTITIES
    PROXY_IDENTITIES = {
        p: Identity(random.Random(f"{p}-0")) for p in proxy_list
    }

def pick_headers_params(proxy: str, _: int = 0) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Возвращает (headers, params) «личности» этого proxy — без ротации."""
    return PROXY_IDENTITIES[proxy].fresh()
