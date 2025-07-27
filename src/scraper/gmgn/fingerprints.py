# src/scraper/gmgn/fingerprints.py
"""
Генерация и кеширование «личностей» (статичных browser-fingerprints)
для скрейпера gmgn.ai.
"""

from __future__ import annotations
import random, uuid, hashlib, time, datetime as dt
from collections import deque
from typing import Dict, Tuple, List

# ────────────────── источники реальных значений ────────────────────
_BROWSERS: List[tuple[str, str]] = [
    # (brand, UA-template)
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

_TZS = [
    ("Europe/Moscow",  10800),
    ("Europe/Berlin",   7200),
    ("Asia/Tokyo",    32400),
]

# ────────────────── базовые генераторы ──────────────────────────────
def _uid() -> str:
    """32-символьный hex UUID4."""
    return uuid.uuid4().hex


def _build_stamp() -> str:
    """Версия вида 20250721-123-abcdef0 (дата-рандом-соль)."""
    today = dt.datetime.utcnow().strftime("%Y%m%d")
    return f"{today}-{random.randint(50,1500)}-{_uid()[:7]}"


def _build_baggage() -> str:
    """Sentry baggage (новый каждый запрос)."""
    return (
        "sentry-environment=production,"
        f"sentry-release={_build_stamp()},"
        "sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,"
        f"sentry-trace_id={_uid()},"
        "sentry-sample_rate=0.005,sentry-sampled=false"
    )


def _build_trace() -> str:
    """sentry-trace (traceId-spanId-sampled)."""
    return f"{_uid()}-{_uid()[:16]}-0"

# ────────────────── статичные части fingerprint ─────────────────────
def _build_static_headers(rnd: random.Random) -> Dict[str, str]:
    brand, ua_tpl = rnd.choice(_BROWSERS)
    major        = rnd.randint(133, 140)
    ua           = ua_tpl.format(major=major)

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

        # UA-Client-Hints (фиксируем!)
        "sec-ch-ua-full-version-list": full_list,
        "sec-ch-ua-full-version":      f'"{full_ver}"',
        "sec-ch-ua":                   sec_ch,
        "sec-ch-ua-platform":          f'"{platform}"',
        "sec-ch-ua-platform-version":  f'"{platform_ver}"',
        "sec-ch-ua-arch":              f'"{arch}"',
        "sec-ch-ua-bitness":           f'"{bits}"',
        "sec-ch-ua-model":             '""',
        "sec-ch-ua-mobile":            "?0",

        # volatile поля (поставим плейсхолдеры, заменим при .fresh())
        "baggage":      "",
        "sentry-trace": "",

        # Referer добавляется в запросе (зависит от wallet)
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

        # неизменные фильтры
        "limit": "50",
        "orderby": "last_active_timestamp",
        "direction": "desc",
        "showsmall": "true",
        "sellout":   "true",
        "tx30d":     "true",
    }

# ────────────────── объект Identity ──────────────────────────────────
class Identity:
    """
    Статичный fingerprint (UA + UA-Client-Hints + params).
    .fresh() добавляет только «живые» поля (baggage, trace, fp_did).
    """
    def __init__(self, rnd: random.Random | None = None):
        if rnd is None:
            rnd = random.Random(uuid.uuid4().int)
        self.static_headers = _build_static_headers(rnd)
        self.static_params  = _build_static_params(rnd)

    def fresh(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        h = self.static_headers.copy()
        p = self.static_params.copy()

        # volatile — обновляем каждое обращение
        h["baggage"]      = _build_baggage()
        h["sentry-trace"] = _build_trace()
        p["fp_did"]       = hashlib.md5(_uid().encode()).hexdigest()

        return h, p

# ────────────────── кеш «личностей» по IP/прокси ─────────────────────
IDENTITIES_PER_PROXY = 4
PROXY_IDENTITIES: Dict[str, deque[Identity]] = {}
LAST_SWITCH: Dict[str, float] = {}

ROTATE_EVERY_N   = random.randint(50, 80)
ROTATE_EVERY_SEC = 30 * 60           # секунд (30 минут)

def init_proxies(proxy_list: List[str] | None = None) -> None:
    """
    Подготовить deque из Identity для каждого IP/прокси.
    Если список пустой → создаётся один ключ 'local'.
    """
    if not proxy_list:
        proxy_list = ["local"]

    global PROXY_IDENTITIES, LAST_SWITCH
    PROXY_IDENTITIES = {
        p: deque(
            Identity(random.Random(f"{p}-{i}"))
            for i in range(IDENTITIES_PER_PROXY)
        )
        for p in proxy_list
    }
    LAST_SWITCH = {p: 0.0 for p in proxy_list}


def pick_headers_params(proxy: str, counter: int) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Вернуть (headers, params) для текущей «личности» данного proxy-IP.
    Ротация раз в ROTATE_EVERY_N запросов **или** каждые ROTATE_EVERY_SEC секунд.
    """
    dq = PROXY_IDENTITIES[proxy]
    if (
        counter % ROTATE_EVERY_N == 0
        or time.time() - LAST_SWITCH[proxy] > ROTATE_EVERY_SEC
    ):
        dq.rotate()
        LAST_SWITCH[proxy] = time.time()

    return dq[0].fresh()
