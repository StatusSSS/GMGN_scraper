# src/scraper/gmgn/holdings_scraper.py
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List, Sequence, Optional, Tuple
from urllib.parse import quote, urlparse

from loguru import logger
from curl_cffi import requests as curl
from curl_cffi.requests.errors import CurlError
from playwright.async_api import async_playwright
from sqlalchemy import select

from src.sdk.databases.postgres.dependency import AsyncSessionLocal
from src.sdk.databases.postgres.models import Wallet, WalletSnapshot

# ───────────────────────── constants / env ──────────────────────────

LOG_DIR = Path("logs"); LOG_DIR.mkdir(exist_ok=True)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
GMGN_CHAIN = "sol"
HOME_URL = f"https://gmgn.ai/?chain={GMGN_CHAIN}"
API_ENDPOINT_TMPL = "https://gmgn.ai/api/v1/wallet_holdings/{chain}/{address}"

# мягкая пауза между запросами API
SLEEP_BETWEEN_REQ = (
    float(os.getenv("REQ_SLEEP_MIN", "3.0")),
    float(os.getenv("REQ_SLEEP_MAX", "4.0")),
)

# управление логами успешных 2xx
SUCCESS_LOG_SAMPLE_RATE = float(os.getenv("SUCCESS_LOG_SAMPLE_RATE", "0.02"))
SUCCESS_LOG_SLOW_MS = float(os.getenv("SUCCESS_LOG_SLOW_MS", "1500"))

# cookie refresh
WAIT_CLEARANCE_SECONDS = int(os.getenv("WAIT_CLEARANCE_SECONDS", "75"))
COOKIE_REFRESH_TIMEOUT = int(os.getenv("COOKIE_REFRESH_TIMEOUT", "180"))
COOKIES_MAX_AGE_S = int(os.getenv("COOKIES_MAX_AGE_S", "5400"))
HEADLESS = os.getenv("HEADLESS", "0") not in ("0", "false", "False")

# ───────────────────────── logging setup ────────────────────────────

def setup_logger():
    logger.remove()
    logger.add(
        sys.stderr, level=LOG_LEVEL, enqueue=True, backtrace=False, colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | {level:<8} | {message} | {extra}",
    )

setup_logger()
logger = logger.bind(app="gmgn-holdings")

# ──────────────────────── UA / headers / params ─────────────────────

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
]

ACCEPT_LANGS = [
    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "ru,ru-RU;q=0.9,en;q=0.8",
    "ru-RU,ru;q=0.95,en;q=0.7",
    "ru,en-US;q=0.9,en;q=0.8",
    "ru-RU,ru;q=0.9,en;q=0.8,fi;q=0.6",
]

BASE_PARAMS = {
    "from_app": "gmgn",
    "os": "web",
    "period": "7d",
    "tz_name": "Europe/Moscow",
    "tz_offset": "10800",
    "app_lang": "ru",
}

def headers_for_worker(idx: int, ua: str) -> Dict[str, str]:
    lang = ACCEPT_LANGS[idx % len(ACCEPT_LANGS)]
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": lang,
        "user-agent": ua,
        "referer": HOME_URL,
        "origin": "https://gmgn.ai",
    }

def ensure_browser_like_headers(sess: curl.Session):
    sess.headers.setdefault("sec-fetch-site", "same-origin")
    sess.headers.setdefault("sec-fetch-mode", "cors")
    sess.headers.setdefault("sec-fetch-dest", "empty")
    sess.headers.setdefault("accept-encoding", "gzip, deflate, br, zstd")

def _choose_alt_ua_idx(current_idx: int) -> int:
    n = len(UA_LIST)
    if n <= 1:
        return current_idx
    j = random.randrange(n - 1)
    if j >= current_idx:
        j += 1
    return j

def _gen_ids(seed_bytes: bytes | None = None) -> tuple[str, str, str, str]:
    import secrets, uuid, hashlib as _hash, datetime as _dt
    if seed_bytes is None:
        seed_bytes = secrets.token_bytes(16)
    device_id = str(uuid.uuid4())
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y%m%d-%H%M")
    rand8 = secrets.token_hex(4)
    client_id = f"gmgn_web_{ts}-{rand8}"
    app_ver = client_id
    fp_material = device_id.encode() + seed_bytes
    fp_did = _hash.md5(fp_material).hexdigest()
    return device_id, client_id, app_ver, fp_did

def _tz_offset_seconds(tz_name: str, fallback_seconds: int = 0) -> int:
    try:
        from zoneinfo import ZoneInfo
        z = ZoneInfo(tz_name)
        now = datetime.now(z)
        offs = now.utcoffset()
        return int(offs.total_seconds()) if offs else fallback_seconds
    except Exception:
        return fallback_seconds

def make_worker_params(base: dict, worker_idx: int, ua: str,
                       tz_name: str = "Europe/Moscow", app_lang: str = "ru") -> dict:
    import hashlib, time as _t
    seed = hashlib.sha256(f"{worker_idx}-{ua}-{_t.time_ns()}".encode()).digest()
    device_id, client_id, app_ver, fp_did = _gen_ids(seed)
    try:
        fallback = int(str(base.get("tz_offset", "0")))
    except Exception:
        fallback = 0
    tz_off = _tz_offset_seconds(tz_name, fallback_seconds=fallback)
    params = base.copy()
    params.update({
        "device_id": device_id,
        "client_id": client_id,
        "app_ver": app_ver,
        "fp_did": fp_did,
        "from_app": "gmgn",
        "os": "web",
        "period": params.get("period", "7d"),
        "app_lang": app_lang,
        "tz_name": tz_name,
        "tz_offset": str(tz_off),
    })
    return params

# ───────────────────────── proxies ──────────────────────────────────

DEFAULT_PROXIES = [
    "168.81.207.52:9373:dTf2he:JUTqBm",
    "168.81.207.75:9410:dTf2he:JUTqBm",
    "168.81.206.188:9145:dTf2he:JUTqBm",
    "168.81.205.109:9378:dTf2he:JUTqBm",
    "168.81.204.100:9610:dTf2he:JUTqBm",
]
PROXY_STRS = [p.strip() for p in os.getenv("PROXY_LIST", ",".join(DEFAULT_PROXIES)).split(",") if p.strip()]

def mask_proxy(p: str) -> str:
    try:
        if "://" in p:
            scheme, rest = p.split("://", 1)
        else:
            scheme, rest = "http", p
        host, port, user, *_ = rest.split(":")
        return f"{scheme}://{host}:{port}:{user}:***"
    except Exception:
        return p

@dataclass
class ProxyCfg:
    server_url: str
    username: str
    password: str
    def for_playwright(self) -> dict:
        return {"server": self.server_url, "username": self.username, "password": self.password}
    def for_curl(self) -> dict:
        p = urlparse(self.server_url)
        proxy = f"{p.scheme}://{self.username}:{self.password}@{p.hostname}:{p.port}"
        return {"http": proxy, "https": proxy}
    @classmethod
    def parse(cls, s: str) -> "ProxyCfg":
        if "://" in s:
            scheme, rest = s.split("://", 1)
        else:
            scheme, rest = "http", s
        parts = rest.split(":")
        if len(parts) < 4:
            raise ValueError(f"Bad proxy string '{s}'. Expected host:port:user:pass")
        host, port, user = parts[0], parts[1], parts[2]
        pwd = ":".join(parts[3:])
        return cls(server_url=f"{scheme}://{host}:{port}", username=user, password=pwd)

# ───────────────────────── worker state ─────────────────────────────

@dataclass
class Stats:
    ok: int = 0
    forbidden: int = 0
    rate_limited: int = 0
    server_err: int = 0
    other_err: int = 0
    exceptions: int = 0
    bytes_rx: int = 0
    attempts: int = 0
    refreshes: int = 0
    ua_switches: int = 0

@dataclass
class Worker:
    name: str
    user_agent: str
    proxy: ProxyCfg
    ua_idx: int = 0
    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, str] = field(default_factory=dict)
    cookies_ts: float = 0.0
    stats: Stats = field(default_factory=Stats)

def rotate_identity(worker: Worker, sess: Optional[curl.Session] = None, reason: str = "") -> None:
    try:
        old_idx = worker.ua_idx if hasattr(worker, "ua_idx") else UA_LIST.index(worker.user_agent)
    except ValueError:
        old_idx = 0
    new_idx = _choose_alt_ua_idx(old_idx)
    new_ua = UA_LIST[new_idx]
    worker.ua_idx = new_idx
    worker.user_agent = new_ua
    worker.headers = headers_for_worker(new_idx, new_ua)
    worker.stats.ua_switches += 1
    logger.bind(worker=worker.name).warning(f"Смена UA/headers: idx {old_idx} → {new_idx} ({reason})")
    if sess is not None:
        sess.headers.update(worker.headers)

def build_workers(n: int) -> List[Worker]:
    proxies = [ProxyCfg.parse(s) for s in PROXY_STRS]
    workers: List[Worker] = []
    for i in range(n):
        proxy = proxies[i % len(proxies)]
        ua_idx = i % len(UA_LIST)
        ua = UA_LIST[ua_idx]
        w = Worker(name=f"W{i+1}", user_agent=ua, proxy=proxy, ua_idx=ua_idx)
        w.headers = headers_for_worker(ua_idx, ua)
        w.params = make_worker_params(BASE_PARAMS, ua_idx, ua, tz_name="Europe/Moscow", app_lang="ru")
        workers.append(w)
    return workers

def split_evenly(items: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        return [items]
    k, r = divmod(len(items), n)
    res, start = [], 0
    for i in range(n):
        end = start + k + (1 if i < r else 0)
        res.append(items[start:end])
        start = end
    return res

# ───────────────────── cookie refresh via Playwright ─────────────────

async def wait_for_cf_clearance(context, timeout_s: int) -> bool:
    deadline = time.monotonic() + max(0, timeout_s)
    while time.monotonic() < deadline:
        cookies = await context.cookies("https://gmgn.ai")
        if any(c.get("name") == "cf_clearance" and c.get("value") for c in cookies):
            return True
        await asyncio.sleep(1.5)
    return False

async def fetch_cookies_for_worker(worker: Worker) -> Dict[str, str]:
    log = logger.bind(worker=worker.name, proxy=mask_proxy(worker.proxy.server_url))
    log.info("Открываю браузер для съёма cookies")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            proxy=worker.proxy.for_playwright(),
            args=["--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check"],
        )
        try:
            context = await browser.new_context(
                locale="ru-RU",
                timezone_id=worker.params.get("tz_name", "Europe/Moscow"),
                user_agent=worker.user_agent,
                viewport={"width": 1366, "height": 850},
            )
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
            page = await context.new_page()
            try:
                await page.goto(HOME_URL, wait_until="networkidle", timeout=45000)
            except Exception as e:
                log.warning(f"Первый goto() дал исключение: {e!r}")

            got = await wait_for_cf_clearance(context, 10)
            if not got:
                try:
                    await page.reload(wait_until="networkidle")
                except Exception as e:
                    log.warning(f"reload() исключение: {e!r}")
                _ = await wait_for_cf_clearance(context, max(0, WAIT_CLEARANCE_SECONDS - 10))

            cookies_list = await context.cookies("https://gmgn.ai")
            cookies_dict = {c["name"]: c["value"] for c in cookies_list}
            if "cf_clearance" in cookies_dict and cookies_dict["cf_clearance"]:
                log.success("cf_clearance получен")
            else:
                log.warning("cf_clearance НЕ получен — работаем с тем, что есть")
            return cookies_dict
        finally:
            try:
                await browser.close()
            except Exception:
                pass

# ───────────────────── HTTP helpers / retries ───────────────────────

def maybe_log_success(log, sc: int, dt_ms: float, resp_len: int):
    if SUCCESS_LOG_SLOW_MS and dt_ms >= SUCCESS_LOG_SLOW_MS:
        log.bind(slow=True).info(f"{sc} OK in {dt_ms:.0f} ms, bytes={resp_len}")
    elif SUCCESS_LOG_SAMPLE_RATE and random.random() < SUCCESS_LOG_SAMPLE_RATE:
        log.bind(sample=True).info(f"{sc} OK in {dt_ms:.0f} ms, bytes={resp_len}")

def get_with_retry(sess, url, params, log, cookies, max_attempts=3, sleep_base=2.0):
    last_resp = None
    for attempt in range(1, max_attempts + 1):
        t0 = time.perf_counter()
        try:
            resp = sess.get(url, params=params, cookies=cookies)
            dt = (time.perf_counter() - t0) * 1000.0
            sc = resp.status_code
            log_attempt = log.bind(attempt=attempt)
            if 200 <= sc < 300:
                maybe_log_success(log_attempt, sc, dt, len(resp.content))
                return resp
            elif sc == 403:
                log_attempt.warning(f"{sc} FORBIDDEN in {dt:.0f} ms")
            elif sc == 429:
                log_attempt.warning(f"{sc} RATE_LIMIT in {dt:.0f} ms")
            elif 500 <= sc < 600:
                log_attempt.error(f"{sc} SERVER_ERROR in {dt:.0f} ms")
            else:
                log_attempt.warning(f"{sc} OTHER in {dt:.0f} ms")
            last_resp = resp
        except Exception as e:
            dt = (time.perf_counter() - t0) * 1000.0
            log_attempt = log.bind(attempt=attempt)
            log_attempt.exception(f"EXC in {dt:.0f} ms: {e!r}")
        if attempt < max_attempts:
            sleep_extra = 0.0
            if last_resp is not None and getattr(last_resp, "status_code", None) == 429:
                ra = last_resp.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_extra = float(ra)
                    except Exception:
                        pass
            time.sleep(sleep_base * attempt + sleep_extra)
    return last_resp

# ───────────────────── holdings client (stateful) ───────────────────

class HoldingsClient:
    def __init__(self, worker: Worker):
        self.worker = worker
        self.sess = curl.Session(impersonate="chrome", proxies=worker.proxy.for_curl(), timeout=30)
        self.sess.headers.update(worker.headers)
        ensure_browser_like_headers(self.sess)

    async def ensure_cookies(self, reason: str) -> None:
        self.worker.stats.refreshes += 1
        new_cookies = await fetch_cookies_for_worker(self.worker)
        if new_cookies:
            self.worker.cookies = new_cookies
            self.worker.cookies_ts = time.time()
            await asyncio.sleep(random.uniform(1.0, 2.0))

    async def fetch_holdings(self, address: str, *, max_retry: int = 5) -> List[Dict[str, Any]]:
        log = logger.bind(worker=self.worker.name, wallet=address, proxy=mask_proxy(self.worker.proxy.server_url))
        url = API_ENDPOINT_TMPL.format(chain=quote(GMGN_CHAIN), address=quote(address))

        # проактивное обновление устаревших cookies
        now = time.time()
        if self.worker.cookies and self.worker.cookies_ts and (now - self.worker.cookies_ts > COOKIES_MAX_AGE_S):
            log.info("Cookies устарели → обновляю заранее (proactive)")
            await self.ensure_cookies("proactive")

        attempt = 0
        while True:
            attempt += 1
            # ВАЖНО: синхронный HTTP с ретраями — уводим в thread, чтобы не блокировать loop
            resp = await asyncio.to_thread(
                get_with_retry, self.sess, url, self.worker.params, log, self.worker.cookies, 3
            )
            self.worker.stats.attempts += 1

            if resp is None:
                self.worker.stats.exceptions += 1
                log.error("Нет ответа после всех попыток")
            else:
                sc = resp.status_code
                if 200 <= sc < 300:
                    self.worker.stats.ok += 1
                    self.worker.stats.bytes_rx += len(resp.content)
                    try:
                        return resp.json()["data"]["holdings"]
                    except Exception as exc:
                        raise RuntimeError(f"bad payload: {exc}") from exc
                elif sc == 403:
                    self.worker.stats.forbidden += 1
                    rotate_identity(self.worker, self.sess, reason=f"403 on {address}")
                    await self.ensure_cookies("403")
                elif sc == 429:
                    self.worker.stats.rate_limited += 1
                elif 500 <= sc < 600:
                    self.worker.stats.server_err += 1
                else:
                    self.worker.stats.other_err += 1

            if attempt >= max_retry:
                raise RuntimeError(f"all retries failed ({max_retry})")
            await asyncio.sleep(2)  # не блокируем event loop

    def close(self):
        try:
            self.sess.close()
        except Exception:
            pass

# ─────────────────────────── utils (unchanged) ──────────────────────

def humanize(sec: int | float | None) -> str | None:
    if sec is None:
        return None
    td = timedelta(seconds=sec)
    d, rem = divmod(int(td.total_seconds()), 86_400)
    h, rem = divmod(rem, 3_600)
    m, s = divmod(rem, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h or (d and (m or s)):
        parts.append(f"{h}h")
    if m or (d or h) and s:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)

def calc_basic(data: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    pnl = [float(it["total_profit_pnl"]) for it in data]
    wins = sum(p >= 0 for p in pnl)
    losses = len(pnl) - wins
    winrate = wins / len(pnl) * 100 if pnl else 0

    durations = [
        (it["end_holding_at"] - it["start_holding_at"])
        for it in data
        if it.get("start_holding_at") and it.get("end_holding_at")
    ]
    avg_hold = mean(durations) if durations else None

    starts = sorted(int(it["start_holding_at"]) for it in data if it.get("start_holding_at"))
    intervals = [b - a for a, b in zip(starts, starts[1:])]
    avg_gap = mean(intervals) if intervals else None

    return {
        "total_trades": len(pnl),
        "winrate_pct": round(winrate, 2),
        "wins": wins,
        "losses": losses,
        "avg_hold_human": humanize(avg_hold),
        "avg_interval_human": humanize(avg_gap),
    }

def calc_quality(data: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    pnl = [float(it["total_profit_pnl"]) for it in data]
    gross_p = sum(p for p in pnl if p > 0)
    gross_l = -sum(p for p in pnl if p < 0)
    profit_factor = gross_p / gross_l if gross_l else None
    expectancy = mean(pnl) if pnl else 0
    total_pnl = sum(pnl)

    wins = [p for p in pnl if p > 0]
    losses = [p for p in pnl if p < 0]
    rr = abs(mean(wins) / mean(losses)) if wins and losses else None

    liqs = [float(it["liquidity"]) for it in data if it.get("liquidity")]
    med_liq = median(liqs) if liqs else None

    usd_vals = [float(it["usd_value"]) for it in data if float(it["usd_value"]) > 0]
    hhi = None
    if usd_vals:
        total = sum(usd_vals)
        hhi = sum((v / total) ** 2 for v in usd_vals) if total else None

    honeypot_share = (
        sum(bool(it.get("token", {}).get("is_honeypot")) for it in data) / len(data) * 100 if data else 0
    )

    net_pnl_30d = sum(
        float(it.get("realized_profit_30d", 0)) + float(it.get("unrealized_profit", 0))
        for it in data
    )

    turnover = sum(
        float(it.get("history_bought_cost", 0)) + float(it.get("history_sold_income", 0))
        for it in data
    )
    pnl_per_turn = net_pnl_30d / turnover if turnover else None

    last_ts = max((int(it["last_active_timestamp"]) for it in data if it.get("last_active_timestamp")), default=None)
    days_idle = (time.time() - last_ts) / 86_400 if last_ts else None

    return {
        "profit_factor": round(profit_factor, 8) if profit_factor else 0,
        "expectancy": round(expectancy, 8),
        "risk_reward": round(rr, 8) if rr else 0,
        "median_liquidity_usd": round(med_liq, 2) if med_liq else 0,
        "hhi": round(hhi, 6) if hhi else 0,
        "honeypot_share_pct": round(honeypot_share, 2),
        "net_pnl_30d_usd": round(net_pnl_30d, 2),
        "turnover_usd": round(turnover, 2),
        "pnl_per_turnover": round(pnl_per_turn, 6) if pnl_per_turn else 0,
        "days_idle": round(days_idle, 2) if days_idle else 0,
        "pnl": round(total_pnl, 2),
    }

def build_snapshot(row: Dict[str, Any]) -> WalletSnapshot:
    return WalletSnapshot(
        address              = row["address"],
        ts_utc               = datetime.fromisoformat(row["timestamp_utc"]),
        profit_factor        = row["profit_factor"],
        expectancy           = row["expectancy"],
        risk_reward          = row["risk_reward"],
        winrate_pct          = row["winrate_pct"],
        net_pnl_30d_usd      = row["net_pnl_30d_usd"],
        median_liquidity_usd = row["median_liquidity_usd"],
        hhi                  = row["hhi"],
        honeypot_share_pct   = row["honeypot_share_pct"],
        days_idle            = row["days_idle"],
        total_trades         = row.get("total_trades", 0),
        avg_hold_human       = row.get("avg_hold_human") or "",
        avg_interval_human   = row.get("avg_interval_human") or "",
        turnover_usd         = row["turnover_usd"],
        pnl_per_turnover     = row["pnl_per_turnover"],
        pnl                  = row["pnl"],
    )

async def load_wallet_addresses(limit: int | None = None) -> List[str]:
    async with AsyncSessionLocal() as session:
        result = await session.scalars(select(Wallet.address).order_by(Wallet.id))
        addresses = result.all()
        if limit is not None:
            addresses = addresses[:limit]
        return addresses

# ───────────────────────── app logic (unchanged API) ────────────────

async def analyse_wallet(addr: str, client: HoldingsClient) -> Dict[str, Any]:
    data = await client.fetch_holdings(addr)
    basic = calc_basic(data)
    qual = calc_quality(data)
    return {
        "address": addr,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **basic,
        **qual,
    }

# ───────────────────────── workers processing ───────────────────────

async def process_chunk(worker: Worker, addresses: List[str], fixed_delay: Optional[float]) -> Tuple[str, Stats, int]:
    client = HoldingsClient(worker)
    processed = 0
    try:
        async with AsyncSessionLocal() as session:
            for addr in addresses:
                try:
                    full: Dict[str, Any] = await analyse_wallet(addr, client)

                    snapshot = build_snapshot(full)
                    session.add(snapshot)
                    await session.commit()
                    processed += 1
                except Exception as exc:
                    await session.rollback()
                    logger.bind(worker=worker.name, wallet=addr).exception(f"⚠️  {addr}: {exc!r}")
                # пауза
                if fixed_delay and fixed_delay > 0:
                    await asyncio.sleep(fixed_delay)
                else:
                    await asyncio.sleep(random.uniform(*SLEEP_BETWEEN_REQ))
    finally:
        client.close()
    # итог по воркеру
    st = worker.stats
    logger.info(
        f"[{worker.name}] done: processed={processed} ok={st.ok} 403={st.forbidden} 429={st.rate_limited} "
        f"5xx={st.server_err} other4xx={st.other_err} exc={st.exceptions} "
        f"ua_switches={st.ua_switches} refreshes={st.refreshes} bytes={st.bytes_rx}"
    )
    return worker.name, worker.stats, processed

# ─────────────────────────── main loop ──────────────────────────────

async def main_async(limit: int | None, delay: float, workers_num: int) -> None:
    try:
        wallets = await load_wallet_addresses(limit)
    except Exception as exc:
        sys.exit(f"Не удалось загрузить адреса из БД: {exc}")

    if not wallets:
        sys.exit("Список кошельков пуст — нечего анализировать")

    # количество воркеров
    NUM_WORKERS = max(1, min(workers_num, len(wallets)))
    workers = build_workers(NUM_WORKERS)
    parts = split_evenly(wallets, NUM_WORKERS)

    for i, w in enumerate(workers):
        logger.info(f"[{w.name}] назначено кошельков: {len(parts[i])} | proxy={mask_proxy(w.proxy.server_url)} | ua_idx={w.ua_idx}")

    # последовательный съём cookies для каждого воркера
    logger.info(f"Снимаю cookies последовательно для {NUM_WORKERS} воркеров...")
    for w in workers:
        try:
            c = await asyncio.wait_for(fetch_cookies_for_worker(w), timeout=COOKIE_REFRESH_TIMEOUT)
        except asyncio.TimeoutError:
            c = {}
            logger.bind(worker=w.name).warning(f"Таймаут получения cookies за {COOKIE_REFRESH_TIMEOUT}s")
        w.cookies = c or {}
        w.cookies_ts = time.time() if w.cookies else 0.0
        if not w.cookies:
            logger.bind(worker=w.name).warning("Пустые cookies — возможны 403")
        await asyncio.sleep(0.5)

    # параллельная обработка чанков
    tasks = [
        asyncio.create_task(process_chunk(w, parts[i], delay))
        for i, w in enumerate(workers)
        if parts[i]  # пропускаем пустые
    ]

    results: List[Tuple[str, Stats, int]] = []
    for coro in asyncio.as_completed(tasks):
        try:
            results.append(await coro)
        except Exception as e:
            logger.exception(f"Исключение в таске воркера: {e!r}")

    # свод
    total = Stats()
    processed_total = 0
    for _, st, proc in results:
        total.ok += st.ok
        total.forbidden += st.forbidden
        total.rate_limited += st.rate_limited
        total.server_err += st.server_err
        total.other_err += st.other_err
        total.exceptions += st.exceptions
        total.bytes_rx += st.bytes_rx
        total.attempts += st.attempts
        total.refreshes += st.refreshes
        total.ua_switches += st.ua_switches
        processed_total += proc

    logger.success(
        "Свод: "
        f"processed={processed_total}, ok={total.ok}, 403={total.forbidden}, 429={total.rate_limited}, "
        f"5xx={total.server_err}, other4xx={total.other_err}, exc={total.exceptions}, "
        f"refreshes={total.refreshes}, ua_switches={total.ua_switches}, bytes={total.bytes_rx}, attempts={total.attempts}"
    )

if __name__ == "__main__":
    prs = argparse.ArgumentParser("GMGN holdings scraper → WalletSnapshot")
    prs.add_argument("--limit", "-l", type=int, help="Максимум адресов для обработки")
    prs.add_argument("--delay", "-d", type=float, default=0.0, help="Пауза между кошельками (сек); если 0 — используется REQ_SLEEP_MIN..MAX")
    prs.add_argument("--workers", "-w", type=int, default=int(os.getenv("GMGN_WORKERS", "5")), help="Количество параллельных воркеров")
    args = prs.parse_args()
    try:
        asyncio.run(main_async(args.limit, args.delay, args.workers))
    except KeyboardInterrupt:
        pass
