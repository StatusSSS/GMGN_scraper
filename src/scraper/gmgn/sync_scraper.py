# gmgn_multi_workers.py  (обновлён под Redis + DB)
import os, sys
import asyncio, time, random, secrets, uuid, hashlib, json
import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote, urlparse
from concurrent.futures import ThreadPoolExecutor

from loguru import logger
from playwright.async_api import async_playwright
from curl_cffi import requests as curl

# ── добавлено для БД/Redis (как в sync_scraper) ──────────────────────
from src.sdk.databases.postgres.dependency import with_db_session
from src.sdk.databases.postgres.models import Wallet
from src.sdk.queues.redis_connect import get_redis

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ─── логирование ─────────────────────────────────────────────────────
LOG_DIR = Path("logs"); LOG_DIR.mkdir(exist_ok=True)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

def setup_logger():
    logger.remove()
    # Только stderr — файл не пишем
    logger.add(
        sys.stderr,
        level=LOG_LEVEL,
        enqueue=True,
        backtrace=False,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | {level:<8} | {message} | {extra}",
    )
    # УБРАНО: file sink, чтобы не писать логи в файл

setup_logger()
logger = logger.bind(app="gmgn")

# ─── настройки ───────────────────────────────────────────────────────
CHAIN = "sol"
HOME_URL = f"https://gmgn.ai/?chain={CHAIN}"

# берём батчи из Redis
QUEUE_NAME = os.getenv("REDIS_QUEUE", "wallet_queue")

# порог PnL для записи в БД
PNL_MIN_THRESHOLD = float(os.getenv("PNL_MIN_THRESHOLD", "0.6"))

# базовые query-параметры (уникализируем на воркера ниже)
PARAMS = {
    "from_app": "gmgn",
    "os": "web",
    "period": "7d",
    "tz_name": "Europe/Moscow",
    "tz_offset": "10800",
    "app_lang": "ru",
}

WAIT_CLEARANCE_SECONDS = 75
HEADLESS = False
SLEEP_BETWEEN_REQ = (3.0, 4.0)
API_TEMPLATE = "https://gmgn.ai/api/v1/wallet_stat/{chain}/{wallet}/7d"

COOKIE_REFRESH_JITTER = (1.0, 2.0)
COOKIE_REFRESH_TIMEOUT = int(os.getenv("COOKIE_REFRESH_TIMEOUT", "180"))
COOKIES_MAX_AGE_S = int(os.getenv("COOKIES_MAX_AGE_S", "5400"))

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_7_10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:138.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
]

ACCEPT_LANGS = [
    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "ru,ru-RU;q=0.9,en;q=0.8",
    "ru-RU,ru;q=0.95,en;q=0.7",
    "ru,en-US;q=0.9,en;q=0.8",
    "ru-RU,ru;q=0.9,en;q=0.8,uk;q=0.6",
    "ru-RU,ru;q=0.9,en;q=0.8,de;q=0.7",
    "ru-RU,ru;q=0.9,en;q=0.8,fi;q=0.6",
    "ru-RU,ru;q=0.9,en;q=0.8,es;q=0.5",
    "ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7",
    "ru-RU,ru;q=0.9,en;q=0.75",
]

def headers_for_worker(idx: int, ua: str) -> Dict[str, str]:
    lang = ACCEPT_LANGS[idx % len(ACCEPT_LANGS)]
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": lang,
        "user-agent": ua,
        "referer": HOME_URL,
        "origin": "https://gmgn.ai",
    }

# ─── прокси ──────────────────────────────────────────────────────────
PROXY_STRS = [
    "178.171.42.135:9056:t1d496:grovgA",
    "217.106.239.149:9890:Rte1x5:tmbgBn",
    "81.177.23.10:9448:Rte1x5:tmbgBn",
    "88.218.73.202:9837:Rte1x5:tmbgBn",
    "188.119.124.152:9151:KsVAeX:8b12ZJ",
    "188.119.126.206:9458:KsVAeX:8b12ZJ",
    "188.119.126.41:9153:KsVAeX:8b12ZJ",
    "5.8.15.181:9408:KsVAeX:8b12ZJ",
    "5.8.13.246:9363:KsVAeX:8b12ZJ",
    "212.102.145.1:9130:hBBd4b:oSV52Q",
    "178.171.43.171:9324:ep5rm9:gJvgf9",
    "212.102.146.81:9719:ep5rm9:gJvgf9",
    "212.102.144.216:9525:ep5rm9:gJvgf9",
    "212.102.144.28:9109:ep5rm9:gJvgf9",
    "212.102.146.211:9286:ep5rm9:gJvgf9",
    "212.102.146.17:9806:ep5rm9:gJvgf9",
    "178.171.42.12:9378:ep5rm9:gJvgf9",
    "212.102.144.190:9887:ep5rm9:gJvgf9",
    "178.171.42.249:9883:ep5rm9:gJvgf9",
    "212.102.144.39:9099:ep5rm9:gJvgf9",
]

# ─── утилиты ─────────────────────────────────────────────────────────
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

# ─── модели ──────────────────────────────────────────────────────────
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
    wallets: List[str] = field(default_factory=list)
    params: Dict[str, str] = field(default_factory=dict)
    cookies_ts: float = 0.0
    stats: Stats = field(default_factory=Stats)

# ─── UA/headers утилиты ──────────────────────────────────────────────
def _choose_alt_ua_idx(current_idx: int) -> int:
    n = len(UA_LIST)
    if n <= 1:
        return current_idx
    j = random.randrange(n - 1)
    if j >= current_idx:
        j += 1
    return j

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

def _tz_offset_seconds(tz_name: str, fallback_seconds: int = 0) -> int:
    try:
        if ZoneInfo is not None:
            z = ZoneInfo(tz_name)
            now = dt.datetime.now(z)
            offs = now.utcoffset()
            if offs is not None:
                return int(offs.total_seconds())
    except Exception as e:
        logger.warning(f"ZoneInfo failed for '{tz_name}': {e!r}. Fallback to {fallback_seconds}s.")
    return fallback_seconds

def _gen_ids(seed_bytes: bytes | None = None) -> tuple[str, str, str, str]:
    if seed_bytes is None:
        seed_bytes = secrets.token_bytes(16)

    device_id = str(uuid.uuid4())
    ts = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M")
    rand8 = secrets.token_hex(4)
    client_id = f"gmgn_web_{ts}-{rand8}"
    app_ver = client_id

    fp_material = device_id.encode() + seed_bytes
    fp_did = hashlib.md5(fp_material).hexdigest()
    return device_id, client_id, app_ver, fp_did

def make_worker_params(base: dict, worker_idx: int, ua: str,
                       tz_name: str = "Europe/Moscow", app_lang: str = "ru") -> dict:
    seed = hashlib.sha256(f"{worker_idx}-{ua}-{time.time_ns()}".encode()).digest()
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

# ─── Cookie refresh ──────────────────────────────────────────────────
COOKIE_QUEUE: Optional["CookieRefreshQueue"] = None

class CookieRefreshQueue:
    def __init__(self, loop: asyncio.AbstractEventLoop, max_parallel: int = 1):
        self.loop = loop
        self.sem = asyncio.Semaphore(max_parallel)

    async def _refresh(self, worker: Worker, reason: str) -> Dict[str, str]:
        log = logger.bind(worker=worker.name, proxy=mask_proxy(worker.proxy.server_url))
        log.warning(f"Запрос на обновление cookies ({reason}) — ожидаю свободный слот...")
        async with self.sem:
            log.warning("Получил слот → обновляю cookies через Playwright")
            try:
                cookies = await asyncio.wait_for(
                    fetch_cookies_for_worker(worker),
                    timeout=COOKIE_REFRESH_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.error(f"Таймаут обновления cookies через {COOKIE_REFRESH_TIMEOUT}s")
                return {}
            return cookies or {}

    def refresh_blocking(self, worker: Worker, reason: str, timeout: float = None) -> Dict[str, str]:
        if timeout is None:
            timeout = COOKIE_REFRESH_TIMEOUT + 30
        fut = asyncio.run_coroutine_threadsafe(self._refresh(worker, reason), self.loop)
        return fut.result(timeout=timeout)

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

# ─── HTTP helpers ───────────────────────────────────────────────────

# Новые настройки для управления логами успешных 2xx
SUCCESS_LOG_SAMPLE_RATE = float(os.getenv("SUCCESS_LOG_SAMPLE_RATE", "0.02"))
SUCCESS_LOG_SLOW_MS = float(os.getenv("SUCCESS_LOG_SLOW_MS", "1500"))

def maybe_log_success(log, sc: int, dt_ms: float, resp_len: int):
    """Логировать успешный ответ только если включён сэмплинг или он медленный."""
    if SUCCESS_LOG_SLOW_MS and dt_ms >= SUCCESS_LOG_SLOW_MS:
        log.bind(slow=True).info(f"{sc} OK in {dt_ms:.0f} ms, bytes={resp_len}")
    elif SUCCESS_LOG_SAMPLE_RATE and random.random() < SUCCESS_LOG_SAMPLE_RATE:
        log.bind(sample=True).info(f"{sc} OK in {dt_ms:.0f} ms, bytes={resp_len}")
    # иначе — молчим, чтобы не спамить

def ensure_browser_like_headers(sess):
    sess.headers.setdefault("sec-fetch-site", "same-origin")
    sess.headers.setdefault("sec-fetch-mode", "cors")
    sess.headers.setdefault("sec-fetch-dest", "empty")
    sess.headers.setdefault("accept-encoding", "gzip, deflate, br, zstd")

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
                # отключаем спам успешных запросов (сэмплим/логируем только медленные при желании)
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

# ─── DB save (async) ─────────────────────────────────────────────────
@with_db_session
async def save_snapshot_if_positive(wallet: str, pnl_value: float, *, db_session):
    if pnl_value is None or pnl_value <= PNL_MIN_THRESHOLD:
        return
    snapshot = Wallet(address=wallet, pnl=round(pnl_value, 3))
    db_session.add(snapshot)
    try:
        await db_session.flush()
        logger.success(f"Snapshot saved for {wallet} (PnL {pnl_value:.3f})")
    except Exception as e:
        # допускаем уникальный индекс на address
        try:
            from sqlalchemy.exc import IntegrityError
            if isinstance(e, IntegrityError):
                await db_session.rollback()
                logger.info(f"Snapshot duplicate ignored for {wallet}")
                return
        except Exception:
            pass
        await db_session.rollback()
        logger.exception(f"DB error on save {wallet}: {e!r}")

# ─── основная работа воркера (sync, в пуле) ─────────────────────────
def run_worker_requests(worker: Worker) -> Tuple[str, Stats, List[Tuple[str, float]]]:
    log = logger.bind(worker=worker.name, proxy=mask_proxy(worker.proxy.server_url))
    proxies = worker.proxy.for_curl()
    try:
        sess = curl.Session(impersonate="chrome", proxies=proxies, timeout=30)
    except Exception:
        sess = curl.Session(impersonate="chrome", proxies=proxies, timeout=30)

    sess.headers.update(worker.headers)
    ensure_browser_like_headers(sess)

    log.debug(f"Headers: {dict(sess.headers)}")
    log.debug(f"Cookies: {list(worker.cookies.keys())}")
    log.debug(f"Params: {worker.params}")

    total = len(worker.wallets)
    log.info(f"Начинаю обработку кошельков: {total}")

    positives: List[Tuple[str, float]] = []

    for idx, wallet in enumerate(worker.wallets, 1):
        url = API_TEMPLATE.format(chain=quote(CHAIN), wallet=quote(wallet))
        req_log = log.bind(wallet=wallet)

        now = time.time()
        if worker.cookies and worker.cookies_ts and (now - worker.cookies_ts > COOKIES_MAX_AGE_S):
            req_log.info("Cookies устарели → обновляю заранее (proactive)")
            worker.stats.refreshes += 1
            if COOKIE_QUEUE is not None:
                try:
                    new_cookies = COOKIE_QUEUE.refresh_blocking(worker, reason="proactive refresh")
                    if new_cookies:
                        worker.cookies = new_cookies
                        worker.cookies_ts = time.time()
                        time.sleep(random.uniform(*COOKIE_REFRESH_JITTER))
                    else:
                        req_log.warning("Не удалось обновить cookies проактивно")
                except Exception as e:
                    worker.stats.exceptions += 1
                    req_log.exception(f"Ошибка proactive refresh: {e!r}")

        resp = None
        try:
            resp = get_with_retry(sess, url, worker.params, req_log, cookies=worker.cookies, max_attempts=3)
            worker.stats.attempts += 1
            if resp is None:
                worker.stats.exceptions += 1
                req_log.error("Нет ответа после всех попыток")
                if SLEEP_BETWEEN_REQ[1] > 0:
                    time.sleep(random.uniform(*SLEEP_BETWEEN_REQ))
                continue

            sc = resp.status_code

            if sc == 403:
                req_log.warning("403 → смена UA/headers и обновление cookies")
                try:
                    rotate_identity(worker, sess, reason=f"403 on {wallet}")
                    worker.stats.refreshes += 1

                    if COOKIE_QUEUE is not None:
                        new_cookies = COOKIE_QUEUE.refresh_blocking(worker, reason=f"403 on {wallet}")
                        if new_cookies:
                            worker.cookies = new_cookies
                            worker.cookies_ts = time.time()
                            req_log.info("Cookies обновлены (под новым UA). Повторяю запрос")
                            time.sleep(random.uniform(*COOKIE_REFRESH_JITTER))

                            resp2 = get_with_retry(
                                sess, url, worker.params, req_log.bind(after="refresh+ua"),
                                cookies=worker.cookies, max_attempts=2, sleep_base=1.0
                            )
                            worker.stats.attempts += 1
                            if resp2 is None:
                                worker.stats.exceptions += 1
                                req_log.error("Нет ответа после обновления cookies (под новым UA)")
                            else:
                                sc2 = resp2.status_code
                                if 200 <= sc2 < 300:
                                    # ── PARSE + COLLECT POSITIVES ──
                                    try:
                                        data = resp2.json()
                                        pnl = (data or {}).get("data", {}).get("pnl")
                                        if isinstance(pnl, (int, float)) and pnl > PNL_MIN_THRESHOLD:
                                            positives.append((wallet, float(pnl)))
                                    except Exception:
                                        pass
                                    worker.stats.ok += 1
                                    worker.stats.bytes_rx += len(resp2.content)
                                elif sc2 == 403:
                                    worker.stats.forbidden += 1
                                elif sc2 == 429:
                                    worker.stats.rate_limited += 1
                                elif 500 <= sc2 < 600:
                                    worker.stats.server_err += 1
                                else:
                                    worker.stats.other_err += 1
                        else:
                            req_log.warning("Не удалось получить новые cookies (под новым UA)")
                            worker.stats.forbidden += 1
                    else:
                        req_log.warning("COOKIE_QUEUE не инициализирована")
                        worker.stats.forbidden += 1
                except Exception as e:
                    worker.stats.exceptions += 1
                    req_log.exception(f"Ошибка при обновлении cookies/смене UA: {e!r}")

            elif 200 <= sc < 300:
                # ── PARSE + COLLECT POSITIVES ──
                try:
                    data = resp.json()
                    pnl = (data or {}).get("data", {}).get("pnl")
                    if isinstance(pnl, (int, float)) and pnl > PNL_MIN_THRESHOLD:
                        positives.append((wallet, float(pnl)))
                except Exception:
                    pass
                worker.stats.ok += 1
                worker.stats.bytes_rx += len(resp.content)

            elif sc == 429:
                worker.stats.rate_limited += 1

            elif 500 <= sc < 600:
                worker.stats.server_err += 1

            else:
                worker.stats.other_err += 1

        except Exception as e:
            worker.stats.exceptions += 1
            req_log.exception(f"Исключение при запросе: {e!r}")

        if SLEEP_BETWEEN_REQ[1] > 0:
            time.sleep(random.uniform(*SLEEP_BETWEEN_REQ))

    sess.close()
    log.info(
        "Итог воркера → "
        f"ok={worker.stats.ok}, 403={worker.stats.forbidden}, 429={worker.stats.rate_limited}, "
        f"5xx={worker.stats.server_err}, other4xx={worker.stats.other_err}, exc={worker.stats.exceptions}, "
        f"refreshes={worker.stats.refreshes}, ua_switches={worker.stats.ua_switches}, bytes={worker.stats.bytes_rx}"
    )
    return worker.name, worker.stats, positives

# ─── сборка воркеров ─────────────────────────────────────────────────
def build_workers() -> List[Worker]:
    proxies = [ProxyCfg.parse(s) for s in PROXY_STRS]
    if len(UA_LIST) != len(proxies):
        raise RuntimeError(f"UA_LIST({len(UA_LIST)}) must match proxies({len(proxies)}).")
    workers: List[Worker] = []
    for i in range(len(proxies)):
        w = Worker(
            name=f"W{i+1}",
            user_agent=UA_LIST[i],
            proxy=proxies[i],
            ua_idx=i,
        )
        w.headers = headers_for_worker(i, w.user_agent)
        w.params  = make_worker_params(PARAMS, i, w.user_agent, tz_name="Europe/Moscow", app_lang="ru")
        workers.append(w)
    return workers

def _fmt_hms(seconds: float) -> str:
    s = int(seconds)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

async def process_batch(wallets: List[str], *, token: str = "batch"):
    t_start = time.perf_counter()

    logger.info(f"Старт батча token={token} wallets={len(wallets)}")
    if not wallets:
        logger.warning("Пустой батч — пропускаю.")
        return

    workers = build_workers()

    NUM_WORKERS = min(20, len(workers))
    parts = split_evenly(wallets, NUM_WORKERS)
    if len(workers) < NUM_WORKERS:
        logger.warning(f"workers={len(workers)} < {NUM_WORKERS}; часть частей не будет назначена.")
    for i, w in enumerate(workers[:NUM_WORKERS]):
        w.wallets = parts[i]
        logger.info(f"[{w.name}] назначено кошельков: {len(w.wallets)} | proxy={mask_proxy(w.proxy.server_url)}")

    # ── СНАЧАЛА КУКИ ДЛЯ ВСЕХ ВОРКЕРОВ (ПО ОЧЕРЕДИ) ───────────────────
    selected = workers[:NUM_WORKERS]
    logger.info(f"Последовательный съём cookies для {len(selected)} воркеров...")
    cookies_list: List[Dict[str, str]] = []
    for w in selected:
        try:
            c = await fetch_cookies_for_worker(w)   # строго последовательно
        except Exception as e:
            logger.bind(worker=w.name).exception(f"Ошибка при съёме cookies: {e!r}")
            c = {}
        c = c or {}
        cookies_list.append(c)
        w.cookies = c
        w.cookies_ts = time.time() if w.cookies else 0.0
        if not w.cookies:
            logger.bind(worker=w.name).warning("Пустые cookies — возможны 403.")
        await asyncio.sleep(0.5)  # мягкая пауза между воркерами (можно убрать)

    # ── ТЕПЕРЬ ЗАПУСКАЕМ HTTP-ОБРАБОТКУ ДЛЯ ВСЕХ ──────────────────────
    results: List[Tuple[str, Stats, List[Tuple[str, float]]]] = []
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=min(NUM_WORKERS, len(workers))) as pool:
        tasks = [loop.run_in_executor(pool, run_worker_requests, w) for w in selected]
        for coro in asyncio.as_completed(tasks):
            try:
                results.append(await coro)
            except Exception as e:
                logger.exception(f"Исключение в пуле: {e!r}")

    # ── свод + сохранение в БД (как было) ─────────────────────────────
    total_stats = Stats()
    positives_all: List[Tuple[str, float]] = []
    for name, st, pos in results:
        total_stats.ok += st.ok
        total_stats.forbidden += st.forbidden
        total_stats.rate_limited += st.rate_limited
        total_stats.server_err += st.server_err
        total_stats.other_err += st.other_err
        total_stats.exceptions += st.exceptions
        total_stats.bytes_rx += st.bytes_rx
        total_stats.attempts += st.attempts
        total_stats.refreshes += st.refreshes
        total_stats.ua_switches += st.ua_switches
        positives_all.extend(pos)

    if positives_all:
        logger.info(f"Сохранение {len(positives_all)} записей в БД (PnL > {PNL_MIN_THRESHOLD})")
        for wallet, pnl in positives_all:
            try:
                await save_snapshot_if_positive(wallet, pnl)
            except Exception as e:
                logger.exception(f"DB save failed for {wallet}: {e!r}")

    elapsed = time.perf_counter() - t_start
    logger.success(
        "Свод: "
        f"ok={total_stats.ok}, 403={total_stats.forbidden}, 429={total_stats.rate_limited}, "
        f"5xx={total_stats.server_err}, other4xx={total_stats.other_err}, exc={total_stats.exceptions}, "
        f"refreshes={total_stats.refreshes}, ua_switches={total_stats.ua_switches}, bytes={total_stats.bytes_rx}, "
        f"attempts={total_stats.attempts}, wallets_total={len(wallets)}, duration={_fmt_hms(elapsed)} ({elapsed:.1f}s)"
    )
# ─── Redis-консюмер (бесконечный цикл) ───────────────────────────────
async def redis_loop() -> None:
    rds = get_redis()  # async Redis клиент
    logger.success(f"Worker started, queue '{QUEUE_NAME}'")

    while True:
        try:
            _key, payload = await rds.blpop(QUEUE_NAME)
            raw = json.loads(payload)

            if isinstance(raw, dict):
                wallets = raw.get("wallets", []) or []
                token   = raw.get("token", "unknown")
            else:
                wallets = raw or []
                token = "legacy"

            # нормализуем строки
            wallets = [w["signing_wallet"] if isinstance(w, dict) and "signing_wallet" in w else str(w) for w in wallets]
            # фильтруем пустые
            wallets = [w.strip() for w in wallets if w]

            if not wallets:
                logger.warning(f"Пустое сообщение в очереди: {raw}")
                continue

            await process_batch(wallets, token=token)

        except Exception as exc:
            logger.exception(f"redis_loop error: {exc!r}")
            await asyncio.sleep(3)

# ─── entrypoint ──────────────────────────────────────────────────────
async def main():
    logger.info("Старт gmgn_multi_workers (режим Redis→GMGN→DB)")
    # инициализируем очередь обновлений cookies (параллелизм = 1)
    global COOKIE_QUEUE
    COOKIE_QUEUE = CookieRefreshQueue(asyncio.get_running_loop(), max_parallel=1)
    # запускаем консюмер очереди
    await redis_loop()

if __name__ == "__main__":
    asyncio.run(main())
