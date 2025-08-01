#!/usr/bin/env python
# scripts/push_cookie_tasks.py
#
# Кладёт в Redis очередь заданий «получить cookies»:
#   key   = "cookie_tasks"
#   value = {"proxy": "ip:port:user:pwd", "ua": "<User-Agent>"}
#
# UA генерируем из fingerprints.Identity с тем же сидом,
# что использует sync_scraper: seed = f"{proxy}-0"

import json, os, random, redis
from pathlib import Path

from src.scraper.gmgn.fingerprints import Identity

# ─── local helper (чтобы не тащить sync_scraper с Selenium) ─────────
def load_lines(path: str):
    fp = Path(path)
    if not fp.exists():
        raise SystemExit(f"{path} not found")
    with fp.open(encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]

# ─── Redis ──────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)

# ─── Прокси ─────────────────────────────────────────────────────────
proxies = load_lines("src/scraper/gmgn/proxies.txt")        # ip:port:user:pwd
if not proxies:
    raise SystemExit("proxies.txt пуст или не найден")

r.delete("cookie_tasks")                   # очистить очередь

for p in proxies:
    ident = Identity(random.Random(f"{p}-0"))   # детерминированная «личность»
    ua = ident.static_headers["User-Agent"]
    r.rpush("cookie_tasks", json.dumps({"proxy": p, "ua": ua}))

print(
    f"Pushed {len(proxies)} tasks into "
    f"Redis:{REDIS_HOST}:{REDIS_PORT} › cookie_tasks"
)
