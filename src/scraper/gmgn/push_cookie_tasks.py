#!/usr/bin/env python
"""
Кладёт задания в Redis-очередь cookie_tasks:

    {"proxy": "ip:port", "ua": "<User-Agent>"}

UA детерминирован: сид = f"{proxy}-0"
"""

import json, os, random, redis
from pathlib import Path
from src.scraper.gmgn.fingerprints import Identity

# ─── helpers ─────────────────────────────────────────────────────────
def load_lines(path: str) -> list[str]:
    fp = Path(path)
    if not fp.exists():
        raise SystemExit(f"{path} not found")
    with fp.open(encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]

# ─── Redis ──────────────────────────────────────────────────────────
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)

# ─── список прокси (ip:port) ────────────────────────────────────────
proxies = load_lines("src/scraper/gmgn/proxies.txt")
if not proxies:
    raise SystemExit("proxies.txt пуст или не найден")

r.delete("cookie_tasks")          # очистка очереди

for p in proxies:                 # p уже вида ip:port
    ua = Identity(random.Random(f"{p}-0")).static_headers["User-Agent"]
    r.rpush("cookie_tasks", json.dumps({"proxy": p, "ua": ua}))

print(f"Pushed {len(proxies)} tasks → cookie_tasks")
