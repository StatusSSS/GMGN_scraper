#!/usr/bin/env python
# scripts/push_cookie_tasks.py
#
# Кладёт в Redis очередь заданий «получить cookies»:
#   key   = "cookie_tasks"
#   value = {"proxy": "ip:port:user:pwd", "ua": "<User-Agent>"}
#
# UA генерируем из fingerprints.Identity с тем же сидом,
# что будет использоваться в sync_scraper (seed = f"{proxy}-0").

import json, os, random, redis

from src.scraper.gmgn.fingerprints import Identity
from src.scraper.gmgn.sync_scraper import load_lines      # переиспользуем утилиту

# ─── Redis ──────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)

# ─── Прокси ─────────────────────────────────────────────
proxies = load_lines("proxies.txt")        # формат ip:port:user:pwd
if not proxies:
    raise SystemExit("proxies.txt пуст или не найден")

r.delete("cookie_tasks")                   # обнуляем очередь

for p in proxies:
    ident = Identity(random.Random(f"{p}-0"))             # ДЕТЕРМИНИРОВАННЫЙ UA
    ua = ident.static_headers["User-Agent"]
    r.rpush("cookie_tasks", json.dumps({"proxy": p, "ua": ua}))

print(f"Pushed {len(proxies)} tasks into Redis:{REDIS_HOST}:{REDIS_PORT} › cookie_tasks")
