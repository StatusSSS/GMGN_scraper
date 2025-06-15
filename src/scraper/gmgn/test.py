from curl_cffi import requests as curl
from datetime import datetime, timezone
from statistics import mean
from datetime import timedelta


headers = {
    'sec-ch-ua-full-version-list': '"Google Chrome";v="137.0.7151.104", "Chromium";v="137.0.7151.104", "Not/A)Brand";v="24.0.0.0"',
    'sec-ch-ua-platform': '"Windows"',
    'Referer': 'https://gmgn.ai/sol/address/43DbKtri_DxE1bvLvb6wWzDC9EbdoRw2VpyQYb6MnvE5PgRDCUh4J',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-mobile': '?0',
    'baggage': 'sentry-environment=production,sentry-release=20250613-2194-6838f94,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=9bc3568e003d40309d28bec0dc9cb4c1,sentry-sample_rate=0.005,sentry-sampled=false',
    'sentry-trace': '8dbd9d39cc9f4fb1af272112d5045f41-a013b6a10e3129bc-0',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-full-version': '"137.0.7151.104"',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'sec-ch-ua-platform-version': '"15.0.0"',
}

params = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'client_id': 'gmgn_web_20250613-2194-6838f94',
    'from_app': 'gmgn',
    'app_ver': '20250613-2194-6838f94',
    'tz_name': 'Europe/Moscow',
    'tz_offset': '10800',
    'app_lang': 'ru',
    'fp_did': '77abb27885cffbec63c7f9fbd35b4116',
    'os': 'web',
    'limit': '50',
    'orderby': 'last_active_timestamp',
    'direction': 'desc',
    'showsmall': 'true',
    'sellout': 'true',
    'tx30d': 'true',
}

response = curl.get(
    'https://gmgn.ai/api/v1/wallet_holdings/sol/91qH4GakcjasJRo5A2Ht7wQSmCV1gSmf7QnvE5DTBxES',
    params=params,
    headers=headers,
    impersonate="chrome120",
    timeout=30,
)

data = response.json()["data"]['holdings']



def ts_to_str(ts: int | None) -> str | None:
    """Преобразует Unix-timestamp в строку, если ts не None."""
    if ts is None:
        return None          # Можно вернуть "" — как вам удобнее
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


summary = [
    {
        "symbol": item["token"]["symbol"],
        "total_profit_pnl": float(item["total_profit_pnl"]),
        "total_profit": item["total_profit"],
        "start_holding_at": item["start_holding_at"],
        "started": ts_to_str(item["start_holding_at"]),
        "end_holding_at": item["end_holding_at"],
        "ended": ts_to_str(item["end_holding_at"]),   # ← безопасно
    }
    for item in data
]

# # Посмотреть результат
# for row in summary:
#     print(row)

wins   = [t for t in summary if t["total_profit_pnl"] >= 0]
losses = [t for t in summary if t["total_profit_pnl"] < 0]

# 2. win-rate
total_trades = len(summary)
winrate = len(wins) / total_trades * 100 if total_trades else 0

# 3. сделки с PnL ≥ 100 %
big_wins = [t for t in summary if t["total_profit_pnl"] >= 1]

# 4. среднее время удержания (только там, где обе даты заданы)
durations = [
    t["end_holding_at"] - t["start_holding_at"]
    for t in summary
    if t["start_holding_at"] and t["end_holding_at"]
]
avg_hold = timedelta(seconds=mean(durations)) if durations else None

# 5. печать
print(f"Всего сделок          : {total_trades}")
print(f"Win-rate              : {winrate:.1f} %")
print(f"Плюсовых сделок       : {len(wins)}")
print(f"Минусовых сделок      : {len(losses)}")
print(f"Сделок с PnL ≥ 100 %  : {len(big_wins)}")
if avg_hold:
    m, s = divmod(avg_hold.seconds, 60)
    print(f"Среднее удержание     : {m} мин {s} сек (на {len(durations)} закрытых сделок)")
else:
    print("Среднее удержание     : нет закрытых сделок")