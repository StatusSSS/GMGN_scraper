import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from src.scraper.gmgn import fingerprints
from curl_cffi import requests as curl


fingerprints.init_proxies(["local"])
PROXY_KEY = "local"
request_counter = 0

HEADERS = {
    "sec-ch-ua-full-version-list": "\"Google Chrome\";v=\"137.0.7151.104\", \"Chromium\";v=\"137.0.7151.104\", \"Not/A)Brand\";v=\"24.0.0.0\"",
    "sec-ch-ua-platform": "\"Windows\"",
    "Referer": "https://gmgn.ai/sol/address/43DbKtri_9vooJ8FPhM4k7n39JC9NtW2942K1NUtotNjbeCbbC3jF",
    "sec-ch-ua": "\"Google Chrome\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
    "sec-ch-ua-bitness": "\"64\"",
    "sec-ch-ua-model": "\"\"",
    "sec-ch-ua-mobile": "?0",
    "baggage": "sentry-environment=production,sentry-release=20250617-62-c04b007,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=4d83c3ec170d4d598ecb8d9293b3647e,sentry-sample_rate=0.005,sentry-sampled=false",
    "sentry-trace": "461f75434c7d4bb3b80adcb56f4244fe-8e0d7e42f7995574-0",
    "sec-ch-ua-arch": "\"x86\"",
    "sec-ch-ua-full-version": "\"137.0.7151.104\"",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "sec-ch-ua-platform-version": "\"15.0.0\"",
}

BASE_PARAMS = {
    "device_id": "c45e37f7-53ff-4d68-813b-fd0f7b736979",
    "client_id": "gmgn_web_20250617-62-c04b007",
    "from_app": "gmgn",
    "app_ver": "20250617-62-c04b007",
    "tz_name": "Europe/Moscow",
    "tz_offset": "10800",
    "app_lang": "ru",
    "fp_did": "77abb27885cffbec63c7f9fbd35b4116",
    "os": "web",
    "limit": "50",
    "orderby": "last_active_timestamp",
    "direction": "desc",
    "showsmall": "true",
    "sellout": "true",
    "tx30d": "true",
}

API_URL = "https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/7d"


# ----------------------------  запрос к API  ---------------------------- #
def fetch_wallet_stat(wallet: str):
    """
    Запрашивает статистику по кошельку и возвращает кортеж:
    (wallet, pnl_7d(%), winrate(%))
    """
    url = API_URL.format(wallet=wallet)
    # можно добавить Referer, если нужен 100 % паритет с оригиналом
    headers = HEADERS | {
        'Referer': f'https://gmgn.ai/sol/address/{wallet}',
    }

    response = curl.get(
        url,
        params=BASE_PARAMS,
        headers=headers,
        impersonate="chrome120",
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()["data"]
    pnl_pct = round(data["pnl_7d"] * 100, 2)
    win_pct = round(data["winrate"] * 100, 2)
    print(pnl_pct)
    return wallet, pnl_pct, win_pct


# ----------------------------  основной поток  ---------------------------- #
def main():
    # читаем адреса из wallets.txt (по одному в строке)
    wallets = [
        line.strip()
        for line in Path("src/scraper/gmgn/wallets.txt").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    # запускаем максимум по 4 параллельных запроса
    with ThreadPoolExecutor(max_workers=1) as pool:
        futures = []
        for wallet in wallets:
            futures.append(pool.submit(fetch_wallet_stat, wallet))
            time.sleep(0.5)        # задержка перед отправкой следующего задания


if __name__ == "__main__":
    main()
