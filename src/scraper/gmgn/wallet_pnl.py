import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from curl_cffi import requests as curl


# ----------------------------  общие настройки  ---------------------------- #
HEADERS = {
    'sec-ch-ua-full-version-list': '"Google Chrome";v="137.0.7151.104", '
                                   '"Chromium";v="137.0.7151.104", '
                                   '"Not/A)Brand";v="24.0.0.0"',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-mobile': '?0',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'),
}

BASE_PARAMS = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'client_id': 'gmgn_web_20250613-2194-6838f94',
    'from_app': 'gmgn',
    'app_ver': '20250613-2194-6838f94',
    'tz_name': 'Europe/Moscow',
    'tz_offset': '10800',
    'app_lang': 'ru',
    'fp_did': '77abb27885cffbec63c7f9fbd35b4116',
    'os': 'web',
    'period': '7d',
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
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = []
        for wallet in wallets:
            futures.append(pool.submit(fetch_wallet_stat, wallet))
            time.sleep(0.5)        # задержка перед отправкой следующего задания

        # выводим результаты по мере готовности
        for fut in as_completed(futures):
            wallet, pnl, winrate = fut.result()
            print(f"{wallet}: PnL 7d = {pnl} %, Winrate = {winrate} %")


if __name__ == "__main__":
    main()
