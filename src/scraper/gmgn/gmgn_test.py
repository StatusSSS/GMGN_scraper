"""
Тестовый запуск без Redis: запрашиваем gmgn.ai по списку кошельков,
печатаем статус и простую метрику (pnl) для каждого.
Пример:
    python -m src.gmgn.gmgn_test DxE1bvLvb6wW... 4YNB8qed56...
или   python -m src.gmgn.gmgn_test --file wallets.txt
"""
import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import List

from curl_cffi import requests as curl
from dotenv import load_dotenv

load_dotenv()

API_PERIOD   = os.getenv("GMGN_PERIOD", "7d")
API_TIMEOUT  = int(os.getenv("GMGN_TIMEOUT", "30"))
MAX_RETRIES  = int(os.getenv("GMGN_RETRIES", "3"))




headers = {
    'sec-ch-ua-full-version-list': '"Google Chrome";v="137.0.7151.104", "Chromium";v="137.0.7151.104", "Not/A)Brand";v="24.0.0.0"',
    'sec-ch-ua-platform': '"Windows"',
    'Referer': 'https://gmgn.ai/sol/address/43DbKtri_9vooJ8FPhM4k7n39JC9NtW2942K1NUtotNjbeCbbC3jF',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-mobile': '?0',
    'baggage': 'sentry-environment=production,sentry-release=20250617-62-c04b007,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=4d83c3ec170d4d598ecb8d9293b3647e,sentry-sample_rate=0.005,sentry-sampled=false',
    'sentry-trace': '461f75434c7d4bb3b80adcb56f4244fe-8e0d7e42f7995574-0',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-full-version': '"137.0.7151.104"',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'sec-ch-ua-platform-version': '"15.0.0"',
}
params = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'client_id': 'gmgn_web_20250617-62-c04b007',
    'from_app': 'gmgn',
    'app_ver': '20250617-62-c04b007',
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

def build_url(wallet: str) -> str:
    return 'https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/7d'

def fetch_wallet_stat(wallet: str) -> dict | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = curl.get(
                url=f'https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/7d',
                params=params,
                headers=headers,
                impersonate="chrome120",
                timeout=API_TIMEOUT,
            )
            resp.raise_for_status()
            print(resp.text)
            return resp.json()
        except Exception as e:
            print(f"[{wallet}] попытка {attempt}/{MAX_RETRIES} → {e}")
            if attempt == MAX_RETRIES:
                return None
            time.sleep(1.5)

async def one_wallet(wallet: str) -> None:
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_wallet_stat, wallet)
    if data:
        print(f"{wallet[:6]}… ✔ pnl={data.get('pnl')}")
    else:
        print(f"{wallet[:6]}… ❌")

async def main(wallets: List[str]) -> None:
    tasks = [asyncio.create_task(one_wallet(w)) for w in wallets]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("wallets", nargs="*", help="кошельки через пробел")
    parser.add_argument("--file", help="путь к файлу со списком кошельков")
    args = parser.parse_args()

    _wallets: List[str] = args.wallets
    if args.file:
        _wallets.extend(
            [line.strip() for line in Path(args.file).read_text().splitlines() if line.strip()]
        )

    if not _wallets:
        parser.error("укажите кошельки аргументами или через --file")

    asyncio.run(main(_wallets))
