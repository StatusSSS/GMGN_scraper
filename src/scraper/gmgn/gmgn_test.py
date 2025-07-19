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
