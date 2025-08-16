import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from src.scraper.gmgn import fingerprints
from curl_cffi import requests as curl


fingerprints.init_proxies(["local"])
PROXY_KEY = "local"
request_counter = 0

proxie = "81.177.23.10:9448:Rte1x5:tmbgBn"
host, port, user, pwd = proxie.split(":", 3)
PROXY_URL = f"http://{user}:{pwd}@{host}:{port}"
PROXIES    = {"http": PROXY_URL, "https": PROXY_URL}



cookies = {
    '_ga': 'GA1.1.1261906334.1735592964',
    '_ga_0XM0LYXGC8': 'GS2.1.s1754919035$o316$g1$t1754919070$j25$l0$h0',
    '__cf_bm': 'FHPrlg9P5gGGsH7d.1mFTINHHVCzg0u7yQTbPK_8eOo-1754940834-1.0.1.1-z9IeRx9lqSqae9oaheDy5coY.YlChXr1yy_uPTP857PktK7616.6l2T5rogBpdZlj.N5e1JTeZLqTomYbQfFl7TifOlJ2zRVTv3P89MDRCI',
    'cf_clearance': '22jA.ydn5pMFgTCtwmHmscnuNJ1xrbEQJsMGDc3gP2I-1754940878-1.2.1.1-mW6E19ejoE138PW43OFWNrZ7S3lEpC1fsqO3qg8wr_3.Dnjjm7XKpO8usdq29z6GPCrFQ1K28YhJY11GOcLKGtIBFdPnOz.25dNHwuH2hI__HXZkLiUk4K1X8hBQ0vJFVTEVQOjPJ4jn4hhozJ7leFvuUjpQNLw8UMJf8CUYQGxZrS0RUpzQqV5GQiLJGg8QmgzXdugSETWpjY4iQ1nS0pgE33DfHlt5CIDBTCBYHBM',
}
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://gmgn.ai/sol/address/43DbKtri_Bw3reJNLgGzAksMMtamNvEYCEqm7oio2d5kUQwAAfB9P',
    'sentry-trace': '0eb253b8aedd4bf7ae9c0507ae4b9d87-ac2e17a1ac76bc11-0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    # 'cookie': '_ga=GA1.1.1261906334.1735592964; GMGN_CHAIN=sol; GMGN_THEME=dark; GMGN_LOCALE=ru; cf_chl_rc_i=1; sid=gmgn%7C67d336d86a583113f6d9524c331a5425; _ga_UGLVBMV4Z0=GS1.2.1754656053375975.67ff2258fe16dc4664041c17afaac8bb.DsZwhlX%2B4G5SoAz92KTwhg%3D%3D.46Atxrl%2FBbbtnrd6RdpYWQ%3D%3D.7eeTa2Z662jGxDgFzSOO%2BA%3D%3D.s7nxJmPVZdnu98gOT7G83Q%3D%3D; __cf_bm=D0q4CFLCj8Eh4OKPJBofS9z2z0vwKPyCc5J7db1wx8E-1754656451-1.0.1.1-zVffOsHsPLg0wV_5_.D06nqUVg_V5if78gEtPj1Av1hd9E3CWiufO_3z31N4zJxzY9AsApLhYRAf6ZRQ9NIs8RAC17q_c44yA_rW9dgvnyY; cf_clearance=paDmKrCSNSaMXQYZeyHst5ExhlWQdlUFt5DeyhKLI8A-1754656588-1.2.1.1-msLKiqJSp_vvlSx4HH1QnNht4arhC2oJYDcC8.wRVI1Wx5SKEu9SgmYC2.dZTUAHb2eh0gCBlHfLO3M03jWcVT74yXPIhe33AuCFlfObcfPcHesjQht6t08Dn2NZT4gMM6OAsHFQemimA_ucpcOeK7qupU4JvjUXHCvVtip5yk_twfb1IqyjfTuveyNBArpr59hd5Rn_3mSAepNW1rdzKKQLXp2Cx4_Zmh85CrcOnT8; _ga_0XM0LYXGC8=GS2.1.s1754656009$o314$g1$t1754656546$j56$l0$h0',
}

params = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'client_id': 'gmgn_web_20250807-2092-28237ac',
    'from_app': 'gmgn',
    'app_ver': '20250807-2092-28237ac',
    'tz_name': 'Europe/Moscow',
    'tz_offset': '10800',
    'app_lang': 'ru',
    'fp_did': '77abb27885cffbec63c7f9fbd35b4116',
    'os': 'web',
    'period': '7d',
}

API_URL = "https://gmgn.ai/api/v1/wallet_stat/sol/{wallet}/7d"

def fetch_wallet_stat(wallet: str):
    """
    Запрашивает статистику по кошельку и возвращает кортеж:
    (wallet, pnl_7d(%), winrate(%))
    """
    url = API_URL.format(wallet=wallet)
    # можно добавить Referer, если нужен 100 % паритет с оригиналом
    headerss = headers | {
        'Referer': f'https://gmgn.ai/sol/address/{wallet}',
    }
    try:
        response = curl.get(
            url,
            params=params,
            headers=headerss,
            impersonate="chrome",
            timeout=30,
            cookies=cookies,
            proxies=PROXIES,
        )
        print(params)
        print(headerss)
        response.raise_for_status()
        data = response.json()["data"]
        pnl_pct = round(data["pnl_7d"] * 100, 2)
        win_pct = round(data["winrate"] * 100, 2)
        print(pnl_pct)
    except Exception as e:
        print(e)

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
            time.sleep(1)        # задержка перед отправкой следующего задания


if __name__ == "__main__":
    main()
