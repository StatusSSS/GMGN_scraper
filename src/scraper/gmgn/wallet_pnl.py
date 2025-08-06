import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from src.scraper.gmgn import fingerprints
from curl_cffi import requests as curl


fingerprints.init_proxies(["local"])
PROXY_KEY = "local"
request_counter = 0

cookies = {
    '_ga': 'GA1.1.1261906334.1735592964',
    'GMGN_CHAIN': 'sol',
    'GMGN_THEME': 'dark',
    'GMGN_LOCALE': 'ru',
    'sid': 'gmgn%7C9bfc79f3abed53636baf47a8b684ea2e',
    '_ga_UGLVBMV4Z0': 'GS1.2.1754481492313289.67ff2258fe16dc4664041c17afaac8bb.DsZwhlX%2B4G5SoAz92KTwhg%3D%3D.PlgyNy8D9xT9aCqrakXITQ%3D%3D.qnl4SP0BhxS6Nda6cznAtg%3D%3D.hOHO4ib59aMH67p5H4Gamw%3D%3D',
    '__cf_bm': 'lh2T1btICM_2gdTE7MRBwbC9A2R2EJdvtAtgBasn3kU-1754482516-1.0.1.1-RuzyB.CsD2VoEa.Lc4fM80n.7sTqKyc1HgCILX09r3hex2bwybqKPeAwpeVHNneHxHVsMKGVb8ZuTNZFjDaxIQwNSjHFuGQSPxmbtkdlOCI',
    'cf_clearance': 'G7.esrdPmFsSGEPUKD5_ORY7cFsChEORANDsJb4Fauk-1754482518-1.2.1.1-qe0PYOWAjsHpaC9wg3UGpnUYRIgiOjidOZd4caZYAp_CNoeSqbqElPYaIAxQ5whDREiPWbCNsb_aPmEQwYUQeYne80WJPKDisk8EGRNFpum7FYZjaReG8RMbfdVXCdq57E6cV5o4a1n7looL_8EoQwfs3f.EjenXFm1ybmSlGb64oJZmSF1Mzpp8wcdHymVERdc1S9.b3QiHOEq44APPHcqnGiLLGrP8FeEOsBrl0H8',
    '_ga_0XM0LYXGC8': 'GS2.1.s1754481451$o305$g1$t1754482728$j60$l0$h0',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'baggage': 'sentry-environment=production,sentry-release=20250805-1994-2fe2cb8,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=b7126acb50ec4ff99bcbd6c74775bcea,sentry-sample_rate=0.01,sentry-sampled=false',
    'priority': 'u=1, i',
    'referer': 'https://gmgn.ai/sol/address/9NQn3TLma9sKd6pbBuXy5we6PMs6QLWAz8awhZyGNgmo',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"138.0.7204.184"',
    'sec-ch-ua-full-version-list': '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.7204.184", "Google Chrome";v="138.0.7204.184"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': 'b7126acb50ec4ff99bcbd6c74775bcea-9e08a2af38218141-0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    # 'cookie': '_ga=GA1.1.1261906334.1735592964; GMGN_CHAIN=sol; GMGN_THEME=dark; GMGN_LOCALE=ru; sid=gmgn%7C9bfc79f3abed53636baf47a8b684ea2e; _ga_UGLVBMV4Z0=GS1.2.1754481492313289.67ff2258fe16dc4664041c17afaac8bb.DsZwhlX%2B4G5SoAz92KTwhg%3D%3D.PlgyNy8D9xT9aCqrakXITQ%3D%3D.qnl4SP0BhxS6Nda6cznAtg%3D%3D.hOHO4ib59aMH67p5H4Gamw%3D%3D; __cf_bm=lh2T1btICM_2gdTE7MRBwbC9A2R2EJdvtAtgBasn3kU-1754482516-1.0.1.1-RuzyB.CsD2VoEa.Lc4fM80n.7sTqKyc1HgCILX09r3hex2bwybqKPeAwpeVHNneHxHVsMKGVb8ZuTNZFjDaxIQwNSjHFuGQSPxmbtkdlOCI; cf_clearance=G7.esrdPmFsSGEPUKD5_ORY7cFsChEORANDsJb4Fauk-1754482518-1.2.1.1-qe0PYOWAjsHpaC9wg3UGpnUYRIgiOjidOZd4caZYAp_CNoeSqbqElPYaIAxQ5whDREiPWbCNsb_aPmEQwYUQeYne80WJPKDisk8EGRNFpum7FYZjaReG8RMbfdVXCdq57E6cV5o4a1n7looL_8EoQwfs3f.EjenXFm1ybmSlGb64oJZmSF1Mzpp8wcdHymVERdc1S9.b3QiHOEq44APPHcqnGiLLGrP8FeEOsBrl0H8; _ga_0XM0LYXGC8=GS2.1.s1754481451$o305$g1$t1754482728$j60$l0$h0',
}

params = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'client_id': 'gmgn_web_20250805-1994-2fe2cb8',
    'from_app': 'gmgn',
    'app_ver': '20250805-1994-2fe2cb8',
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
    headerss = headers | {
        'Referer': f'https://gmgn.ai/sol/address/{wallet}',
    }

    response = curl.get(
        url,
        params=params,
        headers=headerss,
        impersonate="chrome120",
        timeout=30,
    )
    print(params)
    print(headerss)
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
