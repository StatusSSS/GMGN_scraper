import asyncio
from curl_cffi import requests as curl

def get_wallet_data():
    url = 'https://gmgn.ai/api/v1/wallet_stat/sol/DxE1bvLvb6wWzDC9EbdoRw2VpyQYb6MnvE5PgRDCUh4J/7d'
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
        'period': '7d',
    }

    headers = {
        'sec-ch-ua-full-version-list': '"Google Chrome";v="137.0.7151.104", "Chromium";v="137.0.7151.104", "Not/A)Brand";v="24.0.0.0"',
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://gmgn.ai/sol/address/DxE1bvLvb6wWzDC9EbdoRw2VpyQYb6MnvE5PgRDCUh4J',
        'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-mobile': '?0',
        'baggage': 'sentry-environment=production,sentry-release=20250613-2194-6838f94,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=9bc3568e003d40309d28bec0dc9cb4c1,sentry-sample_rate=0.005,sentry-sampled=false',
        'sentry-trace': '9bc3568e003d40309d28bec0dc9cb4c1-affc636cf8d49a8b-0',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-full-version': '"137.0.7151.104"',
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'sec-ch-ua-platform-version': '"15.0.0"',
    }
    response = curl.get(url, params=params, headers=headers, impersonate="chrome120", timeout=30)
    response.raise_for_status()
    return response.json()

async def main():
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, get_wallet_data)
    print(data)

asyncio.run(main())