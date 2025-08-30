import requests

cookies = {
    '_ga': 'GA1.1.107301769.1755792444',
    '__cf_bm': '6KaTYk29CwPs3kZVthUXXuI9Wna2MvWKuzptoAir4Z4-1755944816-1.0.1.1-IGa4j6xf5g9CDDPrEYmv9sq9O6ZaiWD5n2r370JWRhpmKd4gChpwwqY8wxbyy0uYMLsH.oXj3MvaSSZfIepFlck_3OxQr3aibo0JDHaJicI',
    'cf_clearance': 'ZlcHNgvlWxdHmhAMgEigq8QIKiqWJP4rb5iHa_qzKHM-1755944820-1.2.1.1-0aOE6AGWXD5XOUJNP0uj2rSz_a19TnWdeOcaZ8esY7CBAxAHNUYfcHik7.JTa0.dZYW2TD8Aw0Ow5_qE0MD2AbXu7vT4S8X_R8wyFAKQWCCZuyn7BwHFlWQ8i1hHivNHh_xqVd6NRDCr7jqU_5E3_bR1tRf4aI_PaffXAk2ORtp.pd7rUI7XPZ4PxIYzAr1SSLmPrfFOiFpVTmzMF7.fGxBRMzjPjH.KZp_DJd5LVuU',
    '_ga_0XM0LYXGC8': 'GS2.1.s1755944762$o2$g1$t1755944820$j2$l0$h0',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'baggage': 'sentry-environment=production,sentry-release=20250822-2877-33fd8eb,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=7e47c0e96f5e475ca1a3f7ce8b497955,sentry-sample_rate=0.01,sentry-sampled=false',
    'priority': 'u=1, i',
    'referer': 'https://gmgn.ai/sol/address/5wuAszhsuGmQCYSP7i9mxpxcfU15N7m6cqSiNnViqVLS',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"139.0.7258.139"',
    'sec-ch-ua-full-version-list': '"Not;A=Brand";v="99.0.0.0", "Google Chrome";v="139.0.7258.139", "Chromium";v="139.0.7258.139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': '7e47c0e96f5e475ca1a3f7ce8b497955-a9c78412bccba025-0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    # 'cookie': '_ga=GA1.1.107301769.1755792444; __cf_bm=6KaTYk29CwPs3kZVthUXXuI9Wna2MvWKuzptoAir4Z4-1755944816-1.0.1.1-IGa4j6xf5g9CDDPrEYmv9sq9O6ZaiWD5n2r370JWRhpmKd4gChpwwqY8wxbyy0uYMLsH.oXj3MvaSSZfIepFlck_3OxQr3aibo0JDHaJicI; cf_clearance=ZlcHNgvlWxdHmhAMgEigq8QIKiqWJP4rb5iHa_qzKHM-1755944820-1.2.1.1-0aOE6AGWXD5XOUJNP0uj2rSz_a19TnWdeOcaZ8esY7CBAxAHNUYfcHik7.JTa0.dZYW2TD8Aw0Ow5_qE0MD2AbXu7vT4S8X_R8wyFAKQWCCZuyn7BwHFlWQ8i1hHivNHh_xqVd6NRDCr7jqU_5E3_bR1tRf4aI_PaffXAk2ORtp.pd7rUI7XPZ4PxIYzAr1SSLmPrfFOiFpVTmzMF7.fGxBRMzjPjH.KZp_DJd5LVuU; _ga_0XM0LYXGC8=GS2.1.s1755944762$o2$g1$t1755944820$j2$l0$h0',
}

params = {
    'type': [
        'buy',
        'sell',
    ],
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'fp_did': '77abb27885cffbec63c7f9fbd35b4116',
    'client_id': 'gmgn_web_20250822-2877-33fd8eb',
    'from_app': 'gmgn',
    'app_ver': '20250822-2877-33fd8eb',
    'tz_name': 'Europe/Moscow',
    'tz_offset': '10800',
    'app_lang': 'ru',
    'os': 'web',
    'wallet': '5wuAszhsuGmQCYSP7i9mxpxcfU15N7m6cqSiNnViqVLS',
    'limit': '50',
    'cost': '10',
}

response = requests.get('https://gmgn.ai/vas/api/v1/wallet_activity/sol', params=params, cookies=cookies, headers=headers)




import requests

cookies = {
    '_ga': 'GA1.1.107301769.1755792444',
    '__cf_bm': '6KaTYk29CwPs3kZVthUXXuI9Wna2MvWKuzptoAir4Z4-1755944816-1.0.1.1-IGa4j6xf5g9CDDPrEYmv9sq9O6ZaiWD5n2r370JWRhpmKd4gChpwwqY8wxbyy0uYMLsH.oXj3MvaSSZfIepFlck_3OxQr3aibo0JDHaJicI',
    'cf_clearance': 'ZlcHNgvlWxdHmhAMgEigq8QIKiqWJP4rb5iHa_qzKHM-1755944820-1.2.1.1-0aOE6AGWXD5XOUJNP0uj2rSz_a19TnWdeOcaZ8esY7CBAxAHNUYfcHik7.JTa0.dZYW2TD8Aw0Ow5_qE0MD2AbXu7vT4S8X_R8wyFAKQWCCZuyn7BwHFlWQ8i1hHivNHh_xqVd6NRDCr7jqU_5E3_bR1tRf4aI_PaffXAk2ORtp.pd7rUI7XPZ4PxIYzAr1SSLmPrfFOiFpVTmzMF7.fGxBRMzjPjH.KZp_DJd5LVuU',
    '_ga_0XM0LYXGC8': 'GS2.1.s1755944762$o2$g1$t1755944820$j2$l0$h0',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'baggage': 'sentry-environment=production,sentry-release=20250822-2877-33fd8eb,sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,sentry-trace_id=7e47c0e96f5e475ca1a3f7ce8b497955,sentry-sample_rate=0.01,sentry-sampled=false',
    'priority': 'u=1, i',
    'referer': 'https://gmgn.ai/sol/address/5wuAszhsuGmQCYSP7i9mxpxcfU15N7m6cqSiNnViqVLS',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"139.0.7258.139"',
    'sec-ch-ua-full-version-list': '"Not;A=Brand";v="99.0.0.0", "Google Chrome";v="139.0.7258.139", "Chromium";v="139.0.7258.139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': '7e47c0e96f5e475ca1a3f7ce8b497955-af469f3ce8259fad-0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    # 'cookie': '_ga=GA1.1.107301769.1755792444; __cf_bm=6KaTYk29CwPs3kZVthUXXuI9Wna2MvWKuzptoAir4Z4-1755944816-1.0.1.1-IGa4j6xf5g9CDDPrEYmv9sq9O6ZaiWD5n2r370JWRhpmKd4gChpwwqY8wxbyy0uYMLsH.oXj3MvaSSZfIepFlck_3OxQr3aibo0JDHaJicI; cf_clearance=ZlcHNgvlWxdHmhAMgEigq8QIKiqWJP4rb5iHa_qzKHM-1755944820-1.2.1.1-0aOE6AGWXD5XOUJNP0uj2rSz_a19TnWdeOcaZ8esY7CBAxAHNUYfcHik7.JTa0.dZYW2TD8Aw0Ow5_qE0MD2AbXu7vT4S8X_R8wyFAKQWCCZuyn7BwHFlWQ8i1hHivNHh_xqVd6NRDCr7jqU_5E3_bR1tRf4aI_PaffXAk2ORtp.pd7rUI7XPZ4PxIYzAr1SSLmPrfFOiFpVTmzMF7.fGxBRMzjPjH.KZp_DJd5LVuU; _ga_0XM0LYXGC8=GS2.1.s1755944762$o2$g1$t1755944820$j2$l0$h0',
}

params = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'fp_did': '77abb27885cffbec63c7f9fbd35b4116',
    'client_id': 'gmgn_web_20250822-2877-33fd8eb',
    'from_app': 'gmgn',
    'app_ver': '20250822-2877-33fd8eb',
    'tz_name': 'Europe/Moscow',
    'tz_offset': '10800',
    'app_lang': 'ru',
    'os': 'web',
    'limit': '50',
    'orderby': 'last_active_timestamp',
    'direction': 'desc',
    'showsmall': 'true',
    'sellout': 'true',
    'hide_airdrop': 'false',
    'tx30d': 'true',
}

response = requests.get(
    'https://gmgn.ai/api/v1/wallet_holdings/sol/5wuAszhsuGmQCYSP7i9mxpxcfU15N7m6cqSiNnViqVLS',
    params=params,
    cookies=cookies,
    headers=headers,
)