#!/usr/bin/env python
"""
Открывает https://gmgn.ai/?chain=sol&… через прокси 81.177.23.10:9448
(без логина-пароля), ждёт 20 с на ручное прохождение капчи и выводит куки.
pip install selenium==4.* webdriver-manager
"""
import json, time, uuid
from urllib.parse import urlencode, urljoin

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ─── статические заголовки и query-параметры ────────────────────────
EXTRA_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'baggage': ('sentry-environment=production,sentry-release=20250806-2043-a5aa1db,'
                'sentry-public_key=93c25bab7246077dc3eb85b59d6e7d40,'
                'sentry-trace_id=46f49e31e4544b209735bc18c8541097,'
                'sentry-sample_rate=0.01,sentry-sampled=false'),
    'priority': 'u=1, i',
    'referer': ('https://gmgn.ai/sol/address/'
                '43DbKtri_H7qmc5L14Az3dDFwzYEmLPurcZ676m2g4Sd4GMb3nGsJ'),
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"138.0.7204.184"',
    'sec-ch-ua-full-version-list': ('"Not)A;Brand";v="8.0.0.0", '
                                    '"Chromium";v="138.0.7204.184", '
                                    '"Google Chrome";v="138.0.7204.184"'),
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': '46f49e31e4544b209735bc18c8541097-bb65c0969616e8f7-0',
}
QUERY_PARMS = {
    'device_id': 'c45e37f7-53ff-4d68-813b-fd0f7b736979',
    'client_id': 'gmgn_web_20250806-2043-a5aa1db',
    'from_app': 'gmgn',
    'app_ver': '20250806-2043-a5aa1db',
    'tz_name': 'Europe/Moscow',
    'tz_offset': '10800',
    'app_lang': 'ru',
    'fp_did': '77abb27885cffbec63c7f9fbd35b4116',
    'os': 'web',
    'period': '7d',
}

# ─── прокси без авторизации ─────────────────────────────────────────
PROXY = "81.177.23.10:9448"
USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
              'AppleWebKit/537.36 (KHTML, like Gecko) '
              'Chrome/138.0.0.0 Safari/537.36')

# ─── Chrome с прокси ────────────────────────────────────────────────
opts = Options()
opts.add_argument(f"--proxy-server=http://{PROXY}")
opts.add_argument(f"--user-agent={USER_AGENT}")
opts.add_argument("--disable-blink-features=AutomationControlled")

caps = webdriver.DesiredCapabilities.CHROME.copy()
caps['goog:loggingPrefs'] = {'performance': 'ALL'}

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=opts,
    desired_capabilities=caps
)

try:
    # CDP: задаём UA и дополнительные хедеры
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd("Network.setUserAgentOverride",
                           {"userAgent": USER_AGENT})
    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders",
                           {"headers": EXTRA_HEADERS})

    url = urljoin("https://gmgn.ai/", "?chain=sol&" + urlencode(QUERY_PARMS))
    driver.get(url)

    print("⏳ 20 s — решайте капчу…")
    time.sleep(20)

    # убеждаемся, что есть sid (или ставим свой)
    if not any(c['name'] == 'sid' for c in driver.get_cookies()):
        sid = f"gmgn|{uuid.uuid4().hex}"
        driver.add_cookie({"name": "sid", "value": sid,
                           "domain": ".gmgn.ai", "path": "/"})
        print(f"[SID] custom sid set → {sid}")

    # выводим куки
    print("\n--- Cookies ---")
    for c in driver.get_cookies():
        print(f"{c['name']} = {c['value']}")

finally:
    driver.quit()
