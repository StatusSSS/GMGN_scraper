#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Проверка двух вариантов одного и того же прокси:
1) без логина/пароля
2) с логином/паролем
"""

import requests
from requests.exceptions import RequestException, Timeout

TEST_URL = "https://httpbin.org/ip"   # сервис, который возвращает ваш внешний IP
TIMEOUT  = 5                          # секунд на отклик

# ✏️ ваши два варианта прокси
RAW_PROXIES = [
    "45.80.105.92:8080",
    "45.80.105.92:8080:mix11L53WGVK:paIVpvlm",
]

def raw_to_url(raw: str) -> str:
    """
    Превращает строку 'host:port' или 'host:port:user:pass' в URL вида
    'http://host:port' либо 'http://user:pass@host:port'
    """
    parts = raw.split(":")
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    elif len(parts) == 4:
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    else:
        raise ValueError(f"Неизвестный формат строки прокси: {raw}")

def check_proxy(proxy_url: str) -> tuple[bool, str]:
    """
    Пытается сходить на TEST_URL через proxy_url.
    :return: (True, json|ip) если OK, иначе (False, причина)
    """
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        resp = requests.get(TEST_URL, proxies=proxies, timeout=TIMEOUT)
        resp.raise_for_status()
        return True, resp.text.strip()
    except (RequestException, Timeout) as err:
        return False, str(err)

def main() -> None:
    print(f"Тестируем {len(RAW_PROXIES)} варианта(ов)…\n")
    for raw in RAW_PROXIES:
        proxy_url = raw_to_url(raw)
        ok, info  = check_proxy(proxy_url)
        status    = "✅ OK" if ok else "❌ FAIL"
        print(f"{status:8} {raw:<45} → {info}")

if __name__ == "__main__":
    main()
