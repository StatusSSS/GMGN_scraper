from curl_cffi import requests as curl

def build_proxy_url(raw: str) -> str:
    """
    Превращает host:port или host:port:user:pass
    →  http://[user:pass@]host:port   для curl_cffi / libcurl.
    """
    parts = raw.split(":")
    if len(parts) == 2:               # без авторизации
        host, port = parts
        return f"http://{host}:{port}"
    if len(parts) == 4:               # с логином/паролем
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    raise ValueError("Неверный формат прокси")

def check_proxy(proxy_raw: str,
                test_url: str = "https://api.ipify.org?format=json",
                timeout: int = 10) -> bool:
    """
    Возвращает True, если прокси отвечает 200 OK и JSON можно прочитать.
    """
    proxy_url = build_proxy_url(proxy_raw)
    proxies = {"http": proxy_url, "https": proxy_url}

    try:
        resp = curl.get(test_url,
                        proxies=proxies,
                        impersonate="chrome120",
                        timeout=timeout)
        resp.raise_for_status()
        ip = resp.json().get("ip")
        print(f"✓ OK {proxy_raw} → внешний IP {ip}")
        return True
    except Exception as exc:
        print(f"✗ FAIL {proxy_raw}: {exc}")
        return False

if __name__ == "__main__":
    PROXY = "81.177.23.10:9448:Rte1x5:tmbgBn"
    check_proxy(PROXY)
