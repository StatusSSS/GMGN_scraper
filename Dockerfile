FROM python:3.11-slim

# Системные зависимости для Chrome
RUN apt-get update && \
    apt-get install -y \
      libnss3 libgtk-3-0 xvfb wget gnupg2 unzip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Копируем и устанавливаем Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ваш тест / скрипт
COPY . .



ENTRYPOINT ["python", "-m", "src.scraper.dexscreener.fetch_token_addresses"]
