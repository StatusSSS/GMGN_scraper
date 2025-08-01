#!/bin/sh
set -eu

# стартуем tinyproxy в фоне
tinyproxy -c /etc/tinyproxy/tinyproxy.conf
echo "[tinyproxy] started on :8888 (pid $(pidof tinyproxy))"

# запускаем Python-скрипт
exec python -m src.test_cap
