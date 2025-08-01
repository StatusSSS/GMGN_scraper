#!/bin/sh
set -eu


tinyproxy -d -c /etc/tinyproxy/tinyproxy.conf &
echo "[tinyproxy] started on :8888 (pid $(pidof tinyproxy))"


exec python -m src.test_cap