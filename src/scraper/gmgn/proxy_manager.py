from collections import deque
import threading
import time
from typing import Deque, List, Tuple

# общий логгер проекта
from src.sdk.infrastructure.logger import logger


class ProxyManager:
    """
    Пул прокси с обязательным «охлаждением».

    • cool_down_sec — сколько секунд IP обязан отдыхать, прежде чем его можно выдать снова.
    • acquire() блокируется, если все IP ещё остужаются, и ждёт, пока первый «созреет».
    """

    def __init__(self, proxy_list: List[str], *, cool_down_sec: int = 3000):
        self.cool_down_sec = cool_down_sec
        self.ready:   Deque[str]            = deque(proxy_list)     # свободны
        self.cooling: Deque[Tuple[str, float]] = deque()            # (ip, ready_at)
        self.in_use:  set[str]              = set()                 # заняты воркерами
        self.lock = threading.Lock()

        logger.info("[PM] init ready=%s", len(self.ready))

    # ─────────────────── internal ────────────────────
    def _recycle(self) -> None:
        """Перекинуть «созревшие» IP из cooling → ready."""
        now = time.time()
        moved = 0
        while self.cooling and self.cooling[0][1] <= now:
            ip = self.cooling.popleft()[0]
            if ip not in self.in_use:       # может, воркер уже успел взять
                self.ready.append(ip)
                moved += 1
        if moved:
            logger.debug("[PM] recycled %s IP → ready=%s", moved, len(self.ready))

    # ───────────────────── public API ────────────────────────
    def acquire(self) -> str:
        """
        Получить свободный IP.
        Блокируется, если все IP либо в cooling, либо уже выданы.
        """
        while True:
            with self.lock:
                self._recycle()

                # берём первый not-in-use из ready
                while self.ready:
                    ip = self.ready.popleft()
                    if ip not in self.in_use:
                        self.in_use.add(ip)
                        logger.debug("[PM] acquire → %s | in_use=%s ready=%s cooling=%s",
                                     ip, len(self.in_use), len(self.ready), len(self.cooling))
                        return ip
                # ничего готового
                wait = (self.cooling[0][1] - time.time()) if self.cooling else 0.2
                wait = max(0.05, min(wait, 1.0))

            # спим вне критической секции
            time.sleep(wait)

    def release(self, ip: str) -> None:
        """Отправить IP в очередь охлаждения."""
        with self.lock:
            if ip not in self.in_use:
                logger.warning("[PM] release(%s) not in use — пропускаю", ip)
                return
            self.in_use.remove(ip)

            # не кладём дубликат в cooling
            if any(ip == item[0] for item in self.cooling):
                logger.debug("[PM] %s уже в cooling", ip)
            else:
                self.cooling.append((ip, time.time() + self.cool_down_sec))
                logger.debug("[PM] release → %s | will be ready in %.0fs", ip, self.cool_down_sec)
