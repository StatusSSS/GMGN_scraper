from collections import deque
import threading
import time
from typing import Deque, List, Tuple


class ProxyManager:
    """Пул прокси с обязательным «охлаждением».

    • ``cool_down_sec`` — сколько секунд IP обязан отдыхать,
      прежде чем его можно выдать снова.
    • acquire() блокируется, если все IP ещё остужаются,
      и ждёт, пока первый «созреет».
    """

    def __init__(self, proxy_list: List[str], *, cool_down_sec: int = 3000):
        # 50 минут по умолчанию: 3000 с = 50 мин
        self.cool_down_sec = cool_down_sec
        self.ready: Deque[str] = deque(proxy_list)          # доступные сразу
        self.cooling: Deque[Tuple[str, float]] = deque()    # (ip, ready_at)
        self.lock = threading.Lock()

    # ─────────────────── internal helpers ────────────────────
    def _recycle(self) -> None:
        """Возвратить «созревшие» IP из cooling → ready."""
        now = time.time()
        while self.cooling and self.cooling[0][1] <= now:
            self.ready.append(self.cooling.popleft()[0])

    # ───────────────────── public API ────────────────────────
    def acquire(self) -> str:
        """Получить свободный IP. Ждёт, пока освободится хотя бы один."""
        while True:
            with self.lock:
                self._recycle()
                if self.ready:
                    return self.ready.popleft()

                # время, когда «созреет» ближайший IP
                wait = max(0.0, self.cooling[0][1] - time.time()) if self.cooling else 0.1

            # спим вне критической секции
            time.sleep(min(wait, 1.0))

    def release(self, proxy: str) -> None:
        """Отправить IP в очередь охлаждения."""
        with self.lock:
            self.cooling.append((proxy, time.time() + self.cool_down_sec))
