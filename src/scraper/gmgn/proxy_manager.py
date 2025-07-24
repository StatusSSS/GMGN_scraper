from collections import deque
import threading
import time
from typing import Deque, List, Tuple


class ProxyManager:
    """Простой пул прокси с обязательным «охлаждением».

    • ``cool_down_sec`` — сколько секунд прокси должен отдыхать, прежде
      чем снова попасть в работу.
    • Метод :py:meth:`acquire` блокируется, если свободных IP нет, пока
      первый из охлаждающихся не «созреет». Таким образом, ни один прокси
      больше не будет возвращён раньше конца окна охлаждения.
    """

    def __init__(self, proxy_list: List[str], *, cool_down_sec: int = 3000):
        self.cool_down_sec = cool_down_sec  # 50 минут по умолчанию
        self.ready: Deque[str] = deque(proxy_list)  # доступные прямо сейчас
        self.cooling: Deque[Tuple[str, float]] = deque()  # (proxy, ready_at_ts)
        self.lock = threading.Lock()

    # ─────────────────────────── internals ────────────────────────────

    def _recycle(self) -> None:
        """Переместить все «отдохнувшие» прокси обратно в ready."""
        now = time.time()
        while self.cooling and self.cooling[0][1] <= now:
            self.ready.append(self.cooling.popleft()[0])

    # ────────────────────────── public API ────────────────────────────

    def acquire(self) -> str:
        """Получить свободный прокси.

        Блокируется (периодически проверяя очередь) до тех пор, пока не
        появится хотя бы один готовый IP. Это гарантирует, что прокси не
        будет выдан раньше завершения окна cool‑down.
        """
        while True:
            with self.lock:
                self._recycle()

                if self.ready:
                    return self.ready.popleft()

                # ближайшее время «созревания» первого в cooling
                wait = (
                    max(0.0, self.cooling[0][1] - time.time()) if self.cooling else 0.1
                )

            # ➟ спим уже вне критической секции (не держим lock)
            time.sleep(min(wait, 1.0))

    def release(self, proxy: str) -> None:
        """Отправить прокси в очередь охлаждения."""
        with self.lock:
            self.cooling.append((proxy, time.time() + self.cool_down_sec))
