from collections import deque
import threading, time

class ProxyManager:
    def __init__(self, proxy_list, *, cool_down_sec=900):
        self.cool_down_sec = cool_down_sec
        self.ready   = deque(proxy_list)
        self.cooling = deque()
        self.lock    = threading.Lock()

    def _recycle(self):
        now = time.time()
        while self.cooling and self.cooling[0][1] <= now:
            self.ready.append(self.cooling.popleft()[0])

    def acquire(self) -> str:
        with self.lock:
            self._recycle()
            if not self.ready:
                proxy, _ = self.cooling.popleft()
            else:
                proxy = self.ready.popleft()
            return proxy

    def release(self, proxy: str):
        with self.lock:
            self.cooling.append((proxy, time.time() + self.cool_down_sec))
