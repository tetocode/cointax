import time


class RateLimiter:
    def __init__(self, request_per_seconds_limit: float, fn):
        self._last_called_at = 0
        self._min_interval = 1 / request_per_seconds_limit
        self._fn = fn

    def wait(self):
        now = time.time()
        elapsed = now - self._last_called_at
        wait_seconds = self._min_interval - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def __call__(self, *args, **kwargs):
        self.wait()
        try:
            return self._fn(*args, **kwargs)
        finally:
            self._last_called_at = time.time()
