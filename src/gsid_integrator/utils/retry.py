import time
from functools import wraps

def with_retry(max_attempts=3, base_delay=1.0):
    def deco(fn):
        @wraps(fn)
        def _wrap(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    time.sleep(base_delay * (2 ** (attempt - 1)))
        return _wrap
    return deco
