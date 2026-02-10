import time

def with_retries(fn, *, max_retries, backoff, logger):
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(backoff)
