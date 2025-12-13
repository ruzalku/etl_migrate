from functools import wraps
from random import normalvariate
from asyncio import sleep


def backoff(
    start_delay: float = 0.2,
    factor: int = 2,
    border_delay: float = 10,
    retries: int = 10,
    jitter: float = 0.1
):
    def wrapper(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            current_delay = start_delay
            counter = 0

            while counter <= retries:
                try:
                    return await func(*args, **kwargs)
                except:
                    count += 1
                    current_delay = min(start_delay * (factor ** counter), border_delay)
                    current_delay += normalvariate(mu=0, sigma=current_delay * jitter)

                    await sleep(current_delay)

        return inner
    return wrapper
