from functools import wraps
from random import normalvariate
from asyncio import sleep

import logging


logger = logging.getLogger(__name__)


def backoff(
    start_delay: float = 0.2,
    factor: int = 2,
    border_delay: float = 10,
    retries: int = 10,
    jitter: float = 0.1,
    exceptions = (Exception, )
):
    def wrapper(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            current_delay = start_delay
            counter = 0

            while counter < retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    counter += 1
                    current_delay = min(start_delay * (factor ** counter), border_delay)
                    current_delay += max(normalvariate(mu=0, sigma=current_delay * jitter), 0)

                    logger.warning(
                        "Retry %s for %s due to %r, sleep %.2f s",
                        counter, func.__name__, e, current_delay
                    )

                    await sleep(current_delay)
            raise last_exc

        return inner
    return wrapper
