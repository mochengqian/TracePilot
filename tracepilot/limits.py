"""One deadline shared by the runtime, model adapters, MCP, and retries."""

import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Callable, Iterator

from tenacity import RetryCallState


class DeadlineExceeded(TimeoutError):
    pass


_deadline: ContextVar[tuple[float, Callable[[], float]] | None] = ContextVar("tracepilot_deadline", default=None)


@contextmanager
def request_deadline(deadline: float, clock: Callable[[], float] = time.monotonic) -> Iterator[None]:
    token = _deadline.set((deadline, clock))
    try:
        yield
    finally:
        _deadline.reset(token)


def remaining_timeout(configured: float) -> float:
    limit = _deadline.get()
    remaining = configured if limit is None else min(configured, limit[0] - limit[1]())
    if remaining <= 0:
        raise DeadlineExceeded("Investigation deadline exhausted")
    return remaining


def check_retry_deadline(state: RetryCallState) -> None:
    limit = _deadline.get()
    if limit is not None and (state.upcoming_sleep + limit[1]()) >= limit[0]:
        raise DeadlineExceeded("Retry cannot fit within the investigation deadline")
