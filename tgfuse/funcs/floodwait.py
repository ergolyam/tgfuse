import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from pyrogram.errors import FloodWait

from tgfuse.config import logging_config

log = logging_config.setup_logging(__name__)

T = TypeVar("T")


def flood_wait_seconds(exc: FloodWait) -> int:
    value = getattr(exc, "value", None)
    if value is None:
        value = getattr(exc, "x", None)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 1


async def sleep_for_flood_wait(exc: FloodWait, *, label: str = "") -> None:
    seconds = flood_wait_seconds(exc)
    suffix = f" during {label}" if label else ""
    log.warning("Telegram FloodWait%s: waiting %s seconds.", suffix, seconds)
    await asyncio.sleep(seconds)


async def retry_flood_wait(
    operation: Callable[[], Awaitable[T]],
    *,
    label: str,
    sleep_for_wait: Callable[[FloodWait], Awaitable[None]] | None = None,
) -> T:
    while True:
        try:
            return await operation()
        except FloodWait as exc:
            if sleep_for_wait is None:
                await sleep_for_flood_wait(exc, label=label)
            else:
                await sleep_for_wait(exc)
