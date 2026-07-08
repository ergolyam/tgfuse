import asyncio
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tgfuse.config.config import Config
from tgfuse.config import logging_config
log = logging_config.setup_logging(__name__)

log.info(f"Script initialization, logging level: {Config.log_level}")

async def main():
    from tgfuse.core.tg import start_bot
    await start_bot()

def run():
    if not Config.tg_id or not Config.tg_hash:
        log.error("Please set TG_ID and TG_HASH environment variables or .env values.")
        raise SystemExit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Received Ctrl+C - exiting.")

if __name__ == "__main__":
    run()
