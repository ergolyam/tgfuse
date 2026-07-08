from pyrogram.client import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.enums import MessagesFilter
from tgfuse.config import logging_config
from tgfuse.funcs.floodwait import sleep_for_flood_wait, retry_flood_wait
from tgfuse.funcs.media import remote_file_from_message
log = logging_config.setup_logging(__name__)

async def gather_docs_bot(client: Client, chat_id: int) -> list:
    """
    For normal bots:
      - We can't use client.search_messages()
      - We can't reliably call client.get_chat_history() for everything
    So we iterate message IDs in chunks of 200 (the max that get_messages can fetch).
    We'll keep fetching until we repeatedly get "empty" sets (messages that don't exist).
    """
    all_docs = []
    chunk_size = 200
    
    empty_chunk_limit = 10
    empty_chunk_count = 0
    
    current_id = 1
    while True:
        chunk_ids = list(range(current_id, current_id + chunk_size))
        try:
            messages = await retry_flood_wait(
                lambda: client.get_messages(chat_id, chunk_ids),
                label=f"fetch bot messages {current_id}-{current_id + chunk_size - 1}",
            )
        except RPCError as exc:
            log.warning(f"Error while fetching messages in BOT mode: {exc}")
            break

        if not isinstance(messages, list):
            messages = [messages]
        
        found_any_messages = False
        for msg in messages:
            if not msg or msg.empty:
                continue
            found_any_messages = True
            remote_file = remote_file_from_message(msg)
            if remote_file:
                f_id, fname_b, size, t = remote_file
                all_docs.append((msg.id, f_id, fname_b, size, t))
        
        if not found_any_messages:
            empty_chunk_count += 1
        else:
            empty_chunk_count = 0
        
        if empty_chunk_count >= empty_chunk_limit:
            break
        
        current_id += chunk_size
    
    return all_docs


async def gather_docs_userbot(client: Client, chat_id: int) -> list:
    """
    For user accounts, we can simply call client.search_messages()
    with filter=DOCUMENT and iterate over all results.
    """
    while True:
        all_docs = []
        try:
            async for msg in client.search_messages(chat_id, filter=MessagesFilter.DOCUMENT):
                remote_file = remote_file_from_message(msg)
                if not remote_file:
                    continue

                f_id, fname_b, size, t = remote_file
                all_docs.append((msg.id, f_id, fname_b, size, t))
            return all_docs
        except FloodWait as exc:
            await sleep_for_flood_wait(exc, label="search userbot documents")

if __name__ == "__main__":
    raise RuntimeError("This module should be run only via main.py")
