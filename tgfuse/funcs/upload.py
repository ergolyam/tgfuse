import asyncio
import contextlib
import math
import time
from collections.abc import AsyncGenerator

import pyrogram.session.session
from pyrogram import raw, types, utils
from pyrogram.client import Client
from pyrogram.errors import FloodWait

from tgfuse.config import logging_config
from tgfuse.config.config import Config
from tgfuse.funcs.floodwait import flood_wait_seconds

log = logging_config.setup_logging(__name__)

MAX_UPLOAD_PART_SIZE = 512 * 1024
FAST_UPLOAD_MIN_SIZE = 10 * 1024 * 1024
DEFAULT_UPLOAD_WORKERS = 4
DEFAULT_UPLOAD_BUFFER_PARTS = 16


def _upload_parallelism(
    workers: int | None = None,
    buffer_parts: int | None = None,
) -> tuple[int, int]:
    if workers is None:
        workers = Config.tg_upload_workers
    if buffer_parts is None:
        buffer_parts = Config.tg_upload_buffer_parts
    workers = max(1, int(workers))
    buffer_parts = max(workers, int(buffer_parts))
    return workers, buffer_parts


class LocalFileParts:
    def __init__(
        self,
        path: str,
        *,
        name: str,
        total: int,
        cancel_event: asyncio.Event | None = None,
    ):
        self.path = path
        self.name = name
        self.total = int(total)
        self.cancel_event = cancel_event or asyncio.Event()

    def __aiter__(self) -> AsyncGenerator[bytes, None]:
        return self._stream()

    async def _read(self, fp, size: int) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, fp.read, size)

    async def _stream(self) -> AsyncGenerator[bytes, None]:
        remaining = self.total
        with open(self.path, "rb") as fp:
            while remaining > 0:
                if self.cancel_event.is_set():
                    raise asyncio.CancelledError()
                read_size = min(MAX_UPLOAD_PART_SIZE, remaining)
                chunk = await self._read(fp, read_size)
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk


async def _make_media_session(client: Client) -> pyrogram.session.session.Session:
    session = pyrogram.session.session.Session(
        client,
        await client.storage.dc_id(),
        await client.storage.auth_key(),
        await client.storage.test_mode(),
        is_media=True,
    )
    await session.start()
    return session


async def save_big_file_from_path(
    client: Client,
    path: str,
    *,
    file_name: str,
    file_size: int,
    workers: int | None = None,
    buffer_parts: int | None = None,
):
    workers, buffer_parts = _upload_parallelism(workers, buffer_parts)

    premium = bool(getattr(getattr(client, "me", None), "is_premium", False))
    file_size_limit_mib = 4000 if premium else 2000
    if file_size > file_size_limit_mib * 1024 * 1024:
        raise ValueError(f"Files larger than {file_size_limit_mib} MiB cannot be uploaded.")

    file_id = client.rnd_id()
    file_total_parts = math.ceil(file_size / MAX_UPLOAD_PART_SIZE)
    queue: asyncio.Queue[tuple[int, bytes] | None] = asyncio.Queue(maxsize=buffer_parts)
    cancel_event = asyncio.Event()
    parts = LocalFileParts(path, name=file_name, total=file_size, cancel_event=cancel_event)
    sessions = []
    uploaded_size = 0
    uploaded_lock = asyncio.Lock()
    next_log_at = 64 * 1024 * 1024
    started_at = time.monotonic()

    log.info(
        "Fast upload started name=%s size=%s workers=%s buffer_parts=%s",
        file_name,
        file_size,
        workers,
        buffer_parts,
    )

    async def producer():
        try:
            part_index = 0
            async for chunk in parts:
                if part_index + 1 != file_total_parts and len(chunk) % 1024 != 0:
                    raise ValueError("Telegram upload parts must be 1024-byte aligned.")
                await queue.put((part_index, chunk))
                part_index += 1
            for _ in range(workers):
                await queue.put(None)
        except Exception:
            cancel_event.set()
            for _ in range(workers):
                with contextlib.suppress(asyncio.QueueFull):
                    queue.put_nowait(None)
            raise

    async def worker(session: pyrogram.session.session.Session):
        nonlocal next_log_at, uploaded_size
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                part_index, chunk = item
                while True:
                    try:
                        await session.invoke(
                            raw.functions.upload.SaveBigFilePart(
                                file_id=file_id,
                                file_part=part_index,
                                file_total_parts=file_total_parts,
                                bytes=chunk,
                            )
                        )
                        break
                    except FloodWait as exc:
                        await asyncio.sleep(flood_wait_seconds(exc))
                async with uploaded_lock:
                    uploaded_size += len(chunk)
                    if uploaded_size >= next_log_at or uploaded_size == file_size:
                        elapsed = max(time.monotonic() - started_at, 1e-6)
                        log.info(
                            "Fast upload progress name=%s uploaded=%s/%s speed=%.1f MiB/s",
                            file_name,
                            uploaded_size,
                            file_size,
                            uploaded_size / elapsed / 1024 / 1024,
                        )
                        while next_log_at <= uploaded_size:
                            next_log_at += 64 * 1024 * 1024
            finally:
                queue.task_done()

    try:
        sessions = [await _make_media_session(client) for _ in range(workers)]
        producer_task = asyncio.create_task(producer())
        worker_tasks = [asyncio.create_task(worker(session)) for session in sessions]
        all_tasks = [producer_task, *worker_tasks]

        done, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            exc = task.exception()
            if exc is not None:
                cancel_event.set()
                for pending_task in pending:
                    pending_task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                raise exc

        await queue.join()
        await asyncio.gather(*worker_tasks)

        log.info(
            "Fast upload parts complete name=%s size=%s elapsed=%.2fs",
            file_name,
            file_size,
            time.monotonic() - started_at,
        )

        return raw.types.input_file_big.InputFileBig(
            id=file_id,
            parts=file_total_parts,
            name=file_name,
        )
    finally:
        cancel_event.set()
        for session in sessions:
            try:
                await session.stop()
            except Exception:
                pass


async def send_document_from_path(
    client: Client,
    chat_id: int,
    file_path: str,
    file_name: str,
    file_size: int,
):
    if file_size >= FAST_UPLOAD_MIN_SIZE:
        uploaded = await save_big_file_from_path(
            client,
            file_path,
            file_name=file_name,
            file_size=file_size,
        )
    else:
        log.info("Path upload started name=%s size=%s", file_name, file_size)
        uploaded = await client.save_file(file_path)

    log.info("Path upload sending media name=%s", file_name)
    media = raw.types.InputMediaUploadedDocument(
        mime_type=client.guess_mime_type(file_name) or "application/zip",
        file=uploaded,
        force_file=True,
        attributes=[raw.types.DocumentAttributeFilename(file_name=file_name)],
    )

    while True:
        try:
            result = await client.invoke(
                raw.functions.messages.SendMedia(
                    peer=await client.resolve_peer(chat_id),
                    media=media,
                    message="",
                    random_id=client.rnd_id(),
                )
            )
            break
        except FloodWait as exc:
            await asyncio.sleep(flood_wait_seconds(exc))

    for update in result.updates:
        if isinstance(
            update,
            (
                raw.types.UpdateNewMessage,
                raw.types.UpdateNewChannelMessage,
                raw.types.UpdateNewScheduledMessage,
                raw.types.UpdateBotNewBusinessMessage,
            ),
        ):
            return await types.Message._parse(
                client,
                update.message,
                {user.id: user for user in result.users},
                {chat.id: chat for chat in result.chats},
                is_scheduled=isinstance(update, raw.types.UpdateNewScheduledMessage),
            )
    log.warning("Fast upload SendMedia returned no message update name=%s", file_name)
    return None
