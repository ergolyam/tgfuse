import asyncio

from pyrogram import raw
from pyrogram.errors import FloodWait
from pyrogram.file_id import FileId
from pyrogram.session import Auth, Session

from tgfuse.config import logging_config
from tgfuse.funcs.floodwait import flood_wait_seconds

log = logging_config.setup_logging(__name__)


class RawDocumentDownloader:
    def __init__(self, client, file_id: str):
        self.client = client
        self.file_id = file_id
        self.decoded = FileId.decode(file_id)
        self.session = None
        self.location = raw.types.InputDocumentFileLocation(
            id=self.decoded.media_id,
            access_hash=self.decoded.access_hash,
            file_reference=self.decoded.file_reference,
            thumb_size=self.decoded.thumbnail_size,
        )

    async def start(self):
        if self.session is not None:
            return

        dc_id = self.decoded.dc_id
        main_dc_id = await self.client.storage.dc_id()
        auth_key = (
            await Auth(self.client, dc_id, await self.client.storage.test_mode()).create()
            if dc_id != main_dc_id
            else await self.client.storage.auth_key()
        )
        self.session = Session(
            self.client,
            dc_id,
            auth_key,
            await self.client.storage.test_mode(),
            is_media=True,
        )
        await self.session.start()

        if dc_id != main_dc_id:
            exported_auth = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
            await self.session.invoke(
                raw.functions.auth.ImportAuthorization(
                    id=exported_auth.id,
                    bytes=exported_auth.bytes,
                )
            )

    async def close(self):
        if self.session is not None:
            await self.session.stop()
            self.session = None

    async def read_chunk(
        self,
        chunk_index: int,
        *,
        chunk_size: int,
        timeout: float,
        retries: int,
    ) -> bytes:
        await self.start()
        offset = chunk_index * chunk_size
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                result = await self.session.invoke(
                    raw.functions.upload.GetFile(
                        location=self.location,
                        offset=offset,
                        limit=chunk_size,
                        precise=True,
                    ),
                    retries=1,
                    timeout=timeout,
                    sleep_threshold=30,
                )
            except FloodWait as exc:
                await asyncio.sleep(flood_wait_seconds(exc))
                last_error = exc
            except (TimeoutError, asyncio.TimeoutError) as exc:
                last_error = exc
                log.warning(
                    "Raw download timeout file_id=%s chunk=%s attempt=%s/%s",
                    self.file_id,
                    chunk_index,
                    attempt,
                    retries,
                )
            else:
                if isinstance(result, raw.types.upload.File):
                    return result.bytes
                raise RuntimeError(f"Unsupported Telegram download response: {type(result).__name__}")
        raise TimeoutError(f"Can't download chunk {chunk_index}") from last_error
