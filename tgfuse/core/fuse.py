import asyncio
import contextlib
import errno
import os
import shutil
import stat
import tempfile
import time
import uuid
from collections import OrderedDict
from typing import Sequence, Tuple

import pyfuse3
import pyfuse3.asyncio
from pyrogram.errors import RPCError
from pyfuse3 import EntryAttributes, FileInfo, FUSEError, ROOT_INODE

from tgfuse.config import logging_config
from tgfuse.funcs.channel import gather_all_docs
from tgfuse.funcs.download import RawDocumentDownloader
from tgfuse.funcs.floodwait import sleep_for_flood_wait, retry_flood_wait
from tgfuse.funcs.media import (
    DIRECTORY_MARKER_NAME,
    ROOT_DIRECTORY_ID,
    SYMLINK_MARKER_NAME,
    build_directory_caption,
    build_file_caption,
    build_symlink_caption,
    remote_entry_from_message,
    remote_file_from_message,
)
from tgfuse.funcs.upload import send_document_from_path

pyfuse3.asyncio.enable()

log = logging_config.setup_logging(__name__)


class TelegramFS(pyfuse3.Operations):
    _REMOTE_CHUNK_SIZE = 1024 * 1024
    _STREAM_BUFFER_LIMIT = 64 * 1024 * 1024
    _PREFETCH_CHUNKS = 64
    _REMOTE_READ_TIMEOUT = 30
    _REMOTE_READ_RETRIES = 3

    def __init__(self, client, chat_id: int, read_only: bool):
        super().__init__()
        self._tg_client = client
        self._chat_id = chat_id
        self.read_only = read_only

        self.enable_writeback_cache = False
        self.supports_dot_lookup = False

        self._root_inode = ROOT_INODE
        self._next_inode = 2

        self._name_to_inode: dict[tuple[int, bytes], int] = {}
        self._directory_id_to_inode: dict[str, int] = {}
        self._msg_id_to_inode: dict[int, int] = {}
        self._suppressed_msg_ids: set[int] = set()
        self._pending_remote_delete_msg_ids: set[int] = set()
        self._files: dict[int, dict] = {}

        self._delayed_upload_tasks: dict[int, asyncio.Task] = {}
        self._active_upload_tasks: dict[int, set[asyncio.Task]] = {}
        self._remote_delete_tasks: set[asyncio.Task] = set()
        self._sync_task = None

        self._fh_to_inode: dict[int, int] = {}
        self._next_fh = 1

        self._spool_dir = tempfile.mkdtemp(prefix="tgfuse-", dir=tempfile.gettempdir())
        self._inode_locks: dict[int, asyncio.Lock] = {}
        self._stream_buffer: OrderedDict[tuple[str, int], bytes] = OrderedDict()
        self._stream_buffer_bytes = 0
        self._stream_inflight: dict[tuple[str, int], asyncio.Task] = {}
        self._prefetch_tasks: dict[str, asyncio.Task] = {}
        self._prefetch_ranges: dict[str, tuple[int, int]] = {}
        self._downloaders: OrderedDict[str, RawDocumentDownloader] = OrderedDict()
        self._downloaders_limit = 8

    async def init_fs(self):
        """Gather initial docs, then start periodic sync."""
        await self._sync_initial_docs()
        self._sync_task = asyncio.create_task(self._periodic_sync_task())

    async def destroy(self):
        """Called on unmount => stop background tasks and best-effort flush dirty files."""
        pending_upload_inodes = set()
        if self._sync_task:
            self._sync_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sync_task

        for inode, task in list(self._delayed_upload_tasks.items()):
            task.cancel()
            done, pending = await asyncio.wait({task}, timeout=5)
            if pending:
                pending_upload_inodes.add(inode)
                log.warning("Upload task inode=%s did not cancel during unmount.", inode)
            for done_task in done:
                with contextlib.suppress(asyncio.CancelledError):
                    await done_task
            self._delayed_upload_tasks.pop(inode, None)

        for inode in list(self._active_upload_tasks):
            pending_upload_inodes.add(inode)
            await self._cancel_active_upload_tasks(inode)

        for task in list(self._remote_delete_tasks):
            done, pending = await asyncio.wait({task}, timeout=5)
            if pending:
                log.warning("Remote delete task did not finish during unmount.")
            for done_task in done:
                with contextlib.suppress(Exception):
                    await done_task

        stream_tasks = [*self._prefetch_tasks.values(), *self._stream_inflight.values()]
        for task in stream_tasks:
            task.cancel()
        if stream_tasks:
            done, pending = await asyncio.wait(set(stream_tasks), timeout=5)
            if pending:
                log.warning("Streaming read task did not cancel during unmount.")
            for done_task in done:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await done_task
        self._prefetch_tasks.clear()
        self._prefetch_ranges.clear()
        self._stream_inflight.clear()
        self._stream_buffer.clear()
        self._stream_buffer_bytes = 0

        for inode in list(self._files):
            info = self._files.get(inode)
            if (
                info
                and inode not in pending_upload_inodes
                and info.get("dirty")
                and not self.read_only
                and not info.get("read_only", False)
            ):
                with contextlib.suppress(Exception):
                    await self._commit_inode(inode)

        for downloader in list(self._downloaders.values()):
            with contextlib.suppress(Exception):
                await downloader.close()
        self._downloaders.clear()

        shutil.rmtree(self._spool_dir, ignore_errors=True)
        log.info("destroy() done - FS unmounted.")

    async def _sync_initial_docs(self):
        log.info("Initial sync: gather existing docs from channel...")
        docs = [self._normalize_remote_doc(doc) for doc in await gather_all_docs(
            self._tg_client, self._chat_id
        )]
        self._add_remote_docs(docs)

        log.info("Initial sync done, loaded %s entries.", len(self._files))

    def _normalize_remote_doc(self, doc) -> tuple:
        if len(doc) == 5:
            return (*doc, None)
        return tuple(doc)

    def _add_remote_docs(self, docs: list[tuple]):
        directory_docs = self._newest_directory_docs(docs)
        for doc in docs:
            metadata = doc[5]
            if (
                metadata
                and metadata.get("kind") == "directory"
                and directory_docs.get(metadata["directory_id"]) != doc
            ):
                self._suppressed_msg_ids.add(doc[0])
        pending = list(directory_docs.values())
        while pending:
            added = []
            for doc in pending:
                metadata = doc[5]
                if (
                    metadata["parent_id"] == ROOT_DIRECTORY_ID
                    or metadata["parent_id"] in self._directory_id_to_inode
                ):
                    self._add_remote_doc(doc)
                    added.append(doc)
            if not added:
                for doc in pending:
                    log.warning(
                        "Directory %s has a missing or cyclic parent; exposing it in root.",
                        doc[5]["directory_id"],
                    )
                    doc = (*doc[:5], {**doc[5], "parent_id": ROOT_DIRECTORY_ID})
                    self._add_remote_doc(doc)
                break
            pending = [doc for doc in pending if doc not in added]

        for doc in docs:
            metadata = doc[5]
            if metadata and metadata.get("kind") == "directory":
                continue
            self._add_remote_doc(doc)

    def _newest_directory_docs(self, docs: list) -> dict[str, tuple]:
        newest = {}
        for doc in docs:
            metadata = doc[5]
            if not metadata or metadata.get("kind") != "directory":
                continue
            directory_id = metadata["directory_id"]
            if directory_id not in newest or doc[0] > newest[directory_id][0]:
                newest[directory_id] = doc
        return newest

    def _parent_inode_for_id(self, directory_id: str) -> int:
        if directory_id == ROOT_DIRECTORY_ID:
            return self._root_inode
        return self._directory_id_to_inode.get(directory_id, self._root_inode)

    def _directory_id_for_parent(self, parent_inode: int) -> str:
        if parent_inode == self._root_inode:
            return ROOT_DIRECTORY_ID
        info = self._files.get(parent_inode)
        if not info or info.get("kind") != "directory":
            raise FUSEError(errno.ENOTDIR)
        return info["directory_id"]

    def _add_remote_doc(self, doc):
        m_id, f_id, fname_b, size, ts, metadata = doc
        if metadata and metadata.get("kind") == "directory":
            directory_id = metadata["directory_id"]
            existing_inode = self._directory_id_to_inode.get(directory_id)
            if existing_inode is not None:
                info = self._files[existing_inode]
                if m_id <= (info.get("message_id") or 0):
                    return
                old_key = (info["parent_inode"], info["file_name"])
                parent_inode = self._parent_inode_for_id(metadata["parent_id"])
                self._name_to_inode.pop(old_key, None)
                name = self._unique_file_name(parent_inode, metadata["name"])
                old_msg_id = info.get("message_id")
                if old_msg_id:
                    self._msg_id_to_inode.pop(old_msg_id, None)
                    self._suppressed_msg_ids.add(old_msg_id)
                info.update(
                    message_id=m_id,
                    file_id=f_id,
                    file_name=name,
                    parent_inode=parent_inode,
                    timestamp=ts,
                )
                self._name_to_inode[(parent_inode, name)] = existing_inode
                self._msg_id_to_inode[m_id] = existing_inode
                return

            parent_inode = self._parent_inode_for_id(metadata["parent_id"])
            name = self._unique_file_name(parent_inode, metadata["name"])
            inode = self._next_inode
            self._next_inode += 1
            self._files[inode] = self._new_directory_info(
                message_id=m_id,
                file_id=f_id,
                file_name=name,
                parent_inode=parent_inode,
                directory_id=directory_id,
                timestamp=ts,
            )
            self._directory_id_to_inode[directory_id] = inode
        elif metadata and metadata.get("kind") == "symlink":
            parent_inode = self._parent_inode_for_id(metadata["parent_id"])
            name = self._unique_file_name(parent_inode, metadata["name"])
            inode = self._next_inode
            self._next_inode += 1
            self._files[inode] = self._new_symlink_info(
                message_id=m_id,
                file_id=f_id,
                file_name=name,
                parent_inode=parent_inode,
                target=metadata["target"],
                timestamp=ts,
            )
        else:
            parent_id = metadata["parent_id"] if metadata else ROOT_DIRECTORY_ID
            parent_inode = self._parent_inode_for_id(parent_id)
            name = self._unique_file_name(parent_inode, fname_b)
            inode = self._next_inode
            self._next_inode += 1
            self._files[inode] = self._new_file_info(
                message_id=m_id,
                file_id=f_id,
                file_name=name,
                parent_inode=parent_inode,
                size=size,
                timestamp=ts,
            )
        self._name_to_inode[(parent_inode, name)] = inode
        self._msg_id_to_inode[m_id] = inode

    async def _periodic_sync_task(self):
        """Runs every 30s, checks for new/removed docs in the channel."""
        while True:
            try:
                await asyncio.sleep(30)
                await self._sync_channel_updates()
            except asyncio.CancelledError:
                log.info("Background sync task cancelled.")
                return
            except Exception as e:
                log.exception("Periodic sync task error: %s", e)

    def _remote_doc_from_message(self, msg):
        return remote_file_from_message(msg)

    def _remote_entry_from_message(self, msg):
        return remote_entry_from_message(msg)

    async def _fetch_remote_doc_by_msg_id(self, msg_id: int):
        msg = await self._retry_flood_wait(
            f"fetch known msg_id={msg_id}",
            lambda: self._tg_client.get_messages(self._chat_id, msg_id),
        )
        return self._remote_entry_from_message(msg)

    async def _sync_channel_updates(self):
        """Add new docs & remove missing docs from local state."""
        log.debug("Syncing channel updates...")
        docs = [self._normalize_remote_doc(doc) for doc in await gather_all_docs(
            self._tg_client, self._chat_id
        )]
        current_msgs = {}
        seen_msg_ids = set()
        for (m_id, f_id, fname_b, size, ts, metadata) in docs:
            seen_msg_ids.add(m_id)
            if m_id in self._suppressed_msg_ids:
                log.debug("Ignoring locally deleted stale msg_id=%s from channel sync.", m_id)
                continue
            current_msgs[m_id] = (f_id, fname_b, size, ts, metadata)
        self._suppressed_msg_ids.intersection_update(
            seen_msg_ids | self._pending_remote_delete_msg_ids
        )

        old_msg_ids = set(self._msg_id_to_inode.keys())
        new_msg_ids = set(current_msgs.keys())

        for msg_id in sorted(old_msg_ids - new_msg_ids):
            doc = await self._fetch_remote_doc_by_msg_id(msg_id)
            if not doc:
                continue
            current_msgs[msg_id] = doc
            new_msg_ids.add(msg_id)
            log.debug("Kept known msg_id=%s after direct sync check.", msg_id)

        missing_msg_ids = sorted(
            old_msg_ids - new_msg_ids,
            key=lambda msg_id: (
                self._files.get(self._msg_id_to_inode[msg_id], {}).get("kind")
                == "directory",
                -self._inode_depth(self._msg_id_to_inode[msg_id]),
            ),
        )
        for msg_id in missing_msg_ids:
            inode = self._msg_id_to_inode[msg_id]
            info = self._files.get(inode)
            if not info:
                continue
            if info.get("refcount", 0) > 0 or info.get("dirty"):
                log.debug("Skipping removal inode=%s, msg_id=%s because busy.", inode, msg_id)
                continue
            fname = info["file_name"]
            key = (info["parent_inode"], fname)
            if info.get("kind") == "directory" and self._directory_not_empty(inode):
                self._msg_id_to_inode.pop(msg_id, None)
                info["message_id"] = None
                info["file_id"] = None
                log.warning("Directory marker removed for non-empty inode=%s.", inode)
                continue
            log.info("Doc removed => inode=%s name=%s.", inode, fname)
            self._files.pop(inode, None)
            self._name_to_inode.pop(key, None)
            if info.get("kind") == "directory":
                self._directory_id_to_inode.pop(info["directory_id"], None)
            self._msg_id_to_inode.pop(msg_id, None)

        new_docs = [
            (msg_id, *current_msgs[msg_id]) for msg_id in new_msg_ids - old_msg_ids
        ]
        self._add_remote_docs(new_docs)
        for doc in new_docs:
            log.info("New remote entry msg_id=%s", doc[0])

        log.debug("Channel sync complete.")

    def _new_file_info(
        self,
        *,
        message_id: int | None,
        file_id: str | None,
        file_name: bytes,
        parent_inode: int,
        size: int,
        timestamp: int | None = None,
        refcount: int = 0,
    ) -> dict:
        return {
            "message_id": message_id,
            "kind": "file",
            "file_id": file_id,
            "file_name": file_name,
            "parent_inode": parent_inode,
            "size": size,
            "timestamp": int(time.time()) if timestamp is None else timestamp,
            "data": bytearray(),
            "dirty": False,
            "change_id": 0,
            "refcount": refcount,
            "read_only": False,
            "spool_path": None,
            "pending_delete_message_ids": set(),
            "unlinked": False,
            "mode": 0o644,
        }

    def _new_directory_info(
        self,
        *,
        message_id: int | None,
        file_id: str | None,
        file_name: bytes,
        parent_inode: int,
        directory_id: str,
        timestamp: int | None = None,
    ) -> dict:
        return {
            "message_id": message_id,
            "kind": "directory",
            "file_id": file_id,
            "file_name": file_name,
            "parent_inode": parent_inode,
            "directory_id": directory_id,
            "size": 0,
            "timestamp": int(time.time()) if timestamp is None else timestamp,
            "dirty": False,
            "refcount": 0,
            "read_only": False,
            "pending_delete_message_ids": set(),
            "unlinked": False,
            "mode": 0o755,
        }

    def _new_symlink_info(
        self,
        *,
        message_id: int | None,
        file_id: str | None,
        file_name: bytes,
        parent_inode: int,
        target: bytes,
        timestamp: int | None = None,
    ) -> dict:
        return {
            "message_id": message_id,
            "kind": "symlink",
            "file_id": file_id,
            "file_name": file_name,
            "parent_inode": parent_inode,
            "target": target,
            "size": len(target),
            "timestamp": int(time.time()) if timestamp is None else timestamp,
            "dirty": False,
            "refcount": 0,
            "read_only": False,
            "pending_delete_message_ids": set(),
            "unlinked": False,
            "mode": 0o777,
        }

    def _unique_file_name(self, parent_inode: int, fname: bytes) -> bytes:
        """If conflict, append _2, _3, etc."""
        base = fname
        idx = 2
        while (parent_inode, fname) in self._name_to_inode:
            fname = base + f"_{idx}".encode("utf-8")
            idx += 1
        return fname

    def _directory_not_empty(self, inode: int) -> bool:
        return any(parent_inode == inode for parent_inode, _ in self._name_to_inode)

    def _inode_depth(self, inode: int) -> int:
        depth = 0
        info = self._files.get(inode)
        while info and info.get("parent_inode") != self._root_inode:
            depth += 1
            info = self._files.get(info["parent_inode"])
        return depth

    def _is_temp_name(self, name: bytes) -> bool:
        return (
            name.startswith(b".goutputstream-")
            or name.startswith(b".#")
            or name.endswith(b"~")
            or name.endswith(b".swp")
            or name.endswith(b".swx")
            or name == b"4913"
        )

    def _lock_for(self, inode: int) -> asyncio.Lock:
        lock = self._inode_locks.get(inode)
        if lock is None:
            lock = asyncio.Lock()
            self._inode_locks[inode] = lock
        return lock

    def _file_name_text(self, info: dict) -> str:
        return info["file_name"].decode("utf-8", "replace")

    def _new_spool_path(self) -> str:
        fd, path = tempfile.mkstemp(prefix="file-", dir=self._spool_dir)
        os.close(fd)
        return path

    def _remove_spool(self, info: dict):
        path = info.get("spool_path")
        if path:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(path)
            info["spool_path"] = None

    def _remember_pending_delete(self, info: dict, msg_id: int | None):
        if msg_id:
            info.setdefault("pending_delete_message_ids", set()).add(msg_id)

    def _mark_dirty(self, info: dict):
        info["dirty"] = True
        info["change_id"] = info.get("change_id", 0) + 1
        info["timestamp"] = int(time.time())

    async def _sleep_for_flood_wait(self, exc):
        await sleep_for_flood_wait(exc)

    async def _retry_flood_wait(self, label: str, operation):
        return await retry_flood_wait(
            operation,
            label=label,
            sleep_for_wait=self._sleep_for_flood_wait,
        )

    async def _copy_remote_to_path(self, file_id: str, path: str, label: str):
        async def copy_remote_once():
            with open(path, "wb") as out:
                async for chunk in self._tg_client.stream_media(file_id):
                    out.write(chunk)

        await self._retry_flood_wait(label, copy_remote_once)

    async def _ensure_spool(self, inode: int, *, copy_remote: bool) -> str:
        info = self._files[inode]
        path = info.get("spool_path")
        if path:
            return path

        path = self._new_spool_path()
        info["spool_path"] = path

        if copy_remote and info.get("file_id") and info.get("size", 0) > 0:
            log.debug("Spooling remote file inode=%s for modification.", inode)
            await self._copy_remote_to_path(info["file_id"], path, f"spool inode={inode}")
        return path

    async def _truncate_inode(self, inode: int, size: int):
        async with self._lock_for(inode):
            info = self._files[inode]
            if self.read_only or info.get("read_only", False):
                raise FUSEError(errno.EROFS)

            copy_remote = bool(size and info.get("file_id") and not info.get("dirty"))
            path = await self._ensure_spool(inode, copy_remote=copy_remote)
            self._remember_pending_delete(info, info.get("message_id"))
            with open(path, "r+b") as fp:
                fp.truncate(size)
            info["size"] = size
            self._mark_dirty(info)

    async def _read_spool(self, info: dict, offset: int, size: int) -> bytes:
        path = info.get("spool_path")
        if not path or size <= 0:
            return b""
        with open(path, "rb") as fp:
            fp.seek(offset)
            return fp.read(size)

    def _stream_buffer_get(self, key: tuple[str, int]) -> bytes | None:
        chunk = self._stream_buffer.get(key)
        if chunk is not None:
            self._stream_buffer.move_to_end(key)
        return chunk

    def _stream_buffer_put(self, key: tuple[str, int], chunk: bytes):
        old = self._stream_buffer.pop(key, None)
        if old is not None:
            self._stream_buffer_bytes -= len(old)

        self._stream_buffer[key] = chunk
        self._stream_buffer_bytes += len(chunk)
        self._stream_buffer.move_to_end(key)

        while self._stream_buffer_bytes > self._STREAM_BUFFER_LIMIT and self._stream_buffer:
            _, evicted = self._stream_buffer.popitem(last=False)
            self._stream_buffer_bytes -= len(evicted)

    def _drop_stream_buffer_range(self, file_id: str, start_chunk: int, end_chunk: int):
        for chunk_index in range(start_chunk, end_chunk + 1):
            key = (file_id, chunk_index)
            chunk = self._stream_buffer.pop(key, None)
            if chunk is not None:
                self._stream_buffer_bytes -= len(chunk)

    def _finish_stream_chunk_task(self, key: tuple[str, int], task: asyncio.Task):
        if self._stream_inflight.get(key) is task:
            self._stream_inflight.pop(key, None)
        if task.cancelled():
            return
        try:
            chunk = task.result()
        except Exception:
            return
        if chunk:
            self._stream_buffer_put(key, chunk)

    def _start_stream_chunk_task(self, file_id: str, chunk_index: int) -> asyncio.Task:
        key = (file_id, chunk_index)
        task = self._stream_inflight.get(key)
        if task is not None:
            return task

        task = asyncio.create_task(self._download_remote_chunk(file_id, chunk_index))
        self._stream_inflight[key] = task
        task.add_done_callback(lambda done_task: self._finish_stream_chunk_task(key, done_task))
        return task

    async def _download_remote_chunk(self, file_id: str, chunk_index: int) -> bytes:
        async def read_chunk_once():
            if hasattr(self._tg_client, "storage"):
                downloader = await self._downloader_for(file_id)
                return await downloader.read_chunk(
                    chunk_index,
                    chunk_size=self._REMOTE_CHUNK_SIZE,
                    timeout=self._REMOTE_READ_TIMEOUT,
                    retries=self._REMOTE_READ_RETRIES,
                )

            chunk = b""
            async for part in self._tg_client.stream_media(
                file_id,
                limit=1,
                offset=chunk_index,
            ):
                chunk = part
                break
            return chunk

        return await self._retry_flood_wait(
            f"read file_id={file_id} chunk={chunk_index}",
            read_chunk_once,
        )

    async def _read_remote_chunk(self, file_id: str, chunk_index: int) -> bytes:
        key = (file_id, chunk_index)
        cached = self._stream_buffer_get(key)
        if cached is not None:
            return cached

        task = self._start_stream_chunk_task(file_id, chunk_index)
        return await asyncio.shield(task)

    def _schedule_prefetch(self, file_id: str, start_chunk: int, file_size: int):
        if start_chunk < 0 or start_chunk * self._REMOTE_CHUNK_SIZE >= file_size:
            return

        last_file_chunk = (file_size - 1) // self._REMOTE_CHUNK_SIZE
        end_chunk = min(last_file_chunk, start_chunk + self._PREFETCH_CHUNKS - 1)

        active = self._prefetch_tasks.get(file_id)
        active_range = self._prefetch_ranges.get(file_id)
        if active and not active.done() and active_range:
            active_start, active_end = active_range
            if active_start <= start_chunk <= active_end:
                return
            active.cancel()

        task = asyncio.create_task(self._prefetch_worker(file_id, start_chunk, end_chunk))
        self._prefetch_tasks[file_id] = task
        self._prefetch_ranges[file_id] = (start_chunk, end_chunk)

    def _cancel_stale_prefetch(self, file_id: str, requested_chunk: int):
        active = self._prefetch_tasks.get(file_id)
        active_range = self._prefetch_ranges.get(file_id)
        if not active or active.done() or not active_range:
            return
        active_start, active_end = active_range
        if active_start <= requested_chunk <= active_end:
            return
        active.cancel()

    async def _prefetch_worker(self, file_id: str, start_chunk: int, end_chunk: int):
        try:
            for chunk_index in range(start_chunk, end_chunk + 1):
                if (file_id, chunk_index) in self._stream_buffer:
                    continue
                await self._read_remote_chunk(file_id, chunk_index)
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug(
                "Streaming prefetch stopped file_id=%s chunks=%s-%s: %s",
                file_id,
                start_chunk,
                end_chunk,
                exc,
            )
        finally:
            task = asyncio.current_task()
            if self._prefetch_tasks.get(file_id) is task:
                self._prefetch_tasks.pop(file_id, None)
                self._prefetch_ranges.pop(file_id, None)

    async def _downloader_for(self, file_id: str) -> RawDocumentDownloader:
        downloader = self._downloaders.get(file_id)
        if downloader is not None:
            self._downloaders.move_to_end(file_id)
            return downloader

        downloader = RawDocumentDownloader(self._tg_client, file_id)
        self._downloaders[file_id] = downloader
        self._downloaders.move_to_end(file_id)
        while len(self._downloaders) > self._downloaders_limit:
            _, old = self._downloaders.popitem(last=False)
            await old.close()
        return downloader

    async def _read_remote_range(self, info: dict, offset: int, size: int) -> bytes:
        file_id = info.get("file_id")
        file_size = info.get("size", 0)
        if not file_id or size <= 0 or offset >= file_size:
            return b""

        end_offset = min(offset + size, file_size)
        start_chunk = offset // self._REMOTE_CHUNK_SIZE
        end_chunk = (end_offset - 1) // self._REMOTE_CHUNK_SIZE
        expected_size = end_offset - offset
        self._cancel_stale_prefetch(file_id, start_chunk)

        async def read_once() -> bytes:
            pieces: list[bytes] = []
            for chunk_index in range(start_chunk, end_chunk + 1):
                chunk = await self._read_remote_chunk(file_id, chunk_index)
                chunk_start = chunk_index * self._REMOTE_CHUNK_SIZE
                chunk_end = chunk_start + len(chunk)
                take_start = max(offset, chunk_start)
                take_end = min(end_offset, chunk_end)
                if take_start < take_end:
                    pieces.append(chunk[take_start - chunk_start:take_end - chunk_start])
            return b"".join(pieces)

        last_error = None
        for attempt in range(1, self._REMOTE_READ_RETRIES + 1):
            try:
                data = await self._retry_flood_wait(
                    f"read file_id={file_id} offset={offset} size={size}",
                    read_once,
                )
            except (asyncio.TimeoutError, TimeoutError) as exc:
                last_error = exc
                self._drop_stream_buffer_range(file_id, start_chunk, end_chunk)
                log.warning(
                    "Remote read timeout file_id=%s offset=%s size=%s attempt=%s/%s",
                    file_id,
                    offset,
                    size,
                    attempt,
                    self._REMOTE_READ_RETRIES,
                )
            else:
                if len(data) == expected_size:
                    self._schedule_prefetch(file_id, end_chunk + 1, file_size)
                    return data
                last_error = None
                self._drop_stream_buffer_range(file_id, start_chunk, end_chunk)
                log.warning(
                    "Remote read short file_id=%s offset=%s expected=%s got=%s attempt=%s/%s",
                    file_id,
                    offset,
                    expected_size,
                    len(data),
                    attempt,
                    self._REMOTE_READ_RETRIES,
                )
            await asyncio.sleep(0.2)

        raise FUSEError(errno.EIO) from last_error

    def _suppress_remote_message_id(self, msg_id: int):
        self._suppressed_msg_ids.add(msg_id)
        self._pending_remote_delete_msg_ids.add(msg_id)
        self._msg_id_to_inode.pop(msg_id, None)

    def _unsuppress_remote_message_id(self, msg_id: int):
        self._suppressed_msg_ids.discard(msg_id)
        self._pending_remote_delete_msg_ids.discard(msg_id)

    async def _delete_remote_message_remote_only(self, msg_id: int):
        await self._retry_flood_wait(
            f"delete msg_id={msg_id}",
            lambda: self._tg_client.delete_messages(self._chat_id, msg_id),
        )

    async def _delete_remote_message(self, msg_id: int):
        self._suppress_remote_message_id(msg_id)
        try:
            await self._delete_remote_message_remote_only(msg_id)
        except Exception:
            self._unsuppress_remote_message_id(msg_id)
            raise
        self._pending_remote_delete_msg_ids.discard(msg_id)

    async def _delete_remote_messages(self, msg_ids: tuple[int, ...]):
        try:
            for msg_id in msg_ids:
                try:
                    await self._delete_remote_message(msg_id)
                except Exception as exc:
                    log.warning("Can't delete msg_id=%s: %s", msg_id, exc)
        finally:
            task = asyncio.current_task()
            if task is not None:
                self._remote_delete_tasks.discard(task)

    def _schedule_remote_delete(self, msg_ids):
        msg_ids = tuple(sorted(msg_id for msg_id in msg_ids if msg_id))
        if not msg_ids:
            return
        for msg_id in msg_ids:
            self._suppress_remote_message_id(msg_id)
        loop = asyncio.get_running_loop()
        loop.call_later(0.05, self._start_remote_delete_task, msg_ids)

    def _start_remote_delete_task(self, msg_ids: tuple[int, ...]):
        task = asyncio.create_task(self._delete_remote_messages(msg_ids))
        self._remote_delete_tasks.add(task)

    async def _upload_directory_marker(
        self,
        directory_id: str,
        parent_inode: int,
        name: bytes,
    ):
        marker_path = self._new_spool_path()
        try:
            with open(marker_path, "wb") as marker:
                marker.write(b"\0")
            parent_id = self._directory_id_for_parent(parent_inode)
            caption = build_directory_caption(directory_id, parent_id, name)

            async def send_marker_once():
                return await send_document_from_path(
                    self._tg_client,
                    self._chat_id,
                    marker_path,
                    DIRECTORY_MARKER_NAME,
                    1,
                    caption=caption,
                )

            msg = await self._retry_flood_wait(
                f"upload directory marker id={directory_id}",
                send_marker_once,
            )
            remote_entry = self._remote_entry_from_message(msg)
            metadata = remote_entry[4] if remote_entry else None
            if (
                not msg
                or not remote_entry
                or not metadata
                or metadata.get("kind") != "directory"
                or metadata.get("directory_id") != directory_id
            ):
                raise FUSEError(errno.EIO)
            return msg, remote_entry[0]
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(marker_path)

    async def _upload_symlink_marker(
        self,
        parent_inode: int,
        name: bytes,
        target: bytes,
    ):
        marker_path = self._new_spool_path()
        try:
            with open(marker_path, "wb") as marker:
                marker.write(b"\0")
            parent_id = self._directory_id_for_parent(parent_inode)
            caption = build_symlink_caption(parent_id, name, target)

            async def send_marker_once():
                return await send_document_from_path(
                    self._tg_client,
                    self._chat_id,
                    marker_path,
                    SYMLINK_MARKER_NAME,
                    1,
                    caption=caption,
                )

            msg = await self._retry_flood_wait(
                f"upload symlink marker name={name!r}",
                send_marker_once,
            )
            remote_entry = self._remote_entry_from_message(msg)
            metadata = remote_entry[4] if remote_entry else None
            if (
                not msg
                or not remote_entry
                or not metadata
                or metadata.get("kind") != "symlink"
                or metadata.get("name") != name
                or metadata.get("target") != target
            ):
                raise FUSEError(errno.EIO)
            return msg, remote_entry[0]
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(marker_path)

    async def _commit_inode(self, inode: int):
        task = asyncio.create_task(self._commit_inode_impl(inode))
        self._active_upload_tasks.setdefault(inode, set()).add(task)
        try:
            await task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                raise
            if not task.cancelled():
                raise
        finally:
            tasks = self._active_upload_tasks.get(inode)
            if tasks is not None:
                tasks.discard(task)
                if not tasks:
                    self._active_upload_tasks.pop(inode, None)

    async def _commit_inode_impl(self, inode: int):
        if inode not in self._files:
            return

        snapshot_path = None
        delete_after_upload: set[int] = set()
        should_continue = False

        async with self._lock_for(inode):
            if inode not in self._files:
                return

            info = self._files[inode]
            if info.get("kind") != "file":
                return
            if not info.get("dirty"):
                return
            if self.read_only or info.get("read_only", False):
                return

            path = info.get("spool_path")
            size = os.path.getsize(path) if path else info.get("size", 0)
            info["size"] = size

            pending_delete = set(info.get("pending_delete_message_ids") or set())
            change_id = info.get("change_id", 0)
            file_name = self._file_name_text(info)
            parent_id = self._directory_id_for_parent(info["parent_inode"])
            source_file_id = info.get("file_id") if not path else None

            if size == 0:
                delete_after_upload = pending_delete
            elif path:
                snapshot_path = self._new_spool_path()
                shutil.copyfile(path, snapshot_path)
            elif source_file_id:
                snapshot_path = self._new_spool_path()
            else:
                raise FUSEError(errno.EIO)

        delete_failed = False

        if size == 0:
            for old_id in sorted(delete_after_upload):
                try:
                    await self._delete_remote_message(old_id)
                except RPCError as exc:
                    log.warning("Can't delete msg_id=%s: %s", old_id, exc)
                    delete_failed = True

            async with self._lock_for(inode):
                info = self._files.get(inode)
                if not info:
                    return
                if delete_failed:
                    info["read_only"] = True
                    return
                if info.get("change_id", 0) == change_id:
                    self._remove_spool(info)
                    old_msg_id = info.get("message_id")
                    if old_msg_id:
                        self._msg_id_to_inode.pop(old_msg_id, None)
                    info["message_id"] = None
                    info["file_id"] = None
                    info["dirty"] = False
                    info["pending_delete_message_ids"] = set()
                    info["timestamp"] = int(time.time())
                else:
                    should_continue = bool(info.get("dirty") and not info.get("unlinked"))
            if should_continue:
                self._schedule_upload(inode, 0)
            return

        try:
            if source_file_id:
                await self._copy_remote_to_path(
                    source_file_id,
                    snapshot_path,
                    f"snapshot inode={inode}",
                )

            try:
                async def send_document_once():
                    return await send_document_from_path(
                        self._tg_client,
                        self._chat_id,
                        snapshot_path,
                        file_name,
                        size,
                        caption=build_file_caption(parent_id),
                    )

                msg = await self._retry_flood_wait(
                    f"upload inode={inode} name={file_name}",
                    send_document_once,
                )
            except RPCError as exc:
                log.error("Upload failed inode=%s: %s", inode, exc)
                async with self._lock_for(inode):
                    info = self._files.get(inode)
                    if info:
                        info["read_only"] = True
                raise FUSEError(errno.EIO) from exc

            remote_file = self._remote_doc_from_message(msg)
            if not msg or not remote_file:
                raise FUSEError(errno.EIO)

            new_msg_id = msg.id
            new_file_id = remote_file[0]

            async with self._lock_for(inode):
                info = self._files.get(inode)
                if not info or info.get("unlinked"):
                    delete_after_upload = {new_msg_id}
                    should_continue = False
                else:
                    old_msg_id = info.get("message_id")
                    if old_msg_id and old_msg_id != new_msg_id:
                        pending_delete.add(old_msg_id)
                        self._msg_id_to_inode.pop(old_msg_id, None)

                    info["message_id"] = new_msg_id
                    info["file_id"] = new_file_id
                    self._msg_id_to_inode[new_msg_id] = inode

                    if info.get("change_id", 0) == change_id:
                        info["size"] = size
                        info["timestamp"] = int(time.time())
                        info["dirty"] = False
                        info["pending_delete_message_ids"] = set()
                        self._remove_spool(info)
                        should_continue = False
                    else:
                        info.setdefault("pending_delete_message_ids", set()).add(new_msg_id)
                        should_continue = bool(
                            info.get("dirty")
                            and not info.get("unlinked")
                            and info.get("refcount", 0) == 0
                        )

                    delete_after_upload = {
                        msg_id for msg_id in pending_delete if msg_id != new_msg_id
                    }

            for old_id in sorted(delete_after_upload):
                try:
                    await self._delete_remote_message(old_id)
                except RPCError as exc:
                    log.warning("Can't delete old msg_id=%s after upload: %s", old_id, exc)
                    delete_failed = True

            if delete_failed:
                async with self._lock_for(inode):
                    info = self._files.get(inode)
                    if info:
                        info["read_only"] = True

            if should_continue:
                self._schedule_upload(inode, 0)

            log.debug("Committed inode=%s => msg_id=%s", inode, new_msg_id)
        finally:
            if snapshot_path:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(snapshot_path)

    def _cancel_delayed_upload(self, inode: int):
        task = self._delayed_upload_tasks.pop(inode, None)
        if task:
            task.cancel()

    def _schedule_upload(self, inode: int, delay_s: float):
        task = self._delayed_upload_tasks.get(inode)
        if task and not task.done():
            return
        task = asyncio.create_task(self._delayed_commit(inode, delay_s))
        self._delayed_upload_tasks[inode] = task

    async def _cancel_delayed_upload_task(self, inode: int):
        task = self._delayed_upload_tasks.pop(inode, None)
        if not task or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _cancel_active_upload_tasks(self, inode: int):
        current = asyncio.current_task()
        tasks = {
            task
            for task in self._active_upload_tasks.get(inode, set())
            if task is not current and not task.done()
        }
        if not tasks:
            return
        log.info("Cancelling %s active upload(s) for inode=%s.", len(tasks), inode)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _should_commit_dirty_info(self, info: dict) -> bool:
        return bool(
            info.get("kind") == "file"
            and info.get("dirty")
            and not info.get("unlinked")
            and not self.read_only
            and not info.get("read_only", False)
            and (
                info.get("pending_delete_message_ids")
                or not self._is_temp_name(info["file_name"])
            )
        )

    async def _commit_inode_sync(self, inode: int):
        await self._cancel_delayed_upload_task(inode)
        await self._commit_inode(inode)

    async def _delayed_commit(self, inode: int, delay_s: float):
        try:
            await asyncio.sleep(delay_s)
            while True:
                await self._commit_inode(inode)
                info = self._files.get(inode)
                if (
                    not info
                    or not info.get("dirty")
                    or info.get("unlinked")
                    or info.get("read_only", False)
                    or self.read_only
                    or info.get("refcount", 0) > 0
                ):
                    break
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("Delayed upload failed inode=%s: %s", inode, exc)
        finally:
            task = asyncio.current_task()
            if self._delayed_upload_tasks.get(inode) is task:
                self._delayed_upload_tasks.pop(inode, None)

    async def getattr(self, inode, ctx=None) -> EntryAttributes:
        now_ns = int(time.time() * 1e9)
        if inode == self._root_inode:
            attr = EntryAttributes()
            attr.st_mode = stat.S_IFDIR | 0o755
            attr.st_ino = inode
            attr.st_uid = os.getuid()
            attr.st_gid = os.getgid()
            attr.st_size = 0
            attr.st_nlink = 2
            attr.st_atime_ns = now_ns
            attr.st_mtime_ns = now_ns
            attr.st_ctime_ns = now_ns
            attr.entry_timeout = 0
            attr.attr_timeout = 0
            return attr

        info = self._files.get(inode)
        if not info or info.get("unlinked"):
            raise FUSEError(errno.ENOENT)

        is_directory = info.get("kind") == "directory"
        is_symlink = info.get("kind") == "symlink"
        default_mode = 0o755 if is_directory else (0o777 if is_symlink else 0o644)
        mode = info.get("mode", default_mode)
        is_ro = self.read_only or info.get("read_only", False)
        attr = EntryAttributes()
        attr.st_ino = inode
        file_type = stat.S_IFDIR if is_directory else (
            stat.S_IFLNK if is_symlink else stat.S_IFREG
        )
        read_only_mode = 0o555 if is_directory else (0o777 if is_symlink else 0o444)
        attr.st_mode = file_type | (read_only_mode if is_ro else mode)
        attr.st_uid = os.getuid()
        attr.st_gid = os.getgid()
        attr.st_nlink = 2 if is_directory else 1
        attr.st_size = info["size"]
        t_ns = info["timestamp"] * 10**9
        attr.st_atime_ns = t_ns
        attr.st_mtime_ns = t_ns
        attr.st_ctime_ns = t_ns
        attr.entry_timeout = 0
        attr.attr_timeout = 0
        return attr

    async def lookup(self, parent_inode, name, ctx=None) -> EntryAttributes:
        if parent_inode != self._root_inode:
            parent = self._files.get(parent_inode)
            if not parent or parent.get("kind") != "directory":
                raise FUSEError(errno.ENOTDIR)
        inode = self._name_to_inode.get((parent_inode, name))
        if not inode:
            raise FUSEError(errno.ENOENT)
        return await self.getattr(inode)

    async def opendir(self, inode, ctx):
        if inode != self._root_inode and (
            inode not in self._files or self._files[inode].get("kind") != "directory"
        ):
            raise FUSEError(errno.ENOTDIR)
        return inode

    async def readdir(self, fh, start_id, token):
        if fh != self._root_inode and (
            fh not in self._files or self._files[fh].get("kind") != "directory"
        ):
            raise FUSEError(errno.ENOTDIR)

        entries = (
            (name, inode)
            for (parent_inode, name), inode in self._name_to_inode.items()
            if parent_inode == fh
        )
        for fname, inode in sorted(entries, key=lambda item: item[1]):
            if inode < start_id:
                continue
            attr = await self.getattr(inode)
            next_off = inode + 1
            ok = pyfuse3.readdir_reply(token, fname, attr, next_off)
            if not ok:
                break

    def _create_local_inode(
        self, parent_inode: int, name: bytes, mode: int, refcount: int
    ) -> int:
        self._directory_id_for_parent(parent_inode)
        if (parent_inode, name) in self._name_to_inode:
            raise FUSEError(errno.EEXIST)

        inode = self._next_inode
        self._next_inode += 1
        self._files[inode] = self._new_file_info(
            message_id=None,
            file_id=None,
            file_name=name,
            parent_inode=parent_inode,
            size=0,
            refcount=refcount,
        )
        self._files[inode]["mode"] = mode & 0o777
        self._name_to_inode[(parent_inode, name)] = inode
        return inode

    def _file_info(self, fh: int) -> FileInfo:
        fi = FileInfo(fh=fh)
        fi.direct_io = True
        fi.keep_cache = False
        return fi

    async def create(self, parent_inode, name, mode, flags, ctx):
        if self.read_only:
            raise FUSEError(errno.EROFS)
        inode = self._create_local_inode(parent_inode, name, mode, refcount=1)
        fh = self._next_fh
        self._next_fh += 1
        self._fh_to_inode[fh] = inode

        fi = self._file_info(fh)
        attr = await self.getattr(inode)
        return (fi, attr)

    async def mknod(self, parent_inode, name, mode, rdev, ctx):
        if self.read_only:
            raise FUSEError(errno.EROFS)
        if not stat.S_ISREG(mode):
            raise FUSEError(errno.EPERM)

        inode = self._create_local_inode(parent_inode, name, mode, refcount=0)
        return await self.getattr(inode)

    async def open(self, inode: int, flags: int, ctx) -> FileInfo:
        if inode not in self._files:
            raise FUSEError(errno.ENOENT)

        info = self._files[inode]
        if info.get("kind") == "directory":
            raise FUSEError(errno.EISDIR)
        if info.get("kind") == "symlink":
            raise FUSEError(errno.ELOOP)
        accmode = flags & os.O_ACCMODE
        want_write = accmode in (os.O_WRONLY, os.O_RDWR)

        if (self.read_only or info.get("read_only", False)) and want_write:
            raise FUSEError(errno.EROFS)

        if want_write and flags & os.O_TRUNC:
            await self._truncate_inode(inode, 0)

        info["refcount"] += 1
        fh = self._next_fh
        self._next_fh += 1
        self._fh_to_inode[fh] = inode
        return self._file_info(fh)

    async def release(self, fh):
        inode = self._fh_to_inode.pop(fh, None)
        if inode is None or inode not in self._files:
            return

        async with self._lock_for(inode):
            if inode not in self._files:
                return
            info = self._files[inode]
            info["refcount"] -= 1
            if info["refcount"] < 0:
                info["refcount"] = 0
            refcount = info["refcount"]
            should_upload = bool(
                refcount == 0
                and self._should_commit_dirty_info(info)
            )
            temp_dirty = bool(
                refcount == 0
                and info.get("dirty")
                and not should_upload
                and self._is_temp_name(info["file_name"])
            )
            unlinked = bool(info.get("unlinked"))

        if should_upload:
            await self._commit_inode_sync(inode)
        elif temp_dirty:
            log.debug("Keeping temp inode=%s local until rename or unmount.", inode)

        if unlinked and refcount == 0:
            self._files.pop(inode, None)

    async def read(self, fh, offset, size):
        inode = self._fh_to_inode.get(fh)
        if inode is None:
            raise FUSEError(errno.EBADF)
        info = self._files[inode]
        if info.get("spool_path"):
            return await self._read_spool(info, offset, size)
        return await self._read_remote_range(info, offset, size)

    async def write(self, fh: int, offset: int, data: bytes) -> int:
        inode = self._fh_to_inode.get(fh)
        if inode is None:
            raise FUSEError(errno.EBADF)

        async with self._lock_for(inode):
            info = self._files[inode]
            if self.read_only or info.get("read_only", False):
                raise FUSEError(errno.EROFS)

            copy_remote = bool(info.get("file_id") and not info.get("dirty"))
            path = await self._ensure_spool(inode, copy_remote=copy_remote)
            self._remember_pending_delete(info, info.get("message_id"))

            with open(path, "r+b") as fp:
                fp.seek(0, os.SEEK_END)
                current_size = fp.tell()
                if offset > current_size:
                    fp.write(b"\0" * (offset - current_size))
                fp.seek(offset)
                fp.write(data)
                current_size = max(current_size, offset + len(data))

            info["size"] = current_size
            self._mark_dirty(info)
        return len(data)

    async def unlink(self, parent_inode: int, name: bytes, ctx):
        if self.read_only:
            raise FUSEError(errno.EROFS)
        self._directory_id_for_parent(parent_inode)

        inode = self._name_to_inode.get((parent_inode, name))
        if inode is None:
            raise FUSEError(errno.ENOENT)

        info = self._files[inode]
        if info.get("kind") == "directory":
            raise FUSEError(errno.EISDIR)
        if info.get("read_only", False):
            raise FUSEError(errno.EPERM)

        await self._cancel_delayed_upload_task(inode)
        await self._cancel_active_upload_tasks(inode)
        info = self._files.get(inode)
        if info is None:
            return
        ids = set(info.get("pending_delete_message_ids") or set())
        self._remember_pending_delete(info, info.get("message_id"))
        ids.update(info.get("pending_delete_message_ids") or set())

        try:
            for msg_id in sorted(ids):
                await self._delete_remote_message_remote_only(msg_id)
        except Exception as exc:
            log.warning("Can't unlink %s because remote delete failed: %s", name, exc)
            if info.get("dirty") and info.get("refcount", 0) == 0 and self._should_commit_dirty_info(info):
                self._schedule_upload(inode, 0)
            raise FUSEError(errno.EIO) from exc

        for msg_id in sorted(ids):
            self._suppress_remote_message_id(msg_id)
            self._pending_remote_delete_msg_ids.discard(msg_id)

        self._remove_spool(info)
        self._name_to_inode.pop((parent_inode, name), None)
        info["unlinked"] = True
        info["pending_delete_message_ids"] = set()
        if info.get("refcount", 0) == 0:
            self._files.pop(inode, None)

    async def rename(self, parent_inode_old, name_old, parent_inode_new, name_new, flags, ctx):
        if self.read_only:
            raise FUSEError(errno.EROFS)
        self._directory_id_for_parent(parent_inode_old)
        self._directory_id_for_parent(parent_inode_new)

        old_key = (parent_inode_old, name_old)
        new_key = (parent_inode_new, name_new)
        old_inode = self._name_to_inode.get(old_key)
        if old_inode is None:
            raise FUSEError(errno.ENOENT)
        if old_key == new_key:
            return

        new_inode = self._name_to_inode.get(new_key)
        if flags & pyfuse3.RENAME_NOREPLACE and new_inode is not None:
            raise FUSEError(errno.EEXIST)

        old_info = self._files[old_inode]
        if old_info.get("kind") != "directory":
            await self._cancel_delayed_upload_task(old_inode)
            await self._cancel_active_upload_tasks(old_inode)
        if old_info.get("kind") == "symlink":
            if flags & pyfuse3.RENAME_EXCHANGE:
                raise FUSEError(errno.EINVAL)
            await self._rename_symlink(
                old_inode,
                parent_inode_old,
                name_old,
                parent_inode_new,
                name_new,
                new_inode,
            )
            return
        if old_info.get("kind") == "directory":
            if flags & pyfuse3.RENAME_EXCHANGE:
                raise FUSEError(errno.EINVAL)
            await self._rename_directory(
                old_inode,
                parent_inode_old,
                name_old,
                parent_inode_new,
                name_new,
                new_inode,
            )
            return

        if new_inode is not None and self._files[new_inode].get("kind") == "directory":
            raise FUSEError(errno.EISDIR)

        if flags & pyfuse3.RENAME_EXCHANGE:
            if new_inode is None:
                raise FUSEError(errno.ENOENT)
            new_info = self._files[new_inode]
            self._name_to_inode[old_key] = new_inode
            self._name_to_inode[new_key] = old_inode
            old_info["file_name"] = name_new
            old_info["parent_inode"] = parent_inode_new
            new_info["file_name"] = name_old
            new_info["parent_inode"] = parent_inode_old
            self._remember_pending_delete(old_info, old_info.get("message_id"))
            self._remember_pending_delete(new_info, new_info.get("message_id"))
            self._mark_dirty(old_info)
            self._mark_dirty(new_info)
            if old_info.get("refcount", 0) == 0 and self._should_commit_dirty_info(old_info):
                await self._commit_inode_sync(old_inode)
            if new_info.get("refcount", 0) == 0 and self._should_commit_dirty_info(new_info):
                await self._commit_inode_sync(new_inode)
            return

        if new_inode is not None and new_inode != old_inode:
            new_info = self._files[new_inode]
            await self._cancel_delayed_upload_task(new_inode)
            await self._cancel_active_upload_tasks(new_inode)
            self._remember_pending_delete(old_info, new_info.get("message_id"))
            for msg_id in new_info.get("pending_delete_message_ids") or set():
                self._remember_pending_delete(old_info, msg_id)
            self._name_to_inode.pop(new_key, None)
            new_info["unlinked"] = True
            self._remove_spool(new_info)
            if new_info.get("refcount", 0) == 0:
                self._files.pop(new_inode, None)

        self._name_to_inode.pop(old_key, None)
        self._name_to_inode[new_key] = old_inode
        old_info["file_name"] = name_new
        old_info["parent_inode"] = parent_inode_new

        if old_info.get("file_id") and not old_info.get("dirty"):
            self._remember_pending_delete(old_info, old_info.get("message_id"))
        if (
            old_info.get("dirty")
            or old_info.get("file_id")
            or old_info.get("pending_delete_message_ids")
        ):
            self._mark_dirty(old_info)

        old_info["timestamp"] = int(time.time())
        if old_info.get("dirty") and old_info.get("refcount", 0) == 0:
            if self._should_commit_dirty_info(old_info):
                await self._commit_inode_sync(old_inode)

    def _is_directory_descendant(self, parent_inode: int, directory_inode: int) -> bool:
        current = parent_inode
        while current != self._root_inode:
            if current == directory_inode:
                return True
            info = self._files.get(current)
            if not info or info.get("kind") != "directory":
                return False
            current = info["parent_inode"]
        return False

    async def _rename_symlink(
        self,
        inode: int,
        old_parent: int,
        old_name: bytes,
        new_parent: int,
        new_name: bytes,
        target_inode: int | None,
    ):
        info = self._files[inode]
        if info.get("read_only", False):
            raise FUSEError(errno.EPERM)

        target_info = None
        if target_inode is not None and target_inode != inode:
            target_info = self._files[target_inode]
            if target_info.get("kind") == "directory":
                raise FUSEError(errno.EISDIR)

        msg, file_id = await self._upload_symlink_marker(
            new_parent, new_name, info["target"]
        )
        delete_ids = set(info.get("pending_delete_message_ids") or set())
        if info.get("message_id"):
            delete_ids.add(info["message_id"])
        if target_info:
            await self._cancel_delayed_upload_task(target_inode)
            await self._cancel_active_upload_tasks(target_inode)
            if target_info.get("message_id"):
                delete_ids.add(target_info["message_id"])
            delete_ids.update(target_info.get("pending_delete_message_ids") or set())

        self._name_to_inode.pop((old_parent, old_name), None)
        if target_info:
            self._name_to_inode.pop((new_parent, new_name), None)
            old_target_msg = target_info.get("message_id")
            if old_target_msg:
                self._msg_id_to_inode.pop(old_target_msg, None)
            target_info["unlinked"] = True
            self._remove_spool(target_info)
            if target_info.get("refcount", 0) == 0:
                self._files.pop(target_inode, None)

        old_msg_id = info.get("message_id")
        if old_msg_id:
            self._msg_id_to_inode.pop(old_msg_id, None)
        info.update(
            message_id=msg.id,
            file_id=file_id,
            file_name=new_name,
            parent_inode=new_parent,
            timestamp=int(time.time()),
            pending_delete_message_ids=set(),
        )
        self._name_to_inode[(new_parent, new_name)] = inode
        self._msg_id_to_inode[msg.id] = inode

        try:
            for msg_id in sorted(delete_ids - {msg.id}):
                await self._delete_remote_message(msg_id)
        except Exception as exc:
            info["read_only"] = True
            raise FUSEError(errno.EIO) from exc

    async def _rename_directory(
        self,
        inode: int,
        old_parent: int,
        old_name: bytes,
        new_parent: int,
        new_name: bytes,
        target_inode: int | None,
    ):
        info = self._files[inode]
        if info.get("read_only", False):
            raise FUSEError(errno.EPERM)
        if self._is_directory_descendant(new_parent, inode):
            raise FUSEError(errno.EINVAL)

        target_info = None
        if target_inode is not None and target_inode != inode:
            target_info = self._files[target_inode]
            if target_info.get("kind") != "directory":
                raise FUSEError(errno.ENOTDIR)
            if self._directory_not_empty(target_inode):
                raise FUSEError(errno.ENOTEMPTY)

        msg, file_id = await self._upload_directory_marker(
            info["directory_id"], new_parent, new_name
        )
        delete_ids = set(info.get("pending_delete_message_ids") or set())
        if info.get("message_id"):
            delete_ids.add(info["message_id"])
        if target_info:
            if target_info.get("message_id"):
                delete_ids.add(target_info["message_id"])
            delete_ids.update(target_info.get("pending_delete_message_ids") or set())

        self._name_to_inode.pop((old_parent, old_name), None)
        if target_info:
            self._name_to_inode.pop((new_parent, new_name), None)
            self._directory_id_to_inode.pop(target_info["directory_id"], None)
            old_target_msg = target_info.get("message_id")
            if old_target_msg:
                self._msg_id_to_inode.pop(old_target_msg, None)
            self._files.pop(target_inode, None)

        old_msg_id = info.get("message_id")
        if old_msg_id:
            self._msg_id_to_inode.pop(old_msg_id, None)
        info.update(
            message_id=msg.id,
            file_id=file_id,
            file_name=new_name,
            parent_inode=new_parent,
            timestamp=int(time.time()),
            pending_delete_message_ids=set(),
        )
        self._name_to_inode[(new_parent, new_name)] = inode
        self._msg_id_to_inode[msg.id] = inode

        try:
            for msg_id in sorted(delete_ids - {msg.id}):
                await self._delete_remote_message(msg_id)
        except Exception as exc:
            info["read_only"] = True
            raise FUSEError(errno.EIO) from exc

    async def setattr(self, inode, attr, fields, fh, ctx):
        if inode == self._root_inode:
            return await self.getattr(inode)
        if inode not in self._files:
            raise FUSEError(errno.ENOENT)

        info = self._files[inode]
        if self.read_only or info.get("read_only", False):
            if fields.update_size or fields.update_mode or fields.update_uid or fields.update_gid:
                raise FUSEError(errno.EROFS)

        if fields.update_size:
            if info.get("kind") == "directory":
                raise FUSEError(errno.EISDIR)
            await self._truncate_inode(inode, attr.st_size)
        if fields.update_mode:
            info["mode"] = attr.st_mode & 0o777
        if fields.update_mtime:
            info["timestamp"] = int(attr.st_mtime_ns / 10**9)
        elif fields.update_ctime:
            info["timestamp"] = int(time.time())
        if fields.update_uid or fields.update_gid:
            raise FUSEError(errno.EPERM)

        return await self.getattr(inode)

    async def mkdir(self, parent_inode, name, mode, ctx):
        if self.read_only:
            raise FUSEError(errno.EROFS)
        self._directory_id_for_parent(parent_inode)
        if (parent_inode, name) in self._name_to_inode:
            raise FUSEError(errno.EEXIST)

        directory_id = uuid.uuid4().hex
        msg, file_id = await self._upload_directory_marker(directory_id, parent_inode, name)
        inode = self._next_inode
        self._next_inode += 1
        self._files[inode] = self._new_directory_info(
            message_id=msg.id,
            file_id=file_id,
            file_name=name,
            parent_inode=parent_inode,
            directory_id=directory_id,
        )
        self._files[inode]["mode"] = mode & 0o777
        self._name_to_inode[(parent_inode, name)] = inode
        self._directory_id_to_inode[directory_id] = inode
        self._msg_id_to_inode[msg.id] = inode
        return await self.getattr(inode)

    async def rmdir(self, parent_inode, name, ctx):
        if self.read_only:
            raise FUSEError(errno.EROFS)
        self._directory_id_for_parent(parent_inode)
        inode = self._name_to_inode.get((parent_inode, name))
        if inode is None:
            raise FUSEError(errno.ENOENT)
        info = self._files[inode]
        if info.get("kind") != "directory":
            raise FUSEError(errno.ENOTDIR)
        if self._directory_not_empty(inode):
            raise FUSEError(errno.ENOTEMPTY)
        if info.get("read_only", False):
            raise FUSEError(errno.EPERM)

        ids = set(info.get("pending_delete_message_ids") or set())
        if info.get("message_id"):
            ids.add(info["message_id"])
        try:
            for msg_id in sorted(ids):
                await self._delete_remote_message_remote_only(msg_id)
        except Exception as exc:
            raise FUSEError(errno.EIO) from exc
        for msg_id in sorted(ids):
            self._suppress_remote_message_id(msg_id)
            self._pending_remote_delete_msg_ids.discard(msg_id)
        self._name_to_inode.pop((parent_inode, name), None)
        self._directory_id_to_inode.pop(info["directory_id"], None)
        self._files.pop(inode, None)

    async def link(self, *args, **kwargs):
        raise FUSEError(errno.ENOSYS)

    async def symlink(self, parent_inode, name, target, ctx):
        if self.read_only:
            raise FUSEError(errno.EROFS)
        self._directory_id_for_parent(parent_inode)
        if (parent_inode, name) in self._name_to_inode:
            raise FUSEError(errno.EEXIST)
        if not target or len(target) > 384 or b"\0" in target:
            raise FUSEError(errno.ENAMETOOLONG)

        msg, file_id = await self._upload_symlink_marker(
            parent_inode, name, target
        )
        inode = self._next_inode
        self._next_inode += 1
        self._files[inode] = self._new_symlink_info(
            message_id=msg.id,
            file_id=file_id,
            file_name=name,
            parent_inode=parent_inode,
            target=target,
        )
        self._name_to_inode[(parent_inode, name)] = inode
        self._msg_id_to_inode[msg.id] = inode
        return await self.getattr(inode)

    async def readlink(self, inode, ctx):
        info = self._files.get(inode)
        if not info or info.get("unlinked"):
            raise FUSEError(errno.ENOENT)
        if info.get("kind") != "symlink":
            raise FUSEError(errno.EINVAL)
        return info["target"]

    async def flush(self, fh: pyfuse3.FileHandleT) -> None:
        inode = self._fh_to_inode.get(fh)
        if inode is None or inode not in self._files:
            return
        info = self._files[inode]
        if info.get("refcount", 0) <= 1 and self._should_commit_dirty_info(info):
            await self._commit_inode_sync(inode)

    async def fsync(self, fh: pyfuse3.FileHandleT, datasync: bool) -> None:
        inode = self._fh_to_inode.get(fh)
        if inode is None or inode not in self._files:
            return
        info = self._files[inode]
        if self._should_commit_dirty_info(info):
            await self._commit_inode_sync(inode)

    async def fsyncdir(self, fh: pyfuse3.FileHandleT, datasync: bool) -> None:
        return

    async def releasedir(self, fh: pyfuse3.FileHandleT) -> None:
        return

    async def forget(self, inode_list: Sequence[Tuple[pyfuse3.InodeT, int]]) -> None:
        return

    async def ioctl(self, fh, command, arg, fip, in_buf, out_buf_size) -> bytes:
        raise FUSEError(errno.ENOTTY)

    async def copy_file_range(
        self, fh_in, off_in, fh_out, off_out, length, flags
    ) -> int:
        raise FUSEError(errno.EOPNOTSUPP)

    async def statfs(self, ctx):
        st = pyfuse3.StatvfsData()
        st.f_bsize = 4096
        st.f_frsize = 4096
        st.f_blocks = 1_000_000
        st.f_bfree = 500_000
        st.f_bavail = 500_000
        st.f_files = 10_000
        st.f_ffree = 9_000
        st.f_favail = 9_000
        st.f_namemax = 255
        return st


async def fuse_stopper(fs):
    log.info("Unmounting FUSE...")
    await fs.destroy()
    pyfuse3.close()


async def fuse_runner(mountpoint, fs, fuse_opts):
    try:
        log.info("Initializing pyfuse3...")
        pyfuse3.init(fs, mountpoint, fuse_opts)
        log.info("Starting FUSE main loop...")
        await pyfuse3.main()
    finally:
        await fuse_stopper(fs)


if __name__ == "__main__":
    raise RuntimeError("This module should be run only via main.py")
