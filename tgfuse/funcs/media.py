import base64
import json
import time


FILE_MEDIA_FIELDS = ("document", "video", "audio", "animation")
TGFS_CAPTION_PREFIX = "tgfuse:v1:"
ROOT_DIRECTORY_ID = "root"
DIRECTORY_MARKER_NAME = ".tgfuse-directory"


def _encode_name(name: bytes) -> str:
    return base64.urlsafe_b64encode(name).decode("ascii").rstrip("=")


def _decode_name(value: str) -> bytes | None:
    try:
        padding = "=" * (-len(value) % 4)
        name = base64.b64decode(value + padding, altchars=b"-_", validate=True)
    except (ValueError, TypeError):
        return None
    if not name or len(name) > 255 or b"/" in name or b"\0" in name:
        return None
    return name


def _valid_directory_id(value) -> bool:
    if value == ROOT_DIRECTORY_ID:
        return True
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(char in "0123456789abcdef" for char in value)
    )


def build_file_caption(parent_id: str) -> str:
    return TGFS_CAPTION_PREFIX + json.dumps(
        {"type": "file", "parent": parent_id},
        separators=(",", ":"),
        sort_keys=True,
    )


def build_directory_caption(directory_id: str, parent_id: str, name: bytes) -> str:
    return TGFS_CAPTION_PREFIX + json.dumps(
        {
            "id": directory_id,
            "name": _encode_name(name),
            "parent": parent_id,
            "type": "directory",
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def parse_tgfs_caption(caption) -> dict | None:
    if not isinstance(caption, str) or not caption.startswith(TGFS_CAPTION_PREFIX):
        return None
    try:
        value = json.loads(caption[len(TGFS_CAPTION_PREFIX):])
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(value, dict) or not _valid_directory_id(value.get("parent")):
        return None

    if value.get("type") == "file":
        return {"kind": "file", "parent_id": value["parent"]}
    if value.get("type") != "directory" or value.get("id") == ROOT_DIRECTORY_ID:
        return None
    if not _valid_directory_id(value.get("id")):
        return None
    name = _decode_name(value.get("name"))
    if name is None:
        return None
    return {
        "kind": "directory",
        "directory_id": value["id"],
        "parent_id": value["parent"],
        "name": name,
    }


def file_media_from_message(msg):
    if isinstance(msg, list):
        for item in msg:
            media = file_media_from_message(item)
            if media is not None:
                return media
        return None

    if not msg or getattr(msg, "empty", False):
        return None

    for field in FILE_MEDIA_FIELDS:
        media = getattr(msg, field, None)
        if media and getattr(media, "file_id", None):
            return field, media
    return None


def remote_file_from_message(msg):
    media_info = file_media_from_message(msg)
    if media_info is None:
        return None

    media_type, media = media_info
    file_id = media.file_id
    size = getattr(media, "file_size", None) or 0
    file_name = getattr(media, "file_name", None) or f"{media_type}_{file_id[:10]}"

    timestamp = int(time.time())
    if not isinstance(msg, list) and getattr(msg, "date", None):
        timestamp = int(msg.date.timestamp())

    return (file_id, file_name.encode("utf-8", errors="replace"), size, timestamp)


def remote_entry_from_message(msg):
    remote_file = remote_file_from_message(msg)
    if remote_file is None:
        return None
    metadata = None
    if not isinstance(msg, list):
        metadata = parse_tgfs_caption(getattr(msg, "caption", None))
    return (*remote_file, metadata)
