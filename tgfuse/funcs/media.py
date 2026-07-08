import time


FILE_MEDIA_FIELDS = ("document", "video", "audio", "animation")


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
