# tgfuse

TelegramFS `tgfuse` is a FUSE-based filesystem that allows you to mount a Telegram chat `channel` as a local directory on your machine. You can then browse, open, and `optionally` write files directly to Telegram, as if they were stored locally on your disk.

### Initial Setup

1. **Clone the repository**: Clone this repository using `git clone`.
2. **Install fuse3 on your distribution**:
    - fedora: `sudo dnf install fuse3-devel`
    - debian: `sudo apt-get install libfuse3-dev`
    - arch: `sudo pacman -S fuse3`
2. **Download Dependencies**: Download the required dependencies into the Virtual Environment `venv` using `uv`.

```shell
git clone https://github.com/ergolyam/tgfuse.git
cd tgfuse
python -m venv .venv
.venv/bin/python -m pip install uv
.venv/bin/python -m uv sync
```

### Deploy

- Create mount directory:
    ```bash
    mkdir /path/to/mount
    ```

- Run the bot:
    ```bash
    TG_ID="your_telegram_api_id" TG_HASH="your_telegram_api_hash" CHAT_ID="your_channel_id" uv run tgfuse /path/to/mount
    ```
    - *if something goes wrong, use it: `fusermount -u /path/to/mount`*

- Other working env's:
    ```env
    LOG_LEVEL="INFO"
    TG_ID="your_telegram_api_id"
    TG_HASH="your_telegram_api_hash"
    TG_TOKEN="your_telegram_bot_token" # if you don't have one, it's userbot.
    CHAT_ID="your_channel_id"
    TG_UPLOAD_WORKERS="4" # parallel Telegram upload workers for large files
    TG_UPLOAD_BUFFER_PARTS="16" # queued 512 KiB upload parts kept in memory
    ```

### Features

- **Channel as network drive**: Mount a Telegram channel as a local directory using pyfuse3.
- **Read-only or read/write**: If you have permissions to send messages in the specified chat/channel, the filesystem will act in read-write mode. Otherwise, it automatically becomes read-only.
- **Automatic synchronization**: Periodically checks for new/removed files in the Telegram chat and updates the mounted filesystem accordingly.
- **Persistent directories**: Nested and empty directories are restored from versioned metadata stored in Telegram document captions.
- **Unmanaged compatibility directory**: The permanent root-level `unmanaged/` directory contains Telegram documents without valid `tgfuse:v1:` metadata. Files written there are uploaded without a tgfuse caption for compatibility with the original flat filesystem.
- **Lazy streamed downloads**: Files are read from Telegram on demand with bounded read-ahead for sequential media playback.
- **On-demand uploads**: When creating or modifying files, they are uploaded back to the Telegram chat. Removing a file cancels its pending or active upload, including queued large-file chunks.
- **Multiple Client Support**: Enjoy the flexibility to connect to Telegram in two distinct ways.
    - **Userbot Support**: Use your personal Telegram account (userbot) to access all available features when needed.  
    - **Bot Token Support**: Alternatively, utilize a dedicated bot token for accessing Telegram content, offering a robust and controlled method for managing your channels.

### Disclaimer

- **We do not recommend using your personal Telegram account for this project**. There is a potential risk that your account might be flagged or suspended due to excessive API usage or triggering Telegram's spam filters.
- It is best to use a regular bot **with a bot token** for this project to help avoid the risk of blocking. Normal bots, while limited in some API methods (e.g., they must fetch messages in chunks), provide a safer and more controlled environment for these operations.
- Use this project **at your own risk**. Respect Telegram’s terms of service and any relevant local laws when storing or distributing content via Telegram channels/groups.
