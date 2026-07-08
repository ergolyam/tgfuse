import os

class Config:
    log_level: str = "INFO"
    tg_id: str = ''
    tg_hash: str = ''
    tg_token: str = ''
    ftp: bool = False
    chat_id: int = 0
    tg_upload_workers: int = 4
    tg_upload_buffer_parts: int = 16

    @classmethod
    def load_from_env(cls):
        dotenv_values = cls._read_dotenv()
        for key in cls.__annotations__:
            env_key = key.upper()
            env_value = os.getenv(env_key)
            if env_value is None:
                env_value = dotenv_values.get(env_key)
            if env_value is not None:
                current_value = getattr(cls, key)
                if isinstance(current_value, bool):
                    setattr(cls, key, env_value.lower() in ('true', '1', 'yes'))
                elif isinstance(current_value, int):
                    setattr(cls, key, int(env_value))
                else:
                    setattr(cls, key, env_value)

    @staticmethod
    def _read_dotenv(path: str = ".env") -> dict[str, str]:
        values = {}
        try:
            with open(path, "r", encoding="utf-8") as fp:
                for line in fp:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key:
                        values[key] = value
        except FileNotFoundError:
            pass
        return values

Config.load_from_env()

if __name__ == "__main__":
    raise RuntimeError("This module should be run only via main.py")
