import os
from pathlib import Path
from dotenv import load_dotenv


def load_env(env_file: str | None = None) -> None:
    """
    Load environment variables from a .env file.

    Resolution order:
    1. Explicit env_file argument
    2. ENV-specific file (.env.local, .env.dev, .env.prod)
    3. Fallback to .env
    """

    if env_file:
        load_dotenv(env_file)
        return

    env = os.getenv("ENV", "local").lower()

    candidates = [
        f".env.{env}",
        ".env",
    ]

    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            load_dotenv(path)
            return
