import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)


class Settings(BaseSettings):
    # MySql Database Config
    SNOWFLAKE_USER: str = os.environ.get("SNOWFLAKE_USER", "")
    SNOWFLAKE_PASSWORD: str = os.environ.get("SNOWFLAKE_PASSWORD", "")
    SNOWFLAKE_ACCOUNT: str = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    SNOWFLAKE_WAREHOUSE: str = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
    SNOWFLAKE_DATABASE: str = os.environ.get("SNOWFLAKE_DATABASE", "")
    SNOWFLAKE_ROLE: str = os.environ.get("SNOWFLAKE_ROLE", "")

    LSQ_SECRET_KEY: str = os.environ.get("LSQ_SECRET_KEY", "")
    LSQ_ACCESS_KEY: str = os.environ.get("LSQ_ACCESS_KEY", "")

    HOST: str = os.environ.get("HOST", "")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
