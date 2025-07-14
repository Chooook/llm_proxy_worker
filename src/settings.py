from functools import cached_property
from pathlib import Path
from typing import Tuple, Type

from pydantic_settings import (
    BaseSettings, PydanticBaseSettingsSource,
    SettingsConfigDict, YamlConfigSettingsSource)

from schemas.handler import HandlerConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        yaml_file=Path(__file__).parent.parent / 'config.yaml')

    LOGLEVEL: str
    DEBUG: bool = False

    REDIS_HOST: str = '127.0.0.1'
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_EXPIRE_DAYS: int = 7
    MAX_RETRIES: int = 3

    HANDLERS: list[HandlerConfig]
    HANDLER_PORT_RANGE: Tuple[int, int]
    HANDLER_INACTIVITY_TIMEOUT: int
    MAX_CONCURRENT_TASKS: int = 10

    @cached_property
    def redis_store_seconds(self):
        return self.REDIS_EXPIRE_DAYS * 24 * 60 * 60

    @classmethod
    def settings_customise_sources(
            cls,
            settings_cls: Type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (YamlConfigSettingsSource(settings_cls),)


settings = Settings()
