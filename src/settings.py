from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    LOGLEVEL: str = 'INFO'
    DEBUG: bool = False
    HOST: str = '127.0.0.1'
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    MODEL_PATH: str
    GP_HOST: str
    GP_PORT: int
    GP_DATABASE: str
    GP_SCHEMA: str

    class Config:
        env_file = '../.env'
        env_file_encoding = 'utf-8'
        case_sensitive = True

settings = Settings()
