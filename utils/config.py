from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

class Settings(BaseSettings):

    DB_HOST: str
    DRIVER: str
    USERNAME: str
    PASSWORD: str
    DB_NAME: str
    DB_PORT: int = 1433
    START_DATE: str
    DRY_RUN: bool

    load_dotenv(".env", override=True)
    model_config = SettingsConfigDict(env_file="../.env")

    

settings = Settings()
