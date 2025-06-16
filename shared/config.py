import os

from dotenv import load_dotenv

load_dotenv()


class BaseConfig:
    ENV: str = "base"
    DEBUG: bool = False
    LOG_LEVEL: str = "DEBUG"
    USE_CLOUD_LOGGING: bool = False
    DB_URL: str = "sqlite:///./ecommerce_dev.db"
    STORAGE_BUCKET: str = os.getenv("TEST_STORAGE_BUCKET_NAME", "")


class DevConfig(BaseConfig):
    ENV = "dev"
    DEBUG = True


class CloudDevConfig(BaseConfig):
    ENV = "cloud_dev"
    DEBUG = True
    USE_CLOUD_LOGGING = True
    DB_URL = os.getenv("CLOUD_SQL_TEST_DATABASE", "")


class ProdConfig(BaseConfig):
    ENV = "prod"
    DEBUG = False
    LOG_LEVEL = "INFO"
    USE_CLOUD_LOGGING = True
    DB_URL = os.getenv("CLOUD_SQL_DATABASE", "")
    STORAGE_BUCKET: str = os.getenv("STORAGE_BUCKET_NAME", "")


def get_config() -> BaseConfig:
    """
    Returns the configuration class based on the ENV environment variable.

    Returns:
        DevConfig/ProdConfig/CloudDevConfig: The configuration object for the current environment.

    """
    env = os.getenv("ENV", "dev").lower()
    if env == "prod":
        return ProdConfig()
    if env == "cloud_dev":
        return CloudDevConfig()
    return DevConfig()
