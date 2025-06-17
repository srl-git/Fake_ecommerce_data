import os

from dotenv import load_dotenv

load_dotenv()


class BaseConfig:
    ENV: str = "base"
    LOG_LEVEL: str = "DEBUG"
    USE_CLOUD_LOGGING: bool = False
    DB_URL: str = "sqlite:///./ecommerce_dev.db"
    STORAGE_BUCKET: str = os.getenv("TEST_STORAGE_BUCKET_NAME", "")
    CSV_LOCAL_FILE: bool = True
    CSV_CLOUD_STORAGE_FILE: bool = False


class DevConfig(BaseConfig):
    ENV = "dev"


class CloudDevConfig(BaseConfig):
    ENV = "cloud_dev"
    USE_CLOUD_LOGGING = False
    DB_URL = os.getenv("CLOUD_SQL_TEST_DATABASE", "")
    CSV_LOCAL_FILE = False
    CSV_CLOUD_STORAGE_FILE = True


class ProdConfig(BaseConfig):
    ENV = "prod"
    LOG_LEVEL = "INFO"
    USE_CLOUD_LOGGING = True
    DB_URL = os.getenv("CLOUD_SQL_DATABASE", "")
    STORAGE_BUCKET: str = os.getenv("STORAGE_BUCKET_NAME", "")
    CSV_LOCAL_FILE = False
    CSV_CLOUD_STORAGE_FILE = True


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
